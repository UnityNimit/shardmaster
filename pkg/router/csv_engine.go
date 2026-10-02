package router

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"shardmaster/pkg/hash"
	"shardmaster/pkg/storage"
)

// CSVOptions configures the CSV import/export process.
type CSVOptions struct {
	Delimiter   rune   // 0 = auto-detect
	HasHeader   bool   // default true
	TargetTable string // table name to import into or export from
	ShardKey    string // column to use as shard key (auto-detected if empty)
	MaxRows     int    // 0 = unlimited
	BatchSize   int    // batch size for relational inserts (default 500)
}

// CSVImportResult contains detailed metadata about an executed CSV import.
type CSVImportResult struct {
	TableName        string   `json:"table_name"`
	RowsImported     int64    `json:"rows_imported"`
	Columns          []string `json:"columns"`
	ColumnTypes      []string `json:"column_types"`
	ShardKey         string   `json:"shard_key"`
	ShardsSpanned    int      `json:"shards_spanned"`
	ElapsedMs        int64    `json:"elapsed_ms"`
	BytesRead        int64    `json:"bytes_read"`
	AutoCreatedTable bool     `json:"auto_created_table"`
}

// CSVExportResult contains detailed metadata about an executed CSV export.
type CSVExportResult struct {
	FilePath     string   `json:"file_path"`
	RowsExported int64    `json:"rows_exported"`
	Columns      []string `json:"columns"`
	ElapsedMs    int64    `json:"elapsed_ms"`
	BytesWritten int64    `json:"bytes_written"`
}

// ImportCSVFile reads a CSV file from disk and imports it into the distributed cluster.
func (qr *QueryRouter) ImportCSVFile(filePath string, targetTable string, shardKey string, opts CSVOptions) (*CSVImportResult, error) {
	cleanPath := filepath.Clean(strings.Trim(filePath, "\"'` "))
	file, err := os.Open(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file '%s': %w", cleanPath, err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	fileSize := int64(0)
	if stat != nil {
		fileSize = stat.Size()
	}

	opts.TargetTable = targetTable
	opts.ShardKey = shardKey
	res, err := qr.ImportCSV(file, filepath.Base(cleanPath), opts)
	if err != nil {
		return nil, err
	}
	if res.BytesRead == 0 && fileSize > 0 {
		res.BytesRead = fileSize
	}
	return res, nil
}

// ImportCSV reads CSV data from any io.Reader (file, network stream, STDIN) and imports it.
func (qr *QueryRouter) ImportCSV(r io.Reader, filename string, opts CSVOptions) (*CSVImportResult, error) {
	start := time.Now()

	// Wrap reader in bufio.Reader to allow sniffing for delimiter and header detection
	bufReader := bufio.NewReader(r)
	peekBytes, _ := bufReader.Peek(4096)

	delimiter := opts.Delimiter
	if delimiter == 0 {
		delimiter = detectCSVDelimiter(peekBytes)
	}

	targetTable := cleanTableIdentifier(opts.TargetTable)
	if targetTable == "" {
		targetTable = inferTableNameFromFilename(filename)
	}
	if targetTable == "" {
		targetTable = "imported_data"
	}

	csvReader := csv.NewReader(bufReader)
	csvReader.Comma = delimiter
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // Allow variable length records gracefully

	// 1. Read or synthesize headers
	var rawHeaders []string
	var firstRecord []string
	var err error

	if opts.HasHeader {
		rawHeaders, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV header: %w", err)
		}
	} else {
		firstRecord, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first CSV record: %w", err)
		}
		rawHeaders = make([]string, len(firstRecord))
		for i := range firstRecord {
			rawHeaders[i] = fmt.Sprintf("col_%d", i+1)
		}
	}

	cleanCols := make([]string, len(rawHeaders))
	seenCols := make(map[string]int)
	for i, h := range rawHeaders {
		name := sanitizeColumnIdentifier(h, i)
		if count, exists := seenCols[name]; exists {
			seenCols[name] = count + 1
			name = fmt.Sprintf("%s_%d", name, count+1)
		} else {
			seenCols[name] = 1
		}
		cleanCols[i] = name
	}

	// 2. Sample first 100 rows to infer column types & check table existence
	var sampleRows [][]string
	if firstRecord != nil {
		sampleRows = append(sampleRows, firstRecord)
	}
	for len(sampleRows) < 100 {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed lines during sampling
		}
		sampleRows = append(sampleRows, rec)
	}

	// 3. Determine if target table exists in catalog or SQLite
	tableExists := false
	if qr.Schema != nil {
		_, tableExists = qr.Schema.GetTable(targetTable)
	}
	if !tableExists && qr.SQL != nil {
		_, tableExists = qr.SQL.IntrospectDynamicTable(targetTable)
	}

	autoCreated := false
	colTypes := make([]string, len(cleanCols))
	shardKey := opts.ShardKey

	if !tableExists {
		// Infer column types from sample values
		colTypes = inferColumnTypes(cleanCols, sampleRows)

		// Select optimal shard key if not explicitly given
		if shardKey == "" {
			shardKey = detectOptimalShardKey(cleanCols, targetTable)
		}

		// Generate DDL and create table in SchemaCatalog + SQLite
		ddl := generateCreateTableDDL(targetTable, cleanCols, colTypes, shardKey)
		if qr.SQL != nil {
			if _, ddlErr := qr.SQL.ExecuteFullSQL(ddl); ddlErr != nil {
				return nil, fmt.Errorf("failed to auto-create table '%s': %w", targetTable, ddlErr)
			}
		}
		if qr.Schema != nil {
			_, _ = qr.Schema.CreateCustomTable(ddl)
		}
		autoCreated = true
	} else {
		// Existing table: read column types from catalog or default to TEXT
		if qr.Schema != nil {
			if ts, ok := qr.Schema.GetTable(targetTable); ok {
				typeMap := make(map[string]string)
				for _, c := range ts.Columns {
					typeMap[strings.ToLower(c.Name)] = c.DataType
				}
				for i, col := range cleanCols {
					if dt, ok := typeMap[col]; ok {
						colTypes[i] = dt
					} else {
						colTypes[i] = "TEXT"
					}
				}
				if shardKey == "" {
					shardKey = ts.ShardKey
				}
			}
		}
		if shardKey == "" {
			shardKey = detectOptimalShardKey(cleanCols, targetTable)
		}
	}

	shardKeyColIdx := 0
	cleanShardKey := strings.ToLower(strings.TrimSpace(shardKey))
	if paren := strings.IndexByte(cleanShardKey, '('); paren != -1 {
		cleanShardKey = strings.TrimSpace(cleanShardKey[:paren])
	}
	for i, c := range cleanCols {
		if strings.EqualFold(c, cleanShardKey) {
			shardKeyColIdx = i
			break
		}
	}

	// 4. Stream and ingest all rows (both sampled and remaining) into physical shards + relational mirror
	totalImported := int64(0)
	bytesReadCounter := int64(len(peekBytes))
	spannedShardsMap := make(map[uint32]bool)

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	// Process rows in chunks
	var pendingBatch [][]string
	ingestRow := func(record []string) error {
		totalImported++
		for _, cell := range record {
			bytesReadCounter += int64(len(cell)) + 1
		}

		keyVal := ""
		if shardKeyColIdx < len(record) && strings.TrimSpace(record[shardKeyColIdx]) != "" {
			keyVal = strings.TrimSpace(record[shardKeyColIdx])
		} else {
			keyVal = strconv.FormatInt(totalImported, 10)
		}

		bucket := hash.ComputeBucket(keyVal)
		shardID := qr.Dir.GetBucketOwner(bucket)
		spannedShardsMap[shardID] = true

		if targetTable == "users" {
			// Ingest into users table & physical shard slabs
			uid, _ := strconv.ParseInt(keyVal, 10, 64)
			if uid <= 0 {
				uid = totalImported
			}
			userRow := constructUserRowFromRecord(uid, keyVal, bucket, cleanCols, record)
			if sh, ok := qr.Cluster.GetShard(shardID); ok {
				_, _ = sh.TryUpsertUser(userRow, true, false)
			}
			if qr.SQL != nil {
				qr.SQL.SyncUserUpsert(shardID, userRow)
			}
		} else {
			// Ingest into custom table physical slabs
			colMap := make(map[string]string, len(cleanCols))
			for i, cName := range cleanCols {
				if i < len(record) {
					colMap[cName] = record[i]
				} else {
					colMap[cName] = ""
				}
			}
			cRow := storage.CustomRow{
				TableName: targetTable,
				RowKey:    keyVal,
				ShardKey:  keyVal,
				BucketID:  bucket,
				Columns:   colMap,
			}
			cRow.ByteSize = storage.ComputeCustomRowBytes(targetTable, keyVal, keyVal, colMap)

			if sh, ok := qr.Cluster.GetShard(shardID); ok {
				_ = sh.TryUpsertCustomRow(cRow, true, false)
			}

			pendingBatch = append(pendingBatch, record)
			if len(pendingBatch) >= batchSize {
				_ = qr.flushBatchToRelational(targetTable, cleanCols, pendingBatch)
				pendingBatch = pendingBatch[:0]
			}
		}
		return nil
	}

	// Ingest sample rows first
	for _, rec := range sampleRows {
		if opts.MaxRows > 0 && totalImported >= int64(opts.MaxRows) {
			break
		}
		_ = ingestRow(rec)
	}

	// Continue reading until EOF
	for opts.MaxRows == 0 || totalImported < int64(opts.MaxRows) {
		rec, rErr := csvReader.Read()
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			continue // skip corrupt line
		}
		_ = ingestRow(rec)
	}

	// Flush any remaining relational batch
	if len(pendingBatch) > 0 {
		_ = qr.flushBatchToRelational(targetTable, cleanCols, pendingBatch)
	}

	// Update live cluster stats and persisted state
	if qr.SQL != nil {
		qr.SQL.ReseedFromCluster()
	}
	qr.Cluster.SaveStateFile(qr.Dir.SnapshotBuckets())

	elapsed := time.Since(start).Milliseconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	return &CSVImportResult{
		TableName:        targetTable,
		RowsImported:     totalImported,
		Columns:          cleanCols,
		ColumnTypes:      colTypes,
		ShardKey:         cleanCols[shardKeyColIdx],
		ShardsSpanned:    len(spannedShardsMap),
		ElapsedMs:        elapsed,
		BytesRead:        bytesReadCounter,
		AutoCreatedTable: autoCreated,
	}, nil
}

// ExportCSVFile queries data and exports it to a CSV file on disk.
func (qr *QueryRouter) ExportCSVFile(queryOrTable string, filePath string, opts CSVOptions) (*CSVExportResult, error) {
	cleanPath := filepath.Clean(strings.Trim(filePath, "\"'` "))
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0755); err != nil && dir != "." {
		return nil, fmt.Errorf("failed to create directory '%s': %w", dir, err)
	}

	outFile, err := os.Create(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create export file '%s': %w", cleanPath, err)
	}
	defer outFile.Close()

	opts.TargetTable = queryOrTable
	res, err := qr.ExportCSV(outFile, queryOrTable, opts)
	if err != nil {
		return nil, err
	}
	res.FilePath = cleanPath
	return res, nil
}

// ExportCSV executes queryOrTable and writes CSV data into w.
func (qr *QueryRouter) ExportCSV(w io.Writer, queryOrTable string, opts CSVOptions) (*CSVExportResult, error) {
	start := time.Now()

	sql := strings.TrimSpace(queryOrTable)
	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		cleanTbl := cleanTableIdentifier(sql)
		sql = fmt.Sprintf("SELECT * FROM %s;", cleanTbl)
	}

	rs, err := qr.ExecuteSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("CSV export query failed: %w", err)
	}

	delimiter := opts.Delimiter
	if delimiter == 0 {
		delimiter = ','
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = delimiter

	var bytesWritten int64

	// Write header row if requested
	if opts.HasHeader || opts.Delimiter == 0 {
		if err := csvWriter.Write(rs.Columns); err != nil {
			return nil, fmt.Errorf("failed to write CSV header: %w", err)
		}
		for _, c := range rs.Columns {
			bytesWritten += int64(len(c)) + 1
		}
	}

	for _, row := range rs.Rows {
		if err := csvWriter.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
		for _, cell := range row {
			bytesWritten += int64(len(cell)) + 1
		}
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Milliseconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	return &CSVExportResult{
		FilePath:     "stream",
		RowsExported: int64(len(rs.Rows)),
		Columns:      rs.Columns,
		ElapsedMs:    elapsed,
		BytesWritten: bytesWritten,
	}, nil
}

func (qr *QueryRouter) flushBatchToRelational(tableName string, cols []string, batch [][]string) error {
	if qr.SQL == nil || len(batch) == 0 {
		return nil
	}
	colList := strings.Join(cols, ", ")
	valPlaceholders := make([]string, len(batch))
	args := make([]any, 0, len(batch)*len(cols))

	singleRowPH := "(" + strings.Repeat("?,", len(cols)-1) + "?)"
	for rIdx, row := range batch {
		valPlaceholders[rIdx] = singleRowPH
		for cIdx := range cols {
			if cIdx < len(row) {
				args = append(args, row[cIdx])
			} else {
				args = append(args, nil)
			}
		}
	}

	q := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES %s;",
		tableName, colList, strings.Join(valPlaceholders, ","))

	_, err := qr.SQL.db.Exec(q, args...)
	return err
}

func detectCSVDelimiter(sample []byte) rune {
	if len(sample) == 0 {
		return ','
	}
	firstLine := sample
	if nl := bytes.IndexByte(sample, '\n'); nl != -1 {
		firstLine = sample[:nl]
	}

	counts := map[rune]int{
		',':  bytes.Count(firstLine, []byte{','}),
		'\t': bytes.Count(firstLine, []byte{'\t'}),
		';':  bytes.Count(firstLine, []byte{';'}),
		'|':  bytes.Count(firstLine, []byte{'|'}),
	}

	best := ','
	maxCount := 0
	for _, delim := range []rune{',', '\t', ';', '|'} {
		if counts[delim] > maxCount {
			maxCount = counts[delim]
			best = delim
		}
	}
	return best
}

func inferTableNameFromFilename(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	clean := strings.TrimSuffix(base, ext)
	return sanitizeColumnIdentifier(clean, 0)
}

var identRegex = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitizeColumnIdentifier(raw string, colIndex int) string {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, `"'` + "`[]()")
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, ".", "_")
	s = identRegex.ReplaceAllString(s, "")
	s = strings.Trim(s, "_")
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = fmt.Sprintf("col_%s_%d", s, colIndex+1)
		s = strings.Trim(s, "_")
	}
	return s
}

func inferColumnTypes(cols []string, sampleRows [][]string) []string {
	types := make([]string, len(cols))
	if len(sampleRows) == 0 {
		for i := range types {
			types[i] = "TEXT"
		}
		return types
	}

	for colIdx, colName := range cols {
		lowerName := strings.ToLower(colName)
		if strings.HasSuffix(lowerName, "_usd") || strings.Contains(lowerName, "price") || strings.Contains(lowerName, "amount") || strings.Contains(lowerName, "balance") {
			types[colIdx] = "NUMERIC(12,2)"
			continue
		}
		if strings.HasSuffix(lowerName, "_at") || strings.Contains(lowerName, "timestamp") || strings.Contains(lowerName, "date") {
			types[colIdx] = "TIMESTAMPTZ"
			continue
		}
		if lowerName == "id" || strings.HasSuffix(lowerName, "_id") || strings.HasSuffix(lowerName, "_cents") || strings.Contains(lowerName, "count") {
			types[colIdx] = "BIGINT"
			continue
		}

		allInt := true
		allFloat := true
		allBool := true
		allTime := true
		hasValues := false

		for _, row := range sampleRows {
			if colIdx >= len(row) {
				continue
			}
			val := strings.TrimSpace(row[colIdx])
			if val == "" || strings.EqualFold(val, "null") {
				continue
			}
			hasValues = true

			// Integer check
			if allInt {
				if _, err := strconv.ParseInt(val, 10, 64); err != nil {
					allInt = false
				}
			}

			// Float check (strip currency symbols and commas)
			if allFloat {
				cleanedVal := strings.Trim(val, "$€£¥ ")
				cleanedVal = strings.ReplaceAll(cleanedVal, ",", "")
				if _, err := strconv.ParseFloat(cleanedVal, 64); err != nil {
					allFloat = false
				}
			}

			// Boolean check
			if allBool {
				l := strings.ToLower(val)
				if l != "true" && l != "false" && l != "t" && l != "f" && l != "1" && l != "0" && l != "yes" && l != "no" {
					allBool = false
				}
			}

			// Timestamp check
			if allTime {
				if !isTimestampString(val) {
					allTime = false
				}
			}
		}

		if !hasValues {
			types[colIdx] = "TEXT"
		} else if allInt {
			types[colIdx] = "BIGINT"
		} else if allFloat {
			types[colIdx] = "NUMERIC(12,2)"
		} else if allBool {
			types[colIdx] = "BOOLEAN"
		} else if allTime {
			types[colIdx] = "TIMESTAMPTZ"
		} else {
			types[colIdx] = "TEXT"
		}
	}
	return types
}

func isTimestampString(val string) bool {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, l := range layouts {
		if _, err := time.Parse(l, val); err == nil {
			return true
		}
	}
	return false
}

func detectOptimalShardKey(cols []string, tableName string) string {
	tblID := strings.ToLower(tableName) + "_id"
	for _, c := range cols {
		l := strings.ToLower(c)
		if l == "user_id" || l == "id" || l == tblID || l == "account_id" || l == "customer_id" || l == "uuid" || l == "key" {
			return c
		}
	}
	for _, c := range cols {
		l := strings.ToLower(c)
		if strings.HasSuffix(l, "_id") || strings.HasSuffix(l, "id") {
			return c
		}
	}
	if len(cols) > 0 {
		return cols[0]
	}
	return "id"
}

func generateCreateTableDDL(tableName string, cols, types []string, shardKey string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))
	for i, c := range cols {
		t := types[i]
		defaultVal := "DEFAULT ''"
		switch t {
		case "BIGINT", "INT8", "INTEGER", "INT":
			defaultVal = "DEFAULT 0"
		case "NUMERIC(12,2)", "FLOAT", "DOUBLE":
			defaultVal = "DEFAULT 0.00"
		case "BOOLEAN", "BOOL":
			defaultVal = "DEFAULT FALSE"
		case "TIMESTAMPTZ", "TIMESTAMP":
			defaultVal = "DEFAULT CURRENT_TIMESTAMP"
		}

		pkClause := ""
		if strings.EqualFold(c, shardKey) {
			pkClause = " PRIMARY KEY"
		}
		comma := ","
		if i == len(cols)-1 {
			comma = ""
		}
		sb.WriteString(fmt.Sprintf("    %s %s NOT NULL %s%s%s\n", c, t, defaultVal, pkClause, comma))
	}
	sb.WriteString(fmt.Sprintf(") SHARD BY HASH (%s) INTO 1024 VIRTUAL BUCKETS;", shardKey))
	return sb.String()
}

func constructUserRowFromRecord(uid int64, ukey string, bucket uint16, cols, record []string) storage.UserRow {
	u := storage.UserRow{
		UserID:       uid,
		UserKey:      ukey,
		BucketID:     bucket,
		TenantID:     "tenant_core",
		Region:       "local-node-0",
		BalanceCents: 250000,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	for i, cName := range cols {
		if i >= len(record) {
			continue
		}
		val := strings.TrimSpace(record[i])
		switch strings.ToLower(cName) {
		case "name", "full_name", "username":
			if val != "" {
				u.Name = val
			}
		case "email":
			if val != "" {
				u.Email = val
			}
		case "tenant", "tenant_id":
			if val != "" {
				u.TenantID = val
			}
		case "region", "zone":
			if val != "" {
				u.Region = val
			}
		case "balance_cents":
			if iv, err := strconv.ParseInt(val, 10, 64); err == nil {
				u.BalanceCents = iv
			}
		case "balance_usd", "balance":
			cleaned := strings.Trim(val, "$€£ ")
			cleaned = strings.ReplaceAll(cleaned, ",", "")
			if fv, err := strconv.ParseFloat(cleaned, 64); err == nil {
				u.BalanceCents = int64(fv * 100.0)
			}
		case "created_at":
			if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
				u.CreatedAt = t
			}
		}
	}
	if u.Name == "" {
		u.Name = fmt.Sprintf("User_%s", ukey)
	}
	if u.Email == "" {
		u.Email = fmt.Sprintf("user_%s@gmail.com", ukey)
	}
	return u
}
