package router

import (
	"strconv"
	"strings"
)

type QueryKind uint8

const (
	QueryPointSelect QueryKind = iota
	QueryMultiPointSelect
	QueryPointUpsert
	QueryPointUpdate
	QueryPointDelete
	QueryScatterGather
	QueryCountAggregate
	QueryGroupByAggregate
	QuerySelectCDCLog
	QueryShowTables
	QueryDescribeTable
	QueryShowCreateTable
	QueryShowIndexes
	QueryShowSchemas
	QueryCreateTable
	QueryDropTable
	QueryAdminShowShards
	QueryAdminShowBuckets
	QueryAdminShowCDC
	QueryAdminShowHotspots
	QueryAdminShowStats
	QueryAdminShowQueries
	QueryAdminExplainShard
	QueryExplainAnalyze
	QueryAdminRebalance
	QueryAdminVDiff
	QueryFullRelationalSQL
	QuerySystemCatalog
)

type ClassifiedQuery struct {
	Kind           QueryKind
	RawSQL         string
	TableName      string
	ProjectedCols  []string
	HasShardKey    bool
	UserID         int64
	UserIDs        []int64
	UserKey        string
	Name           string
	Email          string
	TenantFilter   string
	RegionFilter   string
	BalanceCents   int64
	HasBalanceUpd  bool
	GroupByCol     string
	OrderByCol     string
	OrderAsc       bool
	Limit          int
	TargetShards   uint32
}

// ClassifySQL parses a SQL statement with fast zero-regex string scanning,
// normalizing multi-line [Shift+Enter] newlines, [Tab] indentation, and SQL comments.
func ClassifySQL(sql string) ClassifiedQuery {
	rawTrimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	trimmed := NormalizeSQLWhitespace(rawTrimmed)
	if trimmed == "" {
		trimmed = rawTrimmed
	}
	upper := strings.ToUpper(trimmed)

	// 1. psql backslash meta-commands (\dt, \d, \di, \dn, \l)
	if strings.HasPrefix(trimmed, `\`) {
		fields := strings.Fields(trimmed)
		cmd := strings.ToLower(fields[0])
		arg := ""
		if len(fields) > 1 {
			arg = fields[1]
		}
		switch cmd {
		case `\dt`:
			return ClassifiedQuery{Kind: QueryShowTables, RawSQL: trimmed}
		case `\di`:
			return ClassifiedQuery{Kind: QueryShowIndexes, RawSQL: trimmed, TableName: arg}
		case `\dn`, `\l`:
			return ClassifiedQuery{Kind: QueryShowSchemas, RawSQL: trimmed}
		case `\d`, `\d+`:
			if arg == "" {
				return ClassifiedQuery{Kind: QueryDescribeTable, RawSQL: trimmed, TableName: "users"}
			}
			return ClassifiedQuery{Kind: QueryDescribeTable, RawSQL: trimmed, TableName: arg}
		}
	}

	// 2. psql startup / transaction / system catalog introspection queries
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "SELECT PG_CATALOG") ||
		strings.HasPrefix(upper, "SELECT VERSION()") ||
		strings.HasPrefix(upper, "SELECT CURRENT_") ||
		strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") {
		return ClassifiedQuery{Kind: QuerySystemCatalog, RawSQL: trimmed}
	}

	// 3. Schema & DDL Catalog Commands
	if strings.HasPrefix(upper, "SHOW TABLES") ||
		strings.Contains(upper, "INFORMATION_SCHEMA.TABLES") ||
		strings.Contains(upper, "PG_TABLES") {
		return ClassifiedQuery{Kind: QueryShowTables, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW SCHEMAS") || strings.HasPrefix(upper, "SHOW DATABASES") {
		return ClassifiedQuery{Kind: QueryShowSchemas, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW CREATE TABLE") {
		tbl := strings.TrimSpace(trimmed[len("SHOW CREATE TABLE"):])
		if tbl == "" {
			tbl = "users"
		}
		return ClassifiedQuery{Kind: QueryShowCreateTable, RawSQL: trimmed, TableName: tbl}
	}
	if strings.HasPrefix(upper, "SHOW INDEX") || strings.HasPrefix(upper, "SHOW KEYS") {
		tbl := extractAfterKeyword(trimmed, "FROM")
		return ClassifiedQuery{Kind: QueryShowIndexes, RawSQL: trimmed, TableName: tbl}
	}
	if strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC ") ||
		upper == "DESC" ||
		strings.HasPrefix(upper, "SHOW COLUMNS") ||
		strings.HasPrefix(upper, "SHOW FIELDS") ||
		strings.HasPrefix(upper, "SHOW SCHEMA") ||
		strings.Contains(upper, "INFORMATION_SCHEMA.COLUMNS") {
		tbl := "users"
		if strings.HasPrefix(upper, "DESCRIBE") {
			if rest := strings.TrimSpace(trimmed[len("DESCRIBE"):]); rest != "" {
				tbl = rest
			}
		} else if strings.HasPrefix(upper, "DESC") {
			if rest := strings.TrimSpace(trimmed[len("DESC"):]); rest != "" {
				tbl = rest
			}
		} else if fromTbl := extractAfterKeyword(trimmed, "FROM"); fromTbl != "" && !strings.Contains(strings.ToUpper(fromTbl), "INFORMATION_SCHEMA") {
			tbl = fromTbl
		} else if tblEq := extractQuotedField(trimmed, "table_name", ""); tblEq != "" {
			tbl = tblEq
		}
		return ClassifiedQuery{Kind: QueryDescribeTable, RawSQL: trimmed, TableName: tbl}
	}
	if strings.HasPrefix(upper, "CREATE TABLE") {
		return ClassifiedQuery{Kind: QueryCreateTable, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "DROP TABLE") {
		rest := strings.TrimSpace(trimmed[len("DROP TABLE"):])
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}
		return ClassifiedQuery{Kind: QueryDropTable, RawSQL: trimmed, TableName: rest}
	}

	// 4. Cluster Control Plane & Diagnostics Commands
	if strings.HasPrefix(upper, "SHOW SHARDS") ||
		strings.HasPrefix(upper, "SHOW TOPOLOGY") ||
		strings.Contains(upper, "_SHARDMASTER_SHARDS") {
		return ClassifiedQuery{Kind: QueryAdminShowShards, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW BUCKETS") ||
		strings.Contains(upper, "_SHARDMASTER_BUCKETS") {
		return ClassifiedQuery{Kind: QueryAdminShowBuckets, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW CDC") || strings.HasPrefix(upper, "SHOW WORKFLOW") || strings.HasPrefix(upper, "SHOW REPLICATION") {
		return ClassifiedQuery{Kind: QueryAdminShowCDC, RawSQL: trimmed}
	}
	if strings.Contains(upper, "_SHARDMASTER_CDC") {
		return ClassifiedQuery{
			Kind:   QuerySelectCDCLog,
			RawSQL: trimmed,
			Limit:  extractLimit(upper, 10),
		}
	}
	if strings.HasPrefix(upper, "SHOW HOTSPOT") || strings.HasPrefix(upper, "SHOW EWMA") {
		return ClassifiedQuery{Kind: QueryAdminShowHotspots, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW STATS") || strings.HasPrefix(upper, "SHOW MEMORY") || strings.HasPrefix(upper, "SHOW TELEMETRY") {
		return ClassifiedQuery{Kind: QueryAdminShowStats, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW QUERIES") || strings.HasPrefix(upper, "SHOW HELP") || strings.HasPrefix(upper, "SHOW COMMANDS") {
		return ClassifiedQuery{Kind: QueryAdminShowQueries, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "EXPLAIN SHARD") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 42
			ukey = "42"
		}
		return ClassifiedQuery{
			Kind:        QueryAdminExplainShard,
			RawSQL:      trimmed,
			HasShardKey: true,
			UserID:      uid,
			UserKey:     ukey,
		}
	}
	if strings.HasPrefix(upper, "EXPLAIN") {
		return ClassifiedQuery{
			Kind:   QueryExplainAnalyze,
			RawSQL: trimmed,
		}
	}
	if strings.HasPrefix(upper, "REBALANCE") {
		target := uint32(8)
		fields := strings.Fields(upper)
		for _, f := range fields {
			if n, err := strconv.Atoi(f); err == nil && n >= 2 && n <= 64 {
				target = uint32(n)
				break
			}
		}
		return ClassifiedQuery{Kind: QueryAdminRebalance, RawSQL: trimmed, TargetShards: target}
	}
	if strings.HasPrefix(upper, "RUN VDIFF") ||
		strings.HasPrefix(upper, "SHOW VDIFF") ||
		strings.Contains(upper, "_SHARDMASTER_VDIFF") {
		return ClassifiedQuery{Kind: QueryAdminVDiff, RawSQL: trimmed}
	}

	// 4b. Route any general / complex relational SQL (JOINs, CTEs, Window Functions, Subqueries,
	// Views, Indexes, ALTER TABLE, custom tables, functions, or complex predicates) to the 100% Full SQL Engine
	if shouldUseFullRelationalEngine(trimmed, upper) {
		return ClassifiedQuery{Kind: QueryFullRelationalSQL, RawSQL: trimmed}
	}

	// 5. Data Mutations: INSERT, UPDATE, DELETE
	if strings.HasPrefix(upper, "INSERT") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 42
			ukey = "42"
		}
		name, email, balCents := extractInsertValues(trimmed, ukey)
		return ClassifiedQuery{
			Kind:         QueryPointUpsert,
			RawSQL:       trimmed,
			HasShardKey:  true,
			UserID:       uid,
			UserKey:      ukey,
			Name:         name,
			Email:        email,
			BalanceCents: balCents,
		}
	}

	if strings.HasPrefix(upper, "UPDATE") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 42
			ukey = "42"
		}
		balCents, hasBal := extractBalanceFromSet(trimmed)
		return ClassifiedQuery{
			Kind:          QueryPointUpdate,
			RawSQL:        trimmed,
			HasShardKey:   true,
			UserID:        uid,
			UserKey:       ukey,
			Name:          extractQuotedField(trimmed, "name", ""),
			Email:         extractQuotedField(trimmed, "email", ""),
			RegionFilter:  extractQuotedField(trimmed, "region", ""),
			TenantFilter:  extractQuotedField(trimmed, "tenant_id", ""),
			BalanceCents:  balCents,
			HasBalanceUpd: hasBal,
		}
	}

	if strings.HasPrefix(upper, "DELETE") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 100
			ukey = "100"
		}
		return ClassifiedQuery{
			Kind:        QueryPointDelete,
			RawSQL:      trimmed,
			HasShardKey: true,
			UserID:      uid,
			UserKey:     ukey,
		}
	}

	// 6. Aggregations & GROUP BY
	if gbIdx := strings.Index(upper, "GROUP BY"); gbIdx != -1 {
		gbRest := strings.TrimSpace(trimmed[gbIdx+8:])
		gbCol := strings.ToLower(readToken(gbRest))
		return ClassifiedQuery{
			Kind:       QueryGroupByAggregate,
			RawSQL:     trimmed,
			GroupByCol: gbCol,
		}
	}
	if strings.Contains(upper, "COUNT(") ||
		strings.Contains(upper, "SUM(") ||
		strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "MIN(") ||
		strings.Contains(upper, "MAX(") {
		return ClassifiedQuery{Kind: QueryCountAggregate, RawSQL: trimmed}
	}

	projected := extractProjectedColumns(trimmed)

	// 7. Multi-Key Point Batch: WHERE user_id IN (...) or WHERE user_id BETWEEN X AND Y
	if ids := extractUserIDMultiClause(trimmed); len(ids) > 0 {
		return ClassifiedQuery{
			Kind:          QueryMultiPointSelect,
			RawSQL:        trimmed,
			ProjectedCols: projected,
			HasShardKey:   true,
			UserIDs:       ids,
		}
	}

	// 8. Single-Key Point Query: WHERE user_id = X
	if uid, ukey, ok := extractUserIDClause(trimmed); ok {
		return ClassifiedQuery{
			Kind:          QueryPointSelect,
			RawSQL:        trimmed,
			ProjectedCols: projected,
			HasShardKey:   true,
			UserID:        uid,
			UserKey:       ukey,
		}
	}

	// 9. Non-key query -> Distributed Scatter-Gather + Streaming K-Way Merge (Pillar 2)
	limit := extractLimit(upper, 10)
	emailLike := extractLikePattern(trimmed, "email")
	regionEq := extractQuotedField(trimmed, "region", "")
	tenantEq := extractQuotedField(trimmed, "tenant_id", "")

	return ClassifiedQuery{
		Kind:          QueryScatterGather,
		RawSQL:        trimmed,
		ProjectedCols: projected,
		HasShardKey:   false,
		Email:         emailLike,
		RegionFilter:  regionEq,
		TenantFilter:  tenantEq,
		Limit:         limit,
	}
}

func extractProjectedColumns(sql string) []string {
	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(upper, "SELECT ") {
		return nil
	}
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		return nil
	}
	selectPart := strings.TrimSpace(sql[7:fromIdx])
	if selectPart == "*" || selectPart == "" {
		return nil
	}
	rawCols := strings.Split(selectPart, ",")
	var cols []string
	for _, rc := range rawCols {
		c := strings.ToLower(strings.TrimSpace(rc))
		if asIdx := strings.Index(c, " as "); asIdx != -1 {
			c = strings.TrimSpace(c[:asIdx])
		}
		if dot := strings.LastIndexByte(c, '.'); dot != -1 {
			c = c[dot+1:]
		}
		if c != "" && c != "*" {
			cols = append(cols, c)
		}
	}
	return cols
}

func extractUserIDMultiClause(sql string) []int64 {
	lower := strings.ToLower(sql)
	idx := strings.LastIndex(lower, "user_id")
	if idx == -1 {
		return nil
	}
	rest := strings.TrimSpace(sql[idx+len("user_id"):])
	restLower := strings.ToLower(rest)

	// Case A: WHERE user_id IN (42, 100, 777)
	if strings.HasPrefix(restLower, "in") {
		openP := strings.IndexByte(rest, '(')
		closeP := strings.IndexByte(rest, ')')
		if openP != -1 && closeP > openP {
			items := strings.Split(rest[openP+1:closeP], ",")
			var ids []int64
			for _, it := range items {
				clean := strings.Trim(it, " '\"`\t\r\n")
				clean = strings.TrimPrefix(strings.ToLower(clean), "user_")
				if id, err := strconv.ParseInt(clean, 10, 64); err == nil {
					ids = append(ids, id)
				}
			}
			return ids
		}
	}

	// Case B: WHERE user_id BETWEEN 100 AND 105
	if strings.HasPrefix(restLower, "between") {
		fields := strings.Fields(restLower)
		if len(fields) >= 4 && fields[2] == "and" {
			startID, err1 := strconv.ParseInt(strings.Trim(fields[1], "'\" "), 10, 64)
			endID, err2 := strconv.ParseInt(strings.Trim(fields[3], "'\"; "), 10, 64)
			if err1 == nil && err2 == nil && endID >= startID {
				if endID-startID > 25 {
					endID = startID + 25
				}
				var ids []int64
				for id := startID; id <= endID; id++ {
					ids = append(ids, id)
				}
				return ids
			}
		}
	}
	return nil
}

func extractUserIDClause(sql string) (int64, string, bool) {
	lower := strings.ToLower(sql)
	idx := strings.LastIndex(lower, "user_id")
	if idx != -1 {
		rest := strings.TrimSpace(sql[idx+len("user_id"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
			token := readToken(rest)
			token = strings.Trim(token, "'\" ")
			tokenClean := strings.TrimPrefix(strings.ToLower(token), "user_")
			if id, err := strconv.ParseInt(tokenClean, 10, 64); err == nil {
				return id, strconv.FormatInt(id, 10), true
			}
			if token != "" {
				return 123, token, true
			}
		}
	}
	// Check VALUES (123, ...)
	vIdx := strings.Index(lower, "values")
	if vIdx != -1 {
		rest := sql[vIdx+6:]
		openParen := strings.IndexByte(rest, '(')
		if openParen != -1 {
			inner := rest[openParen+1:]
			comma := strings.IndexAny(inner, ",)")
			if comma != -1 {
				firstVal := strings.Trim(inner[:comma], "'\" ")
				firstValClean := strings.TrimPrefix(strings.ToLower(firstVal), "user_")
				if id, err := strconv.ParseInt(firstValClean, 10, 64); err == nil {
					return id, strconv.FormatInt(id, 10), true
				}
			}
		}
	}
	return 0, "", false
}

func extractInsertValues(sql string, ukey string) (name string, email string, balCents int64) {
	name = extractQuotedField(sql, "name", "")
	email = extractQuotedField(sql, "email", "")
	balCents = 250000

	lower := strings.ToLower(sql)
	vIdx := strings.Index(lower, "values")
	if vIdx != -1 {
		rest := sql[vIdx+6:]
		openP := strings.IndexByte(rest, '(')
		closeP := strings.LastIndexByte(rest, ')')
		if openP != -1 && closeP > openP {
			parts := strings.Split(rest[openP+1:closeP], ",")
			if len(parts) >= 2 && name == "" {
				name = strings.Trim(parts[1], " '\"`")
			}
			if len(parts) >= 3 && email == "" {
				email = strings.Trim(parts[2], " '\"`")
			}
			if len(parts) >= 4 {
				if b, err := strconv.ParseInt(strings.Trim(parts[3], " '\"`"), 10, 64); err == nil {
					balCents = b
				}
			}
		}
	}
	if name == "" {
		name = "User_" + ukey
	}
	if email == "" {
		email = "user_" + ukey + "@gmail.com"
	}
	return name, email, balCents
}

func extractBalanceFromSet(sql string) (int64, bool) {
	lower := strings.ToLower(sql)
	if idx := strings.Index(lower, "balance_cents"); idx != -1 {
		rest := strings.TrimSpace(sql[idx+len("balance_cents"):])
		if strings.HasPrefix(rest, "=") {
			tok := readToken(strings.TrimSpace(rest[1:]))
			if v, err := strconv.ParseInt(strings.Trim(tok, "'\" "), 10, 64); err == nil {
				return v, true
			}
		}
	}
	if idx := strings.Index(lower, "balance_usd"); idx != -1 {
		rest := strings.TrimSpace(sql[idx+len("balance_usd"):])
		if strings.HasPrefix(rest, "=") {
			tok := readToken(strings.TrimSpace(rest[1:]))
			tok = strings.Trim(tok, "'\"$ ")
			if v, err := strconv.ParseFloat(tok, 64); err == nil {
				return int64(v * 100), true
			}
		}
	}
	return 0, false
}

func extractAfterKeyword(sql, keyword string) string {
	upper := strings.ToUpper(sql)
	kw := strings.ToUpper(keyword) + " "
	idx := strings.Index(upper, kw)
	if idx == -1 {
		return ""
	}
	rest := strings.TrimSpace(sql[idx+len(kw):])
	return readToken(rest)
}

func extractLikePattern(sql, col string) string {
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, col)
	if idx == -1 {
		return ""
	}
	rest := sql[idx+len(col):]
	q1 := strings.IndexByte(rest, '\'')
	if q1 == -1 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '\'')
	if q2 == -1 {
		return ""
	}
	return rest[q1+1 : q1+1+q2]
}

func extractQuotedField(sql, col, fallback string) string {
	val := extractLikePattern(sql, col)
	if val == "" {
		return fallback
	}
	return val
}

func extractLimit(upperSQL string, defaultLimit int) int {
	idx := strings.Index(upperSQL, "LIMIT ")
	if idx == -1 {
		return defaultLimit
	}
	rest := strings.TrimSpace(upperSQL[idx+6:])
	tok := readToken(rest)
	if n, err := strconv.Atoi(tok); err == nil && n > 0 && n <= 500 {
		return n
	}
	return defaultLimit
}

func readToken(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == ';' || ch == ')' || ch == ',' {
			break
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// shouldUseFullRelationalEngine returns true if the SQL statement uses relational constructs
// beyond the single-table `users` fast-path (such as JOINs, CTEs, Window Functions, Subqueries,
// Views, Indexes, ALTER TABLE, custom tables, scalar functions, HAVING, CASE WHEN, or complex WHERE predicates).
func shouldUseFullRelationalEngine(trimmed string, upper string) bool {
	for _, prefix := range []string{
		"WITH ", "PRAGMA ", "VALUES ", "ALTER ", "CREATE VIEW", "CREATE OR REPLACE VIEW",
		"CREATE TEMP ", "CREATE TEMPORARY ", "DROP VIEW", "CREATE INDEX", "CREATE UNIQUE INDEX",
		"DROP INDEX", "CREATE TRIGGER", "DROP TRIGGER", "TRUNCATE ", "REPLACE INTO",
		"SAVEPOINT ", "RELEASE ", "ANALYZE", "VACUUM", "REINDEX",
	} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	// SELECT without FROM (e.g. SELECT 1+1, xxhash64('42'), now())
	if strings.HasPrefix(upper, "SELECT ") && !strings.Contains(upper, " FROM ") {
		return true
	}

	// Check if query contains advanced SQL keywords, functions, aliases, or operators
	for _, kw := range []string{
		" JOIN ", " OVER (", " OVER(", "(SELECT ", " UNION ", " INTERSECT ", " EXCEPT ",
		" HAVING ", " DISTINCT ", " OFFSET ", " CASE ", " WHEN ", " COALESCE(", " IFNULL(", " NULLIF(", " IIF(",
		" ROUND(", " ABS(", " UPPER(", " LOWER(", " SUBSTR(", " SUBSTRING(", " LENGTH(", " REPLACE(", " TRIM(",
		" LTRIM(", " RTRIM(", " PRINTF(", " CONCAT(", " LEFT(", " RIGHT(", " SPLIT_PART(", " REVERSE(",
		" GREATEST(", " LEAST(", " MD5(", " SHA256(", " GEN_RANDOM_UUID(", " UUID(", " RANDOM(",
		" CAST(", " GROUP_CONCAT(", " STRING_AGG(", " JSON_", " STRFTIME(", " DATETIME(", " DATE(", " TIME(",
		" XXHASH64(", " VIRTUAL_BUCKET(", " TARGET_SHARD(", " NOW(", " ON CONFLICT", " RETURNING ",
		" IS NULL", " IS NOT NULL", " NOT IN", " NOT LIKE", " EXISTS ", " EXISTS(",
		" OR ", " != ", " <> ", " >= ", " <= ", " > ", " < ", " || ", " + ", " - ", " / ", " AS ",
	} {
		if strings.Contains(upper, kw) {
			return true
		}
	}

	// Multi-row INSERT VALUES (...), (...) or INSERT INTO ... SELECT
	if strings.HasPrefix(upper, "INSERT ") {
		if strings.Contains(upper, "), (") || strings.Contains(upper, "),(") || strings.Contains(upper, " SELECT ") {
			return true
		}
	}

	// UPDATE or DELETE without an explicit user_id clause
	if strings.HasPrefix(upper, "UPDATE ") || strings.HasPrefix(upper, "DELETE ") {
		if _, _, ok := extractUserIDClause(trimmed); !ok {
			return true
		}
	}

	// ORDER BY on any column other than created_at
	if obIdx := strings.Index(upper, " ORDER BY "); obIdx != -1 {
		obRest := strings.TrimSpace(upper[obIdx+10:])
		if !strings.HasPrefix(obRest, "CREATED_AT") {
			return true
		}
	}

	// Check if target table in FROM / INSERT INTO / UPDATE / DELETE FROM is a table other than `users`
	targetTbl := extractPrimaryTableName(trimmed, upper)
	if targetTbl != "" && targetTbl != "users" && !strings.HasPrefix(targetTbl, "_shardmaster_") {
		return true
	}

	// Check if `FROM users` is followed by a table alias or comma join (e.g. `FROM users u` or `FROM users, orders`)
	if idx := strings.Index(upper, " FROM USERS "); idx != -1 {
		afterUsers := strings.TrimSpace(upper[idx+12:])
		firstAfter := readToken(afterUsers)
		if firstAfter != "" &&
			firstAfter != "WHERE" &&
			firstAfter != "GROUP" &&
			firstAfter != "ORDER" &&
			firstAfter != "LIMIT" &&
			firstAfter != "HAVING" {
			return true
		}
	}
	if strings.Contains(upper, " FROM USERS,") {
		return true
	}

	return false
}

func extractPrimaryTableName(trimmed string, upper string) string {
	var rest string
	switch {
	case strings.HasPrefix(upper, "INSERT INTO "):
		rest = strings.TrimSpace(trimmed[len("INSERT INTO "):])
	case strings.HasPrefix(upper, "INSERT OR REPLACE INTO "):
		rest = strings.TrimSpace(trimmed[len("INSERT OR REPLACE INTO "):])
	case strings.HasPrefix(upper, "INSERT OR IGNORE INTO "):
		rest = strings.TrimSpace(trimmed[len("INSERT OR IGNORE INTO "):])
	case strings.HasPrefix(upper, "UPDATE "):
		rest = strings.TrimSpace(trimmed[len("UPDATE "):])
	case strings.HasPrefix(upper, "DELETE FROM "):
		rest = strings.TrimSpace(trimmed[len("DELETE FROM "):])
	default:
		if idx := strings.Index(upper, " FROM "); idx != -1 {
			rest = strings.TrimSpace(trimmed[idx+6:])
		}
	}
	if rest == "" {
		return ""
	}
	tok := strings.ToLower(strings.Trim(readToken(rest), "\"'`("))
	if dot := strings.LastIndexByte(tok, '.'); dot != -1 {
		tok = tok[dot+1:]
	}
	return tok
}

// NormalizeSQLWhitespace strips SQL comments (-- and /* */) and collapses all newlines,
// tabs, and multi-space indentation outside of string literals into clean single spaces.
func NormalizeSQLWhitespace(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))

	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	lastWasSpace := true

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
				if !lastWasSpace {
					out.WriteByte(' ')
					lastWasSpace = true
				}
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inBlockComment = false
				i++
				if !lastWasSpace {
					out.WriteByte(' ')
					lastWasSpace = true
				}
			}
			continue
		}

		if !inSingle && !inDouble {
			if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
				inLineComment = true
				i++
				continue
			}
			if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
				inBlockComment = true
				i++
				continue
			}
			if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
				if !lastWasSpace {
					out.WriteByte(' ')
					lastWasSpace = true
				}
				continue
			}
		}

		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		out.WriteByte(ch)
		lastWasSpace = false
	}
	return strings.TrimSpace(out.String())
}

// SplitSQLStatements splits a multi-statement SQL script (separated by ';') into individual statements,
// respecting string literals, comments, and CREATE TRIGGER BEGIN ... END blocks.
func SplitSQLStatements(rawSQL string) []string {
	var stmts []string
	var cur strings.Builder

	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	inTriggerBlock := false

	upperRaw := strings.ToUpper(rawSQL)
	if strings.Contains(upperRaw, "CREATE TRIGGER") {
		inTriggerBlock = true
	}

	for i := 0; i < len(rawSQL); i++ {
		ch := rawSQL[i]
		if inLineComment {
			cur.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			cur.WriteByte(ch)
			if ch == '*' && i+1 < len(rawSQL) && rawSQL[i+1] == '/' {
				cur.WriteByte('/')
				inBlockComment = false
				i++
			}
			continue
		}
		if !inSingle && !inDouble {
			if ch == '-' && i+1 < len(rawSQL) && rawSQL[i+1] == '-' {
				inLineComment = true
				cur.WriteString("--")
				i++
				continue
			}
			if ch == '/' && i+1 < len(rawSQL) && rawSQL[i+1] == '*' {
				inBlockComment = true
				cur.WriteString("/*")
				i++
				continue
			}
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if ch == ';' && !inSingle && !inDouble && !inTriggerBlock {
			s := strings.TrimSpace(cur.String())
			if NormalizeSQLWhitespace(s) != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
			continue
		}
		cur.WriteByte(ch)
	}
	if rem := strings.TrimSpace(cur.String()); NormalizeSQLWhitespace(rem) != "" {
		stmts = append(stmts, rem)
	}
	return stmts
}


