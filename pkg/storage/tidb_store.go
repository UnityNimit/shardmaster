package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"shardmaster/pkg/hash"
)

// TiDBConfig holds connection settings for a TiDB Cloud (or TiDB MySQL-compatible) cluster.
type TiDBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	DSN      string
	UseTLS   bool
}

// Enabled reports whether TiDB connection parameters are configured.
func (c TiDBConfig) Enabled() bool {
	if strings.TrimSpace(c.DSN) != "" {
		return true
	}
	return strings.TrimSpace(c.Host) != "" && strings.TrimSpace(c.User) != ""
}

// LoadTiDBConfigFromEnv resolves TiDB Cloud configuration from environment variables
// (and an optional local .env file if present). Zero cluster data is written to local disk.
func LoadTiDBConfigFromEnv() TiDBConfig {
	envMap := loadDotEnvFallback()
	getVal := func(keys ...string) string {
		for _, k := range keys {
			if v := strings.TrimSpace(os.Getenv(k)); v != "" {
				return v
			}
			if v := strings.TrimSpace(envMap[k]); v != "" {
				return v
			}
		}
		return ""
	}

	port := 4000
	if pStr := getVal("TIDB_PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
			port = p
		}
	}
	dbName := getVal("TIDB_DB", "TIDB_DATABASE")
	if dbName == "" {
		dbName = "shardmaster"
	}
	useTLS := true
	if tlsStr := strings.ToLower(getVal("TIDB_TLS")); tlsStr == "false" || tlsStr == "0" || tlsStr == "disable" {
		useTLS = false
	}

	return TiDBConfig{
		Host:     getVal("TIDB_HOST"),
		Port:     port,
		User:     getVal("TIDB_USER", "TIDB_USERNAME"),
		Password: getVal("TIDB_PASSWORD", "TIDB_PASS"),
		Database: dbName,
		DSN:      getVal("TIDB_DSN"),
		UseTLS:   useTLS,
	}
}

func loadDotEnvFallback() map[string]string {
	res := make(map[string]string)
	candidates := []string{".env"}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), ".env"))
	}
	for _, path := range candidates {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(string(raw), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			eq := strings.IndexByte(line, '=')
			if eq <= 0 {
				continue
			}
			k := strings.TrimSpace(line[:eq])
			v := strings.TrimSpace(line[eq+1:])
			v = strings.Trim(v, `"'`)
			if _, exists := res[k]; !exists {
				res[k] = v
			}
		}
	}
	return res
}

// TiDBStore persists all shard metadata, virtual bucket routing, user rows, custom SQL tables,
// and CDC mutation logs directly inside TiDB Cloud with zero local disk persistence.
type TiDBStore struct {
	cfg    TiDBConfig
	db     *sql.DB
	dbName string

	syncMu          sync.Mutex
	lastBuckets     [hash.TotalVirtualBuckets]uint32
	lastDeltaHash   [hash.TotalVirtualBuckets][32]byte
	lastHasSnapshot bool
	lastShardIDs    map[uint32]bool
	lastSyncedLSN   map[uint32]uint64
	usersSeeded     bool
	lastSyncTime    time.Time
	lastError       string
}

// NewTiDBStore connects to TiDB Cloud over TLS, ensures the target database & tables exist,
// and returns an active remote storage store.
func NewTiDBStore(cfg TiDBConfig) (*TiDBStore, error) {
	if !cfg.Enabled() {
		return nil, fmt.Errorf("TiDB configuration missing TIDB_HOST/TIDB_USER or TIDB_DSN")
	}
	dbName := cfg.Database
	if dbName == "" {
		dbName = "shardmaster"
	}

	buildDSN := func(targetDB string) (string, error) {
		if strings.TrimSpace(cfg.DSN) != "" {
			mc, err := mysql.ParseDSN(cfg.DSN)
			if err != nil {
				return "", err
			}
			if targetDB != "" {
				mc.DBName = targetDB
			}
			mc.ParseTime = true
			mc.Loc = time.UTC
			mc.InterpolateParams = true
			return mc.FormatDSN(), nil
		}
		port := cfg.Port
		if port <= 0 {
			port = 4000
		}
		mc := mysql.NewConfig()
		mc.User = cfg.User
		mc.Passwd = cfg.Password
		mc.Net = "tcp"
		mc.Addr = fmt.Sprintf("%s:%d", cfg.Host, port)
		mc.DBName = targetDB
		if cfg.UseTLS {
			mc.TLSConfig = "true"
		}
		mc.ParseTime = true
		mc.Loc = time.UTC
		mc.InterpolateParams = true
		mc.Timeout = 12 * time.Second
		mc.ReadTimeout = 30 * time.Second
		mc.WriteTimeout = 30 * time.Second
		mc.Params = map[string]string{
			"charset": "utf8mb4",
		}
		return mc.FormatDSN(), nil
	}

	// 1. Connect without DBName first (if using host/user) to create the database cleanly.
	if strings.TrimSpace(cfg.DSN) == "" {
		adminDSN, err := buildDSN("")
		if err != nil {
			return nil, fmt.Errorf("invalid TiDB DSN: %w", err)
		}
		adminDB, err := sql.Open("mysql", adminDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open TiDB connection: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		err = adminDB.PingContext(ctx)
		if err == nil {
			_, err = adminDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", sanitizeIdent(dbName)))
		}
		cancel()
		_ = adminDB.Close()
		if err != nil {
			return nil, fmt.Errorf("TiDB Cloud handshake/database setup failed: %w", err)
		}
	}

	// 2. Connect directly to the target database.
	dsn, err := buildDSN(dbName)
	if err != nil {
		return nil, fmt.Errorf("invalid TiDB database DSN: %w", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open TiDB database pool: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping TiDB database %q: %w", dbName, err)
	}

	store := &TiDBStore{
		cfg:           cfg,
		db:            db,
		dbName:        dbName,
		lastShardIDs:  make(map[uint32]bool),
		lastSyncedLSN: make(map[uint32]uint64),
	}
	if err := store.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "shardmaster"
	}
	return b.String()
}

func (ts *TiDBStore) Close() error {
	if ts.db != nil {
		return ts.db.Close()
	}
	return nil
}

func (ts *TiDBStore) Host() string {
	if ts.cfg.Host != "" {
		return ts.cfg.Host
	}
	return "tidb-cloud"
}

func (ts *TiDBStore) StatusInfo() (time.Time, string) {
	ts.syncMu.Lock()
	defer ts.syncMu.Unlock()
	return ts.lastSyncTime, ts.lastError
}

func (ts *TiDBStore) ensureSchema(ctx context.Context) error {
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS shardmaster_shards (
			shard_id INT NOT NULL PRIMARY KEY,
			custom_alias VARCHAR(128) NOT NULL,
			port INT NOT NULL,
			region VARCHAR(64) NOT NULL,
			max_capacity_bytes BIGINT NOT NULL,
			slab_bytes_per_bucket INT NOT NULL,
			disk_capacity_gb INT NOT NULL,
			hardware_tier VARCHAR(64) NOT NULL,
			weight INT NOT NULL,
			target_buckets INT NOT NULL,
			access_mode VARCHAR(32) NOT NULL,
			replication_mode VARCHAR(32) NOT NULL,
			max_connections INT NOT NULL,
			buffer_pool_bytes BIGINT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			used_bytes BIGINT NOT NULL DEFAULT 0,
			lsn_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
			cdc_journal_json LONGTEXT,
			updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
		)`,
		`CREATE TABLE IF NOT EXISTS shardmaster_buckets (
			bucket_id SMALLINT NOT NULL PRIMARY KEY,
			owner_shard_id INT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			delta_json LONGTEXT,
			updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			INDEX idx_owner_shard (owner_shard_id)
		)`,
		`CREATE TABLE IF NOT EXISTS shardmaster_users (
			user_id BIGINT NOT NULL PRIMARY KEY,
			shard_id INT NOT NULL,
			bucket_id SMALLINT NOT NULL,
			user_key VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			email VARCHAR(192) NOT NULL,
			tenant_id VARCHAR(64) NOT NULL,
			region VARCHAR(64) NOT NULL,
			balance_cents BIGINT NOT NULL,
			created_at DATETIME(6) NOT NULL,
			updated_at DATETIME(6) NOT NULL,
			INDEX idx_shard_bucket (shard_id, bucket_id)
		)`,
		`CREATE TABLE IF NOT EXISTS shardmaster_custom_rows (
			table_name VARCHAR(128) NOT NULL,
			row_key VARCHAR(192) NOT NULL,
			shard_id INT NOT NULL,
			bucket_id SMALLINT NOT NULL,
			shard_key VARCHAR(192) NOT NULL,
			columns_json LONGTEXT NOT NULL,
			byte_size BIGINT NOT NULL,
			updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			PRIMARY KEY (table_name, row_key),
			INDEX idx_custom_shard (shard_id, bucket_id)
		)`,
		`CREATE TABLE IF NOT EXISTS shardmaster_cdc_log (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			shard_id INT NOT NULL,
			lsn BIGINT UNSIGNED NOT NULL,
			bucket_id SMALLINT NOT NULL,
			op_type VARCHAR(32) NOT NULL,
			user_id BIGINT NOT NULL,
			summary VARCHAR(255) NOT NULL,
			created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
			UNIQUE KEY uk_shard_lsn (shard_id, lsn)
		)`,
	}
	for _, ddl := range ddls {
		if _, err := ts.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("failed creating TiDB schema table: %w", err)
		}
	}
	return nil
}

// EnsureInitialSeedInTiDB uploads the seeded 10,000 users and initial cluster state
// to TiDB Cloud if the database is brand new (0 rows in shardmaster_shards).
func (ts *TiDBStore) EnsureInitialSeedInTiDB(cs *ClusterStorage, buckets [hash.TotalVirtualBuckets]uint32) error {
	ts.syncMu.Lock()
	defer ts.syncMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var shardCount int
	if err := ts.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM shardmaster_shards").Scan(&shardCount); err != nil {
		ts.lastError = err.Error()
		return err
	}
	if shardCount > 0 {
		ts.usersSeeded = true
		return nil
	}

	// Fresh TiDB database: upload all initial shards, 1,024 buckets, and initial seeded users
	state := cs.buildClusterStateSnapshot(buckets)
	if err := ts.pushStateLocked(ctx, cs, state, true); err != nil {
		ts.lastError = err.Error()
		return err
	}

	if err := ts.bulkUploadAllUsersLocked(ctx, cs, buckets); err != nil {
		ts.lastError = err.Error()
		return err
	}
	ts.usersSeeded = true
	ts.lastSyncTime = time.Now().UTC()
	ts.lastError = ""
	return nil
}

func (ts *TiDBStore) bulkUploadAllUsersLocked(ctx context.Context, cs *ClusterStorage, buckets [hash.TotalVirtualBuckets]uint32) error {
	var userCount int
	if err := ts.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM shardmaster_users").Scan(&userCount); err == nil && userCount > 0 {
		return nil
	}

	var allRows []struct {
		shardID uint32
		row     UserRow
	}
	for _, s := range cs.GetAllShards() {
		for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
			if buckets[b] != s.ShardID {
				continue
			}
			rows := s.scanBucketRows(b)
			for _, r := range rows {
				allRows = append(allRows, struct {
					shardID uint32
					row     UserRow
				}{shardID: s.ShardID, row: r})
			}
		}
	}

	const batchSize = 250
	for start := 0; start < len(allRows); start += batchSize {
		end := start + batchSize
		if end > len(allRows) {
			end = len(allRows)
		}
		batch := allRows[start:end]
		var sb strings.Builder
		sb.WriteString("INSERT IGNORE INTO shardmaster_users (user_id, shard_id, bucket_id, user_key, name, email, tenant_id, region, balance_cents, created_at, updated_at) VALUES ")
		args := make([]any, 0, len(batch)*11)
		for i, item := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
			r := item.row
			created := r.CreatedAt.UTC()
			if created.IsZero() {
				created = time.Now().UTC()
			}
			updated := r.UpdatedAt.UTC()
			if updated.IsZero() {
				updated = created
			}
			args = append(args, r.UserID, item.shardID, r.BucketID, r.UserKey, r.Name, r.Email, r.TenantID, r.Region, r.BalanceCents, created, updated)
		}
		if _, err := ts.db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("failed batch uploading users to TiDB: %w", err)
		}
	}
	return nil
}

// SaveState pushes changed shards, bucket mappings, deltas, custom rows, and CDC events to TiDB Cloud.
func (ts *TiDBStore) SaveState(cs *ClusterStorage, state PersistedClusterState) error {
	ts.syncMu.Lock()
	defer ts.syncMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !ts.usersSeeded {
		var cnt int
		if err := ts.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM shardmaster_users").Scan(&cnt); err == nil {
			if cnt == 0 {
				_ = ts.bulkUploadAllUsersLocked(ctx, cs, state.Buckets)
			}
			ts.usersSeeded = true
		}
	}

	err := ts.pushStateLocked(ctx, cs, state, false)
	if err != nil {
		ts.lastError = err.Error()
		return err
	}
	ts.lastSyncTime = time.Now().UTC()
	ts.lastError = ""
	return nil
}

func (ts *TiDBStore) pushStateLocked(ctx context.Context, cs *ClusterStorage, state PersistedClusterState, forceAllBuckets bool) error {
	// 1. Upsert all active shards
	activeSet := make(map[uint32]bool, len(state.Shards))
	deltaByBucket := make(map[uint16]PersistedBucketDelta)

	for _, sm := range state.Shards {
		activeSet[sm.ShardID] = true

		// Compute live usedBytes for telemetry
		var usedBytes int64
		if sh, ok := cs.GetShard(sm.ShardID); ok {
			usedBytes = sh.UsedMemoryBytes()
		}

		var cdcJSON []byte
		if len(sm.CDCJournal) > 0 {
			// Keep most recent 250 entries in shard metadata column so row stays compact
			tail := sm.CDCJournal
			if len(tail) > 250 {
				tail = tail[len(tail)-250:]
			}
			cdcJSON, _ = json.Marshal(tail)
		}

		const shardUpsert = `INSERT INTO shardmaster_shards (
			shard_id, custom_alias, port, region, max_capacity_bytes, slab_bytes_per_bucket,
			disk_capacity_gb, hardware_tier, weight, target_buckets, access_mode, replication_mode,
			max_connections, buffer_pool_bytes, row_count, used_bytes, lsn_seq, cdc_journal_json
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE
			custom_alias=VALUES(custom_alias),
			port=VALUES(port),
			region=VALUES(region),
			max_capacity_bytes=VALUES(max_capacity_bytes),
			slab_bytes_per_bucket=VALUES(slab_bytes_per_bucket),
			disk_capacity_gb=VALUES(disk_capacity_gb),
			hardware_tier=VALUES(hardware_tier),
			weight=VALUES(weight),
			target_buckets=VALUES(target_buckets),
			access_mode=VALUES(access_mode),
			replication_mode=VALUES(replication_mode),
			max_connections=VALUES(max_connections),
			buffer_pool_bytes=VALUES(buffer_pool_bytes),
			row_count=VALUES(row_count),
			used_bytes=VALUES(used_bytes),
			lsn_seq=VALUES(lsn_seq),
			cdc_journal_json=VALUES(cdc_journal_json)`

		port := sm.Settings.CustomPort
		if port <= 0 {
			port = 5432 + int(sm.ShardID)
		}
		_, err := ts.db.ExecContext(ctx, shardUpsert,
			sm.ShardID, sm.Settings.CustomAlias, port, sm.Settings.Region,
			sm.Settings.MaxCapacityBytes, sm.Settings.SlabBytesPerBucket,
			sm.Settings.DiskCapacityGB, sm.Settings.HardwareTier,
			sm.Settings.Weight, sm.Settings.TargetBuckets,
			sm.Settings.AccessMode, sm.Settings.ReplicationMode,
			sm.Settings.MaxConnections, sm.Settings.BufferPoolBytes,
			sm.RowCount, usedBytes, sm.LSNSeq, string(cdcJSON),
		)
		if err != nil {
			return fmt.Errorf("failed upserting shard %d in TiDB: %w", sm.ShardID, err)
		}

		for _, bd := range sm.BucketData {
			deltaByBucket[bd.BucketID&hash.BucketMask] = bd
		}

		// Sync new CDC log events
		lastLSN := ts.lastSyncedLSN[sm.ShardID]
		var maxLSN uint64 = lastLSN
		for _, ev := range sm.CDCJournal {
			if ev.LSN <= lastLSN {
				continue
			}
			opName := "UPSERT"
			summary := fmt.Sprintf("user_id=%d", ev.UserID)
			switch ev.Op {
			case MutationDelete:
				opName = "DELETE"
			case MutationCustomUpsert:
				opName = "CUSTOM_UPSERT"
				summary = fmt.Sprintf("%s[%s]", ev.Custom.TableName, ev.Custom.RowKey)
			case MutationCustomDelete:
				opName = "CUSTOM_DELETE"
				summary = fmt.Sprintf("%s[%s]", ev.Custom.TableName, ev.Custom.RowKey)
			}
			_, _ = ts.db.ExecContext(ctx,
				`INSERT IGNORE INTO shardmaster_cdc_log (shard_id, lsn, bucket_id, op_type, user_id, summary) VALUES (?,?,?,?,?,?)`,
				sm.ShardID, ev.LSN, ev.BucketID, opName, ev.UserID, summary,
			)
			if ev.LSN > maxLSN {
				maxLSN = ev.LSN
			}
		}
		ts.lastSyncedLSN[sm.ShardID] = maxLSN
	}

	// 2. Remove deleted shards from TiDB
	if len(activeSet) > 0 {
		idList := make([]string, 0, len(activeSet))
		for id := range activeSet {
			idList = append(idList, strconv.FormatUint(uint64(id), 10))
		}
		_, err := ts.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM shardmaster_shards WHERE shard_id NOT IN (%s)", strings.Join(idList, ",")))
		if err != nil {
			return fmt.Errorf("failed pruning deleted shards in TiDB: %w", err)
		}
	}
	ts.lastShardIDs = activeSet

	// 3. Diff buckets so we only send changed buckets & changed bucket owners
	type bucketUpdate struct {
		bucketID  uint16
		ownerID   uint32
		rowCount  int64
		deltaJSON string
		deltaHash [32]byte
		hasDelta  bool
		delta     PersistedBucketDelta
	}

	var changed []bucketUpdate
	movedByTarget := make(map[uint32][]uint16)

	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		owner := state.Buckets[b]
		var dJSON string
		bd, hasDelta := deltaByBucket[b]
		if hasDelta {
			raw, _ := json.Marshal(bd)
			dJSON = string(raw)
		}
		dHash := sha256.Sum256([]byte(dJSON))

		var bRowCount int64
		if sh, ok := cs.GetShard(owner); ok {
			sh.bucketMu[b].RLock()
			if sh.buckets[b] != nil {
				bRowCount = sh.buckets[b].RowCount
			}
			sh.bucketMu[b].RUnlock()
		}

		ownerChanged := !ts.lastHasSnapshot || forceAllBuckets || ts.lastBuckets[b] != owner
		deltaChanged := !ts.lastHasSnapshot || forceAllBuckets || ts.lastDeltaHash[b] != dHash

		if ownerChanged || deltaChanged {
			changed = append(changed, bucketUpdate{
				bucketID:  b,
				ownerID:   owner,
				rowCount:  bRowCount,
				deltaJSON: dJSON,
				deltaHash: dHash,
				hasDelta:  hasDelta,
				delta:     bd,
			})
			if ts.lastHasSnapshot && ts.lastBuckets[b] != owner {
				movedByTarget[owner] = append(movedByTarget[owner], b)
			}
		}
	}

	// Batch upsert changed buckets in chunks of 128
	const bucketBatch = 128
	for start := 0; start < len(changed); start += bucketBatch {
		end := start + bucketBatch
		if end > len(changed) {
			end = len(changed)
		}
		batch := changed[start:end]
		var sb strings.Builder
		sb.WriteString("INSERT INTO shardmaster_buckets (bucket_id, owner_shard_id, row_count, delta_json) VALUES ")
		args := make([]any, 0, len(batch)*4)
		for i, item := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?,?,?)")
			args = append(args, item.bucketID, item.ownerID, item.rowCount, item.deltaJSON)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE owner_shard_id=VALUES(owner_shard_id), row_count=VALUES(row_count), delta_json=VALUES(delta_json)")
		if _, err := ts.db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("failed syncing buckets to TiDB: %w", err)
		}
	}

	// 4. Update shard_id in shardmaster_users & shardmaster_custom_rows for rebalanced buckets
	for targetShardID, bList := range movedByTarget {
		if len(bList) == 0 {
			continue
		}
		bStrs := make([]string, len(bList))
		for i, bid := range bList {
			bStrs[i] = strconv.Itoa(int(bid))
		}
		inClause := strings.Join(bStrs, ",")
		if _, err := ts.db.ExecContext(ctx, fmt.Sprintf("UPDATE shardmaster_users SET shard_id = ? WHERE bucket_id IN (%s)", inClause), targetShardID); err != nil {
			return fmt.Errorf("failed updating user shard placement in TiDB: %w", err)
		}
		if _, err := ts.db.ExecContext(ctx, fmt.Sprintf("UPDATE shardmaster_custom_rows SET shard_id = ? WHERE bucket_id IN (%s)", inClause), targetShardID); err != nil {
			return fmt.Errorf("failed updating custom row shard placement in TiDB: %w", err)
		}
	}

	// 5. Sync changed bucket deltas (user inserts/updates/deletes and custom table rows)
	for _, item := range changed {
		if !ts.lastHasSnapshot && !item.hasDelta {
			continue
		}
		if ts.lastHasSnapshot && ts.lastDeltaHash[item.bucketID] == item.deltaHash {
			continue
		}
		bd := item.delta
		for _, r := range bd.DeltaOverrides {
			created := r.CreatedAt.UTC()
			if created.IsZero() {
				created = time.Now().UTC()
			}
			updated := r.UpdatedAt.UTC()
			if updated.IsZero() {
				updated = created
			}
			_, err := ts.db.ExecContext(ctx, `INSERT INTO shardmaster_users
				(user_id, shard_id, bucket_id, user_key, name, email, tenant_id, region, balance_cents, created_at, updated_at)
				VALUES (?,?,?,?,?,?,?,?,?,?,?)
				ON DUPLICATE KEY UPDATE
				shard_id=VALUES(shard_id), bucket_id=VALUES(bucket_id), user_key=VALUES(user_key),
				name=VALUES(name), email=VALUES(email), tenant_id=VALUES(tenant_id),
				region=VALUES(region), balance_cents=VALUES(balance_cents), updated_at=VALUES(updated_at)`,
				r.UserID, item.ownerID, item.bucketID, r.UserKey, r.Name, r.Email, r.TenantID, r.Region, r.BalanceCents, created, updated,
			)
			if err != nil {
				return fmt.Errorf("failed upserting user %d to TiDB: %w", r.UserID, err)
			}
		}
		for uid, deleted := range bd.DeletedIDs {
			if deleted {
				_, _ = ts.db.ExecContext(ctx, "DELETE FROM shardmaster_users WHERE user_id = ?", uid)
			}
		}

		// Sync custom table rows in this bucket
		_, _ = ts.db.ExecContext(ctx, "DELETE FROM shardmaster_custom_rows WHERE bucket_id = ?", item.bucketID)
		for tblName, rowsMap := range bd.CustomTables {
			for _, cr := range rowsMap {
				colsJSON, _ := json.Marshal(cr.Columns)
				_, err := ts.db.ExecContext(ctx, `INSERT INTO shardmaster_custom_rows
					(table_name, row_key, shard_id, bucket_id, shard_key, columns_json, byte_size)
					VALUES (?,?,?,?,?,?,?)
					ON DUPLICATE KEY UPDATE
					shard_id=VALUES(shard_id), bucket_id=VALUES(bucket_id), shard_key=VALUES(shard_key),
					columns_json=VALUES(columns_json), byte_size=VALUES(byte_size)`,
					tblName, cr.RowKey, item.ownerID, item.bucketID, cr.ShardKey, string(colsJSON), cr.ByteSize,
				)
				if err != nil {
					return fmt.Errorf("failed upserting custom row %s[%s] to TiDB: %w", tblName, cr.RowKey, err)
				}
			}
		}
	}

	// Update local cache
	ts.lastBuckets = state.Buckets
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		if bd, ok := deltaByBucket[b]; ok {
			raw, _ := json.Marshal(bd)
			ts.lastDeltaHash[b] = sha256.Sum256(raw)
		} else {
			ts.lastDeltaHash[b] = sha256.Sum256(nil)
		}
	}
	ts.lastHasSnapshot = true
	return nil
}

// LoadState loads the full persisted cluster state directly from TiDB Cloud.
func (ts *TiDBStore) LoadState() (PersistedClusterState, bool, error) {
	ts.syncMu.Lock()
	defer ts.syncMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var state PersistedClusterState

	// 1. Load shard metadata
	rows, err := ts.db.QueryContext(ctx, `SELECT
		shard_id, custom_alias, port, region, max_capacity_bytes, slab_bytes_per_bucket,
		disk_capacity_gb, hardware_tier, weight, target_buckets, access_mode, replication_mode,
		max_connections, buffer_pool_bytes, row_count, lsn_seq, COALESCE(cdc_journal_json, '')
		FROM shardmaster_shards ORDER BY shard_id ASC`)
	if err != nil {
		ts.lastError = err.Error()
		return state, false, err
	}
	defer rows.Close()

	var shardsMeta []PersistedShardMeta
	shardIdxByID := make(map[uint32]int)

	for rows.Next() {
		var sid uint32
		var cfg ShardSettings
		var rowCount int64
		var lsnSeq uint64
		var cdcRaw string
		if err := rows.Scan(
			&sid, &cfg.CustomAlias, &cfg.CustomPort, &cfg.Region,
			&cfg.MaxCapacityBytes, &cfg.SlabBytesPerBucket, &cfg.DiskCapacityGB,
			&cfg.HardwareTier, &cfg.Weight, &cfg.TargetBuckets, &cfg.AccessMode,
			&cfg.ReplicationMode, &cfg.MaxConnections, &cfg.BufferPoolBytes,
			&rowCount, &lsnSeq, &cdcRaw,
		); err != nil {
			return state, false, err
		}
		if cfg.BufferPoolBytes > 0 {
			cfg.BufferPoolMB = int(cfg.BufferPoolBytes / (1024 * 1024))
		}
		var cdcJournal []MutationLogEntry
		if strings.TrimSpace(cdcRaw) != "" {
			_ = json.Unmarshal([]byte(cdcRaw), &cdcJournal)
		}
		shardIdxByID[sid] = len(shardsMeta)
		shardsMeta = append(shardsMeta, PersistedShardMeta{
			ShardID:    sid,
			Region:     cfg.Region,
			RowCount:   rowCount,
			Settings:   cfg,
			CDCJournal: cdcJournal,
			LSNSeq:     lsnSeq,
		})
	}
	if err := rows.Err(); err != nil {
		return state, false, err
	}
	if len(shardsMeta) == 0 {
		return state, false, nil
	}

	// 2. Load bucket assignments & delta JSON
	bRows, err := ts.db.QueryContext(ctx, `SELECT bucket_id, owner_shard_id, COALESCE(delta_json, '') FROM shardmaster_buckets ORDER BY bucket_id ASC`)
	if err != nil {
		return state, false, err
	}
	defer bRows.Close()

	var deltaHashCache [hash.TotalVirtualBuckets][32]byte
	for bRows.Next() {
		var bid uint16
		var owner uint32
		var dRaw string
		if err := bRows.Scan(&bid, &owner, &dRaw); err != nil {
			return state, false, err
		}
		b := bid & hash.BucketMask
		state.Buckets[b] = owner
		deltaHashCache[b] = sha256.Sum256([]byte(dRaw))

		if strings.TrimSpace(dRaw) != "" {
			var bd PersistedBucketDelta
			if err := json.Unmarshal([]byte(dRaw), &bd); err == nil {
				if idx, ok := shardIdxByID[owner]; ok {
					shardsMeta[idx].BucketData = append(shardsMeta[idx].BucketData, bd)
				}
			}
		}
	}
	if err := bRows.Err(); err != nil {
		return state, false, err
	}

	state.NumShards = len(shardsMeta)
	state.Shards = shardsMeta
	state.UpdatedAt = time.Now().UTC()

	ts.lastBuckets = state.Buckets
	ts.lastDeltaHash = deltaHashCache
	ts.lastHasSnapshot = true
	ts.lastShardIDs = make(map[uint32]bool, len(shardsMeta))
	for _, sm := range shardsMeta {
		ts.lastShardIDs[sm.ShardID] = true
		ts.lastSyncedLSN[sm.ShardID] = sm.LSNSeq
	}
	ts.usersSeeded = true
	ts.lastSyncTime = time.Now().UTC()
	ts.lastError = ""
	return state, true, nil
}

// PullShardSettingsFromTiDB fetches the latest shard metadata rows from TiDB Cloud
// so edits performed directly inside TiDB Cloud SQL Editor sync into live ShardMaster shards.
func (ts *TiDBStore) PullShardSettingsFromTiDB(cs *ClusterStorage) (int, error) {
	ts.syncMu.Lock()
	defer ts.syncMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	rows, err := ts.db.QueryContext(ctx, `SELECT
		shard_id, custom_alias, port, region, max_capacity_bytes, slab_bytes_per_bucket,
		disk_capacity_gb, hardware_tier, weight, target_buckets, access_mode, replication_mode,
		max_connections, buffer_pool_bytes
		FROM shardmaster_shards ORDER BY shard_id ASC`)
	if err != nil {
		ts.lastError = err.Error()
		return 0, err
	}
	defer rows.Close()

	updatedCount := 0
	for rows.Next() {
		var sid uint32
		var cfg ShardSettings
		if err := rows.Scan(
			&sid, &cfg.CustomAlias, &cfg.CustomPort, &cfg.Region,
			&cfg.MaxCapacityBytes, &cfg.SlabBytesPerBucket, &cfg.DiskCapacityGB,
			&cfg.HardwareTier, &cfg.Weight, &cfg.TargetBuckets, &cfg.AccessMode,
			&cfg.ReplicationMode, &cfg.MaxConnections, &cfg.BufferPoolBytes,
		); err != nil {
			return updatedCount, err
		}
		if cfg.BufferPoolBytes > 0 {
			cfg.BufferPoolMB = int(cfg.BufferPoolBytes / (1024 * 1024))
		}
		sh := cs.EnsureShard(sid, cfg.Region)
		prev := sh.GetSettings()
		if prev.CustomAlias != cfg.CustomAlias ||
			prev.CustomPort != cfg.CustomPort ||
			prev.Region != cfg.Region ||
			prev.MaxCapacityBytes != cfg.MaxCapacityBytes ||
			prev.SlabBytesPerBucket != cfg.SlabBytesPerBucket ||
			prev.HardwareTier != cfg.HardwareTier ||
			prev.Weight != cfg.Weight ||
			prev.TargetBuckets != cfg.TargetBuckets ||
			prev.AccessMode != cfg.AccessMode ||
			prev.ReplicationMode != cfg.ReplicationMode ||
			prev.MaxConnections != cfg.MaxConnections ||
			prev.BufferPoolBytes != cfg.BufferPoolBytes {
			sh.UpdateSettings(cfg)
			updatedCount++
		}
	}
	ts.lastSyncTime = time.Now().UTC()
	ts.lastError = ""
	return updatedCount, rows.Err()
}
