package router

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ColumnSchema describes a single column in a ShardMaster table.
type ColumnSchema struct {
	Name            string
	DataType        string
	Nullable        string
	KeyConstraint   string
	DefaultValue    string
	StorageEncoding string
}

// IndexSchema describes a distributed or local index on a ShardMaster table.
type IndexSchema struct {
	TableName      string
	IndexName      string
	IndexType      string
	Columns        string
	RoutingScope   string
	Uniqueness     string
	ExecutionNotes string
}

// TableSchema describes a complete distributed or system catalog table.
type TableSchema struct {
	SchemaName       string
	TableName        string
	TableType        string
	ShardKey         string
	ShardingStrategy string
	VirtualBuckets   int
	StorageEngine    string
	Description      string
	Columns          []ColumnSchema
	Indexes          []IndexSchema
	CreateDDL        string
}

// SchemaCatalog manages built-in and user-created distributed table schemas.
type SchemaCatalog struct {
	mu          sync.RWMutex
	tables      map[string]*TableSchema
	orderedKeys []string
}

// NewSchemaCatalog initializes the built-in ShardMaster distributed tables and system catalog tables.
func NewSchemaCatalog() *SchemaCatalog {
	sc := &SchemaCatalog{
		tables: make(map[string]*TableSchema),
	}
	sc.registerBuiltins()
	return sc
}

func (sc *SchemaCatalog) registerTable(t *TableSchema) {
	key := strings.ToLower(t.TableName)
	if _, exists := sc.tables[key]; !exists {
		sc.orderedKeys = append(sc.orderedKeys, key)
	}
	sc.tables[key] = t
}

func (sc *SchemaCatalog) registerBuiltins() {
	sc.registerTable(&TableSchema{
		SchemaName:       "public",
		TableName:        "users",
		TableType:        "SHARDED TABLE",
		ShardKey:         "user_id (BIGINT)",
		ShardingStrategy: "HASH (xxHash64 & 1023)",
		VirtualBuckets:   1024,
		StorageEngine:    "Columnar Slab + Delta Overlay",
		Description:      "Primary distributed user accounts & balances table",
		Columns: []ColumnSchema{
			{"user_id", "BIGINT", "NOT NULL", "PRIMARY KEY (SHARD KEY)", "nextval('users_id_seq')", "64-Bit Integer Ring Key"},
			{"user_key", "VARCHAR(64)", "NOT NULL", "HASH RING KEY", "CAST(user_id AS TEXT)", "Inline UTF-8 Key"},
			{"name", "VARCHAR(128)", "NOT NULL", "NONE", "''", "Dictionary + Delta Overlay"},
			{"email", "VARCHAR(255)", "NOT NULL", "LOCAL INDEX", "''", "Trigram Indexed String"},
			{"tenant_id", "VARCHAR(64)", "NOT NULL", "LOCAL INDEX", "'tenant_core'", "Interned Low-Cardinality"},
			{"region", "VARCHAR(32)", "NOT NULL", "PARTITION KEY", "'us-west'", "Geo-Placement Tag"},
			{"balance_cents", "BIGINT", "NOT NULL", "COLUMNAR SLAB", "250000", "Pointer-Free []uint32 Slab"},
			{"balance_usd", "NUMERIC(12,2)", "NOT NULL", "GENERATED VIRTUAL", "(balance_cents / 100.0)", "Computed Currency Column"},
			{"bucket_id", "SMALLINT", "NOT NULL", "BUCKET INDEX [0..1023]", "(xxhash64(user_id) & 1023)", "10-Bit Virtual Bucket ID"},
			{"created_at", "TIMESTAMPTZ", "NOT NULL", "MERGE SORT KEY (DESC)", "CURRENT_TIMESTAMP", "64-Bit UTC Epoch Micros"},
			{"updated_at", "TIMESTAMPTZ", "NOT NULL", "CDC LSN TRACKED", "CURRENT_TIMESTAMP", "64-Bit UTC Epoch Micros"},
		},
		Indexes: []IndexSchema{
			{"users", "pk_users_user_id", "HASH_RING_PK", "user_id", "O(1) SINGLE SHARD", "UNIQUE", "Direct L1-Cache [1024]atomic.Uint32 routing (~18ns)"},
			{"users", "idx_users_bucket_id", "SLAB_BUCKET", "bucket_id, user_id", "BUCKET PARTITION", "NON-UNIQUE", "Lock-free Keyset Backfill & VDiff XOR-SHA256 scan"},
			{"users", "idx_users_created_at_desc", "BTREE_DESC", "created_at DESC, user_id DESC", "SCATTER K-WAY MERGE", "NON-UNIQUE", "Streaming Min-Heap K-Way Merge Sort across shards"},
			{"users", "idx_users_email_trgm", "GIN_TRGM", "email", "SCATTER-GATHER", "NON-UNIQUE", "Parallel Goroutine LIKE '%domain%' filter"},
			{"users", "idx_users_region_local", "BTREE_LOCAL", "region, tenant_id", "SHARD PRUNING", "NON-UNIQUE", "Prunes non-matching regional shards before scan"},
		},
		CreateDDL: strings.Join([]string{
			"CREATE TABLE public.users (",
			"    user_id       BIGINT        NOT NULL PRIMARY KEY,",
			"    user_key      VARCHAR(64)   NOT NULL DEFAULT CAST(user_id AS TEXT),",
			"    name          VARCHAR(128)  NOT NULL,",
			"    email         VARCHAR(255)  NOT NULL,",
			"    tenant_id     VARCHAR(64)   NOT NULL DEFAULT 'tenant_core',",
			"    region        VARCHAR(32)   NOT NULL DEFAULT 'us-west',",
			"    balance_cents BIGINT        NOT NULL DEFAULT 250000,",
			"    balance_usd   NUMERIC(12,2) GENERATED ALWAYS AS (balance_cents / 100.0) VIRTUAL,",
			"    bucket_id     SMALLINT      GENERATED ALWAYS AS (xxhash64(user_id) & 1023) STORED,",
			"    created_at    TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,",
			"    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP",
			") SHARD BY HASH (user_id) INTO 1024 VIRTUAL BUCKETS",
			"  WITH (storage_engine = 'columnar_slab_zero_gc', cdc_stream = 'enabled', vdiff = 'xor_sha256');",
		}, "\n"),
	})

	sc.registerTable(&TableSchema{
		SchemaName:       "shardmaster",
		TableName:        "_shardmaster_cdc",
		TableType:        "SYSTEM CDC LOG",
		ShardKey:         "bucket_id, lsn",
		ShardingStrategy: "SHARD LOCAL JOURNAL",
		VirtualBuckets:   1024,
		StorageEngine:    "Append-Only Ring Buffer",
		Description:      "Per-shard Change Data Capture (CDC) LSN mutation log for 0ms downtime resharding",
		Columns: []ColumnSchema{
			{"lsn", "BIGINT", "NOT NULL", "PRIMARY KEY (MONOTONIC)", "atomic_add(&lsn_seq, 1)", "64-Bit Per-Shard Sequence"},
			{"shard_id", "INTEGER", "NOT NULL", "PARTITION ORIGIN", "local_shard_id()", "Physical Shard Node ID"},
			{"bucket_id", "SMALLINT", "NOT NULL", "INDEX (idx_cdc_bkt_lsn)", "(xxhash64(user_id) & 1023)", "10-Bit Virtual Bucket ID"},
			{"op_type", "VARCHAR(16)", "NOT NULL", "NONE", "'UPSERT'", "Mutation Enum (UPSERT | DELETE)"},
			{"user_id", "BIGINT", "NOT NULL", "SHARD KEY REF", "0", "Mutated Row Primary Key"},
			{"email", "VARCHAR(255)", "NOT NULL", "NONE", "''", "After-Image Email Snapshot"},
			{"balance_usd", "NUMERIC(12,2)", "NOT NULL", "NONE", "0.00", "After-Image Balance Snapshot"},
			{"timestamp_us", "BIGINT", "NOT NULL", "NONE", "unix_micro()", "Wall-Clock UTC Microseconds"},
		},
		Indexes: []IndexSchema{
			{"_shardmaster_cdc", "pk_cdc_shard_lsn", "RING_SEQ_PK", "shard_id, lsn", "SHARD LOCAL", "UNIQUE", "Monotonic LSN watermark replication cursor"},
			{"_shardmaster_cdc", "idx_cdc_bucket_lsn", "BTREE_LOCAL", "bucket_id, lsn", "CDC CATCHUP STREAM", "NON-UNIQUE", "Streams in-flight mutations for migrating buckets"},
		},
		CreateDDL: strings.Join([]string{
			"CREATE TABLE shardmaster._shardmaster_cdc (",
			"    lsn          BIGINT        NOT NULL,",
			"    shard_id     INTEGER       NOT NULL,",
			"    bucket_id    SMALLINT      NOT NULL,",
			"    op_type      VARCHAR(16)   NOT NULL,",
			"    user_id      BIGINT        NOT NULL,",
			"    email        VARCHAR(255)  NOT NULL,",
			"    balance_usd  NUMERIC(12,2) NOT NULL,",
			"    timestamp_us BIGINT        NOT NULL,",
			"    PRIMARY KEY (shard_id, lsn)",
			") WITH (storage_engine = 'lock_free_cdc_journal', retention_entries = 32768);",
		}, "\n"),
	})

	sc.registerTable(&TableSchema{
		SchemaName:       "shardmaster",
		TableName:        "_shardmaster_buckets",
		TableType:        "SYSTEM DIRECTORY",
		ShardKey:         "bucket_id [0..1023]",
		ShardingStrategy: "L1 ATOMIC RING (4 KB)",
		VirtualBuckets:   1024,
		StorageEngine:    "[1024]atomic.Uint32 Array",
		Description:      "4 KB L1-cache resident virtual bucket ownership & state indirection table",
		Columns: []ColumnSchema{
			{"bucket_id", "SMALLINT", "NOT NULL", "PRIMARY KEY [0..1023]", "0", "10-Bit Ring Slot Index"},
			{"owner_shard", "INTEGER", "NOT NULL", "FOREIGN KEY (shards)", "0", "Atomic Physical Shard ID"},
			{"region", "VARCHAR(32)", "NOT NULL", "NONE", "'us-west'", "Owning Shard Region"},
			{"directory_state", "VARCHAR(24)", "NOT NULL", "NONE", "'READY'", "READY | CDC_STREAMING | CUTOVER_GATE"},
			{"ewma_qps", "BIGINT", "NOT NULL", "HOTSPOT TELEMETRY", "0", "64-Byte Padded EWMA Counter"},
			{"row_count", "BIGINT", "NOT NULL", "NONE", "0", "Live Rows in Bucket Slab"},
		},
		Indexes: []IndexSchema{
			{"_shardmaster_buckets", "pk_buckets_id", "DIRECT_ARRAY_O1", "bucket_id", "CONTROL PLANE L1", "UNIQUE", "Direct array indexing in ~18ns with 0 allocations"},
		},
		CreateDDL: strings.Join([]string{
			"CREATE TABLE shardmaster._shardmaster_buckets (",
			"    bucket_id       SMALLINT    NOT NULL PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 1023),",
			"    owner_shard     INTEGER     NOT NULL,",
			"    region          VARCHAR(32) NOT NULL,",
			"    directory_state VARCHAR(24) NOT NULL DEFAULT 'READY',",
			"    ewma_qps        BIGINT      NOT NULL DEFAULT 0,",
			"    row_count       BIGINT      NOT NULL DEFAULT 0",
			") WITH (storage_engine = 'l1_cache_atomic_uint32_ring', memory_bytes = 4096);",
		}, "\n"),
	})

	sc.registerTable(&TableSchema{
		SchemaName:       "shardmaster",
		TableName:        "_shardmaster_shards",
		TableType:        "SYSTEM TOPOLOGY",
		ShardKey:         "shard_id",
		ShardingStrategy: "CONTROL PLANE CATALOG",
		VirtualBuckets:   1024,
		StorageEngine:    "In-Memory Registry + JSON",
		Description:      "Physical PostgreSQL / Columnar shard nodes, ports, regions, QPS & LSN watermarks",
		Columns: []ColumnSchema{
			{"shard_id", "INTEGER", "NOT NULL", "PRIMARY KEY", "0", "Physical Shard Identifier"},
			{"port", "INTEGER", "NOT NULL", "UNIQUE", "5432", "PostgreSQL Wire Port"},
			{"region", "VARCHAR(32)", "NOT NULL", "INDEX", "'us-west'", "Availability Zone"},
			{"virtual_buckets", "INTEGER", "NOT NULL", "NONE", "256", "Assigned Virtual Buckets"},
			{"rows", "BIGINT", "NOT NULL", "NONE", "12500000", "Total Materialized Rows"},
			{"qps", "BIGINT", "NOT NULL", "NONE", "0", "Current Queries/Sec"},
			{"p99_latency_ms", "NUMERIC(8,2)", "NOT NULL", "NONE", "0.38", "EWMA P99 Latency (ms)"},
			{"cdc_lsn", "BIGINT", "NOT NULL", "NONE", "0", "Replication Log Sequence"},
		},
		Indexes: []IndexSchema{
			{"_shardmaster_shards", "pk_shards_id", "HASH_PK", "shard_id", "CONTROL PLANE", "UNIQUE", "Physical shard descriptor lookup"},
		},
		CreateDDL: strings.Join([]string{
			"CREATE TABLE shardmaster._shardmaster_shards (",
			"    shard_id        INTEGER      NOT NULL PRIMARY KEY,",
			"    port            INTEGER      NOT NULL UNIQUE,",
			"    region          VARCHAR(32)  NOT NULL,",
			"    virtual_buckets INTEGER      NOT NULL,",
			"    rows            BIGINT       NOT NULL,",
			"    qps             BIGINT       NOT NULL,",
			"    p99_latency_ms  NUMERIC(8,2) NOT NULL,",
			"    cdc_lsn         BIGINT       NOT NULL",
			") WITH (storage_engine = 'control_plane_topology');",
		}, "\n"),
	})

	sc.registerTable(&TableSchema{
		SchemaName:       "shardmaster",
		TableName:        "_shardmaster_vdiff",
		TableType:        "SYSTEM AUDIT",
		ShardKey:         "shard_id, bucket_range",
		ShardingStrategy: "PARALLEL MERKLE XOR",
		VirtualBuckets:   1024,
		StorageEngine:    "256-Bit Commutative XOR-SHA256",
		Description:      "Cryptographic VDiff bit-level parity verification audit table",
		Columns: []ColumnSchema{
			{"shard", "VARCHAR(32)", "NOT NULL", "PRIMARY KEY", "'Shard 0'", "Verified Physical Shard"},
			{"rows_hashed", "BIGINT", "NOT NULL", "NONE", "0", "Rows Included in Digest"},
			{"vdiff_xor_sha256_digest", "CHAR(64)", "NOT NULL", "CRYPTO DIGEST", "'0000...'", "256-Bit Commutative XOR-SHA256"},
			{"parity_status", "VARCHAR(24)", "NOT NULL", "NONE", "'VERIFIED_INTACT'", "Bit-Level Parity Verdict"},
		},
		Indexes: []IndexSchema{
			{"_shardmaster_vdiff", "pk_vdiff_shard", "BTREE_PK", "shard", "ALL SHARDS", "UNIQUE", "Per-shard cryptographic checksum index"},
		},
		CreateDDL: strings.Join([]string{
			"CREATE TABLE shardmaster._shardmaster_vdiff (",
			"    shard                   VARCHAR(32) NOT NULL PRIMARY KEY,",
			"    rows_hashed             BIGINT      NOT NULL,",
			"    vdiff_xor_sha256_digest CHAR(64)    NOT NULL,",
			"    parity_status           VARCHAR(24) NOT NULL",
			") WITH (checksum_algorithm = 'commutative_xor_sha256_256bit');",
		}, "\n"),
	})
}

// ListTables returns all registered tables in deterministic order.
func (sc *SchemaCatalog) ListTables() []*TableSchema {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	out := make([]*TableSchema, 0, len(sc.orderedKeys))
	for _, k := range sc.orderedKeys {
		if t, ok := sc.tables[k]; ok {
			out = append(out, t)
		}
	}
	return out
}

// GetTable looks up a table schema by name (stripping optional schema prefix like `public.`).
func (sc *SchemaCatalog) GetTable(name string) (*TableSchema, bool) {
	clean := strings.ToLower(strings.TrimSpace(name))
	clean = strings.Trim(clean, ";'\"` ")
	if dot := strings.LastIndexByte(clean, '.'); dot != -1 {
		clean = clean[dot+1:]
	}
	if clean == "" {
		clean = "users"
	}
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	t, ok := sc.tables[clean]
	return t, ok
}

// CreateCustomTable parses a `CREATE TABLE <name> (...)` SQL statement and registers it in the catalog.
func (sc *SchemaCatalog) CreateCustomTable(rawSQL string) (*TableSchema, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawSQL), ";"))
	upper := strings.ToUpper(trimmed)

	idx := strings.Index(upper, "TABLE")
	if idx == -1 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}
	rest := strings.TrimSpace(trimmed[idx+5:])
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[13:])
	}

	openParen := strings.IndexByte(rest, '(')
	var tableName string
	var body string
	if openParen != -1 {
		tableName = strings.TrimSpace(rest[:openParen])
		closeParen := strings.LastIndexByte(rest, ')')
		if closeParen > openParen {
			body = rest[openParen+1 : closeParen]
		}
	} else {
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return nil, fmt.Errorf("missing table name in CREATE TABLE")
		}
		tableName = fields[0]
	}

	schemaName := "public"
	if dot := strings.IndexByte(tableName, '.'); dot != -1 {
		schemaName = strings.Trim(tableName[:dot], "\"'` ")
		tableName = strings.Trim(tableName[dot+1:], "\"'` ")
	} else {
		tableName = strings.Trim(tableName, "\"'` ")
	}
	if tableName == "" {
		tableName = "custom_table"
	}

	var cols []ColumnSchema
	shardKey := "id (BIGINT)"
	if strings.TrimSpace(body) != "" {
		parts := splitTopLevelCommas(body)
		for i, p := range parts {
			line := strings.TrimSpace(p)
			if line == "" {
				continue
			}
			tokens := strings.Fields(line)
			if len(tokens) < 2 {
				continue
			}
			upFirst := strings.ToUpper(tokens[0])
			if upFirst == "PRIMARY" || upFirst == "CONSTRAINT" || upFirst == "UNIQUE" || upFirst == "INDEX" || upFirst == "FOREIGN" || upFirst == "CHECK" {
				continue
			}
			colName := strings.Trim(tokens[0], "\"'`")
			colType := strings.ToUpper(tokens[1])
			// If type was split across whitespace like "NUMERIC (12, 2)", recombine parenthesized precision
			if len(tokens) >= 3 && strings.HasPrefix(tokens[2], "(") && !strings.Contains(colType, "(") {
				colType += tokens[2]
			}
			upLine := strings.ToUpper(line)

			nullable := "NULL"
			if strings.Contains(upLine, "NOT NULL") || strings.Contains(upLine, "PRIMARY KEY") {
				nullable = "NOT NULL"
			}
			keyConst := "NONE"
			if strings.Contains(upLine, "PRIMARY KEY") || i == 0 {
				keyConst = "PRIMARY KEY (SHARD KEY)"
				shardKey = fmt.Sprintf("%s (%s)", colName, colType)
			} else if strings.Contains(upLine, "REFERENCES") {
				keyConst = "FOREIGN KEY"
			} else if strings.Contains(upLine, "UNIQUE") {
				keyConst = "UNIQUE"
			}
			defVal := "NULL"
			if defIdx := strings.Index(upLine, "DEFAULT "); defIdx != -1 {
				defRest := strings.TrimSpace(line[defIdx+8:])
				defFields := strings.Fields(defRest)
				if len(defFields) > 0 {
					defVal = defFields[0]
				}
			}
			cols = append(cols, ColumnSchema{
				Name:            colName,
				DataType:        colType,
				Nullable:        nullable,
				KeyConstraint:   keyConst,
				DefaultValue:    defVal,
				StorageEncoding: "Columnar Slab + Delta Overlay",
			})
		}
	}

	if len(cols) == 0 {
		cols = []ColumnSchema{
			{"id", "BIGINT", "NOT NULL", "PRIMARY KEY (SHARD KEY)", "nextval('seq')", "64-Bit Integer Ring Key"},
			{"tenant_id", "VARCHAR(64)", "NOT NULL", "LOCAL INDEX", "'tenant_core'", "Interned String"},
			{"payload", "JSONB", "NOT NULL", "NONE", "'{}'", "Compressed Binary JSON"},
			{"created_at", "TIMESTAMPTZ", "NOT NULL", "MERGE SORT KEY", "CURRENT_TIMESTAMP", "UTC Epoch Micros"},
		}
	}

	pkCol := cols[0].Name
	ddl := trimmed
	if !strings.Contains(strings.ToUpper(ddl), "SHARD BY") {
		ddl += fmt.Sprintf("\nSHARD BY HASH (%s) INTO 1024 VIRTUAL BUCKETS;", pkCol)
	} else {
		ddl += ";"
	}

	t := &TableSchema{
		SchemaName:       schemaName,
		TableName:        tableName,
		TableType:        "SHARDED TABLE",
		ShardKey:         shardKey,
		ShardingStrategy: fmt.Sprintf("HASH (xxHash64(%s) & 1023)", pkCol),
		VirtualBuckets:   1024,
		StorageEngine:    "Columnar Slab + Delta Overlay",
		Description:      "User-defined distributed sharded table across 1,024 Virtual Buckets",
		Columns:          cols,
		Indexes: []IndexSchema{
			{tableName, "pk_" + tableName + "_" + pkCol, "HASH_RING_PK", pkCol, "O(1) SINGLE SHARD", "UNIQUE", "Direct L1-Cache [1024]atomic.Uint32 routing"},
		},
		CreateDDL: ddl,
	}

	sc.mu.Lock()
	sc.registerTable(t)
	sc.mu.Unlock()
	return t, nil
}

// DropCustomTable drops a user-created table (built-in core tables are protected).
func (sc *SchemaCatalog) DropCustomTable(name string) (string, error) {
	clean := strings.ToLower(strings.TrimSpace(name))
	clean = strings.Trim(clean, ";'\"` ")
	if dot := strings.LastIndexByte(clean, '.'); dot != -1 {
		clean = clean[dot+1:]
	}
	if clean == "users" || strings.HasPrefix(clean, "_shardmaster_") {
		return clean, fmt.Errorf("cannot drop protected core distributed table '%s'", clean)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.tables[clean]; !ok {
		return clean, fmt.Errorf("table '%s' does not exist", clean)
	}
	delete(sc.tables, clean)
	newOrder := make([]string, 0, len(sc.orderedKeys))
	for _, k := range sc.orderedKeys {
		if k != clean {
			newOrder = append(newOrder, k)
		}
	}
	sc.orderedKeys = newOrder
	return clean, nil
}

// FormatRowCountForTable returns the live row count string for a catalog table.
func FormatRowCountForTable(tableName string, usersRows int64, cdcEntries int, activeShards int, vdiffCount int) string {
	switch strings.ToLower(tableName) {
	case "users":
		return strconv.FormatInt(usersRows, 10)
	case "orders":
		return "21"
	case "payments":
		return "21"
	case "vip_users_view":
		return "132"
	case "_shardmaster_cdc":
		return strconv.Itoa(cdcEntries)
	case "_shardmaster_buckets":
		return "1024"
	case "_shardmaster_shards":
		return strconv.Itoa(activeShards)
	case "_shardmaster_vdiff":
		return strconv.Itoa(vdiffCount)
	default:
		return "0"
	}
}

func splitTopLevelCommas(s string) []string {
	var out []string
	var cur strings.Builder
	parens := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		} else if !inSingle && !inDouble {
			if ch == '(' {
				parens++
			} else if ch == ')' && parens > 0 {
				parens--
			} else if ch == ',' && parens == 0 {
				out = append(out, cur.String())
				cur.Reset()
				continue
			}
		}
		cur.WriteByte(ch)
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

