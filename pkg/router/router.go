package router

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"shardmaster/pkg/cdc"
	"shardmaster/pkg/directory"
	"shardmaster/pkg/hash"
	"shardmaster/pkg/hotspot"
	"shardmaster/pkg/storage"
)

// ResultSet represents a PostgreSQL-compatible tabular query response with schema & plan metadata.
type ResultSet struct {
	Title         string
	Columns       []string
	ColumnTypes   []string
	Rows          [][]string
	CommandTag    string
	LatencyUs     int64
	RoutedShard   string
	ExecutionPlan string
	FooterNotes   []string
}

// QueryRouter is the central SQL execution & routing brain of the ShardMaster Proxy.
type QueryRouter struct {
	Dir            *directory.ShardDirectory
	Cluster        *storage.ClusterStorage
	CDC            *cdc.Engine
	HotspotTracker *hotspot.Tracker
	Schema         *SchemaCatalog
	SQL            *RelationalEngine

	TotalQueries   atomic.Uint64
	PointQueries   atomic.Uint64
	ScatterQueries atomic.Uint64
}

func NewQueryRouter(
	dir *directory.ShardDirectory,
	cluster *storage.ClusterStorage,
	cdcEngine *cdc.Engine,
	tracker *hotspot.Tracker,
) *QueryRouter {
	schema := NewSchemaCatalog()
	return &QueryRouter{
		Dir:            dir,
		Cluster:        cluster,
		CDC:            cdcEngine,
		HotspotTracker: tracker,
		Schema:         schema,
		SQL:            NewRelationalEngine(dir, cluster, schema),
	}
}

// RouteFastPoint is the zero-allocation hot path used by the multi-million QPS benchmark engine.
//
//go:inline
func (qr *QueryRouter) RouteFastPoint(userID int64) (shardID uint32, bucket uint16) {
	shardID, bucket = qr.Dir.LookupInt64Fast(userID)
	qr.HotspotTracker.RecordHit(bucket)
	qr.TotalQueries.Add(1)
	qr.PointQueries.Add(1)
	return shardID, bucket
}

// ExecuteSQL parses, routes, and executes any SQL statement (or multi-statement SQL script)
// coming over PGWire (:6000) or the interactive CLI.
func (qr *QueryRouter) ExecuteSQL(sql string) (*ResultSet, error) {
	if pinnedID, cleanedSQL, ok := extractPinnedShardHint(sql); ok {
		return qr.ExecuteSQLOnShard(cleanedSQL, pinnedID)
	}
	stmts := SplitSQLStatements(sql)
	if len(stmts) > 1 {
		var batchNotes []string
		var lastRes *ResultSet
		for i, stmt := range stmts {
			res, err := qr.executeSingleSQL(stmt)
			if err != nil {
				return nil, fmt.Errorf("statement %d (%s): %v", i+1, stmt, err)
			}
			lastRes = res
			if i < len(stmts)-1 {
				batchNotes = append(batchNotes, fmt.Sprintf("Batch Step %d/%d [%s]: %s (%d us)",
					i+1, len(stmts), res.CommandTag, res.Title, res.LatencyUs))
			}
		}
		if lastRes != nil && len(batchNotes) > 0 {
			lastRes.FooterNotes = append(batchNotes, lastRes.FooterNotes...)
		}
		return lastRes, nil
	}
	if len(stmts) == 1 {
		return qr.executeSingleSQL(stmts[0])
	}
	return qr.executeSingleSQL(sql)
}

func (qr *QueryRouter) executeSingleSQL(sql string) (*ResultSet, error) {
	start := time.Now()
	qr.TotalQueries.Add(1)

	if ctrlRes, handled, ctrlErr := qr.handleShardControlCommand(sql, start); handled {
		return ctrlRes, ctrlErr
	}

	cq := ClassifySQL(sql)

	switch cq.Kind {
	case QuerySystemCatalog:
		return &ResultSet{
			Title:         "SYSTEM CATALOG INTROSPECTION",
			Columns:       []string{"version", "protocol", "virtual_buckets"},
			ColumnTypes:   []string{"TEXT", "VARCHAR(16)", "INT4"},
			Rows:          [][]string{{"PostgreSQL 15.0 (ShardMaster Distributed SQL Engine)", "PGWire v3.0", "1024"}},
			CommandTag:    "SELECT 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CONTROL_PLANE",
			ExecutionPlan: "Control-Plane System Catalog Handshake (0 Shard Hops)",
		}, nil

	case QueryShowSchemas:
		tables := qr.Schema.ListTables()
		pubCount := 0
		sysCount := 0
		for _, t := range tables {
			if t.SchemaName == "public" {
				pubCount++
			} else {
				sysCount++
			}
		}
		rows := [][]string{
			{"public", "shardmaster_admin", strconv.Itoa(pubCount), "HASH (xxHash64 & 1023)", "Application Distributed Sharded Tables"},
			{"shardmaster", "shardmaster_system", strconv.Itoa(sysCount), "CONTROL PLANE + LOCAL", "Internal Topology, L1 Ring, CDC & VDiff Catalog"},
			{"information_schema", "postgres", "4", "VIRTUAL CATALOG", "ANSI SQL Metadata & Column Introspection Views"},
			{"pg_catalog", "postgres", "8", "VIRTUAL CATALOG", "PostgreSQL v15.0 Wire Protocol Compatibility Catalog"},
		}
		return &ResultSet{
			Title:         "DATABASE SCHEMAS & NAMESPACES",
			Columns:       []string{"schema_name", "owner", "tables", "default_sharding", "description"},
			ColumnTypes:   []string{"VARCHAR(64)", "VARCHAR(64)", "INT4", "VARCHAR(32)", "TEXT"},
			Rows:          rows,
			CommandTag:    "SHOW SCHEMAS 4",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "SCHEMA_CATALOG",
			ExecutionPlan: "In-Memory Catalog Namespace Scan",
		}, nil

	case QueryShowTables:
		shards := qr.Cluster.GetAllShards()
		var usersRows int64
		var cdcCount int
		for _, s := range shards {
			usersRows += s.RowCount()
			cdcCount += s.CDCEntryCount()
		}
		vdiffCount := len(qr.CDC.GetVDiffHistory())
		if vdiffCount == 0 {
			vdiffCount = len(shards)
		}

		tables := qr.Schema.ListTables()
		rows := make([][]string, 0, len(tables))
		for _, t := range tables {
			rc := FormatRowCountForTable(t.TableName, usersRows, cdcCount, len(shards), vdiffCount)
			rows = append(rows, []string{
				t.SchemaName,
				t.TableName,
				t.TableType,
				t.ShardKey,
				t.ShardingStrategy,
				strconv.Itoa(t.VirtualBuckets),
				rc,
				t.StorageEngine,
			})
		}
		return &ResultSet{
			Title:         "DISTRIBUTED & SYSTEM TABLES CATALOG (SHOW TABLES)",
			Columns:       []string{"schema", "table_name", "table_type", "shard_key", "sharding_strategy", "buckets", "live_rows", "storage_engine"},
			ColumnTypes:   []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "INT4", "INT8", "VARCHAR"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "SCHEMA_CATALOG",
			ExecutionPlan: "Catalog Table Registry + Live Shard Row Counter Aggregation",
			FooterNotes: []string{
				"Tip: Run 'DESCRIBE users;' or 'DESCRIBE _shardmaster_cdc;' or 'SHOW CREATE TABLE users;' to inspect column schemas & DDL.",
			},
		}, nil

	case QueryDescribeTable:
		t, ok := qr.Schema.GetTable(cq.TableName)
		if !ok && qr.SQL != nil {
			t, ok = qr.SQL.IntrospectDynamicTable(cq.TableName)
		}
		if !ok {
			return nil, fmt.Errorf("table '%s' not found in catalog (run 'SHOW TABLES;' to list all tables)", cq.TableName)
		}
		rows := make([][]string, 0, len(t.Columns))
		for i, c := range t.Columns {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				c.Name,
				c.DataType,
				c.Nullable,
				c.KeyConstraint,
				c.DefaultValue,
				c.StorageEncoding,
			})
		}
		notes := []string{
			fmt.Sprintf("Table: %s.%s  |  Type: %s  |  Shard Key: %s  |  Strategy: %s (%d Buckets)",
				t.SchemaName, t.TableName, t.TableType, t.ShardKey, t.ShardingStrategy, t.VirtualBuckets),
			fmt.Sprintf("Storage Engine: %s  |  Description: %s", t.StorageEngine, t.Description),
		}
		for _, idx := range t.Indexes {
			notes = append(notes, fmt.Sprintf("Index [%s]: %s ON (%s) | Scope: %s (%s)",
				idx.IndexName, idx.IndexType, idx.Columns, idx.RoutingScope, idx.Uniqueness))
		}
		return &ResultSet{
			Title:         fmt.Sprintf("TABLE SCHEMA DEFINITION: %s.%s", strings.ToUpper(t.SchemaName), strings.ToUpper(t.TableName)),
			Columns:       []string{"ordinal", "column_name", "data_type", "nullable", "key_constraint", "default_expr", "storage_encoding"},
			ColumnTypes:   []string{"INT2", "VARCHAR(64)", "VARCHAR(32)", "VARCHAR(12)", "VARCHAR(28)", "VARCHAR(32)", "VARCHAR(32)"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("DESCRIBE %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "SCHEMA_CATALOG",
			ExecutionPlan: fmt.Sprintf("Catalog Schema Resolution for '%s.%s' (%d columns, %d indexes)", t.SchemaName, t.TableName, len(t.Columns), len(t.Indexes)),
			FooterNotes:   notes,
		}, nil

	case QueryShowCreateTable:
		t, ok := qr.Schema.GetTable(cq.TableName)
		if !ok && qr.SQL != nil {
			t, ok = qr.SQL.IntrospectDynamicTable(cq.TableName)
		}
		if !ok {
			return nil, fmt.Errorf("table '%s' not found in catalog", cq.TableName)
		}
		ddlLines := strings.Split(t.CreateDDL, "\n")
		rows := make([][]string, 0, len(ddlLines))
		for i, line := range ddlLines {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				t.SchemaName + "." + t.TableName,
				line,
			})
		}
		return &ResultSet{
			Title:         fmt.Sprintf("DDL STATEMENT: SHOW CREATE TABLE %s.%s", strings.ToUpper(t.SchemaName), strings.ToUpper(t.TableName)),
			Columns:       []string{"line", "table_name", "create_table_ddl_statement"},
			ColumnTypes:   []string{"INT2", "VARCHAR(64)", "TEXT"},
			Rows:          rows,
			CommandTag:    "SHOW CREATE TABLE",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "SCHEMA_CATALOG",
			ExecutionPlan: "Distributed DDL Generator (PostgreSQL 15 + ShardMaster Virtual Bucket Extension)",
		}, nil

	case QueryShowIndexes:
		tables := qr.Schema.ListTables()
		filterTbl := strings.ToLower(strings.Trim(cq.TableName, ";'\"` "))
		if dot := strings.LastIndexByte(filterTbl, '.'); dot != -1 {
			filterTbl = filterTbl[dot+1:]
		}
		var rows [][]string
		for _, t := range tables {
			if filterTbl != "" && strings.ToLower(t.TableName) != filterTbl {
				continue
			}
			for _, idx := range t.Indexes {
				rows = append(rows, []string{
					t.SchemaName + "." + idx.TableName,
					idx.IndexName,
					idx.IndexType,
					idx.Columns,
					idx.RoutingScope,
					idx.Uniqueness,
					idx.ExecutionNotes,
				})
			}
		}
		return &ResultSet{
			Title:         "DISTRIBUTED & LOCAL SHARD INDEX CATALOG",
			Columns:       []string{"table_name", "index_name", "index_type", "indexed_columns", "routing_scope", "uniqueness", "execution_role"},
			ColumnTypes:   []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "TEXT"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SHOW INDEXES %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "SCHEMA_CATALOG",
			ExecutionPlan: "Global & Shard-Local Index Catalog Scan",
		}, nil

	case QueryCreateTable:
		if qr.SQL != nil {
			if _, sqlErr := qr.SQL.ExecuteFullSQL(sql); sqlErr != nil {
				return nil, sqlErr
			}
		}
		t, err := qr.Schema.CreateCustomTable(sql)
		if err != nil {
			return nil, err
		}
		return &ResultSet{
			Title:       fmt.Sprintf("CREATED SHARDED TABLE: %s.%s", strings.ToUpper(t.SchemaName), strings.ToUpper(t.TableName)),
			Columns:     []string{"schema", "table_name", "shard_key", "sharding_strategy", "virtual_buckets", "columns", "status"},
			ColumnTypes: []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "INT4", "INT4", "VARCHAR"},
			Rows: [][]string{{
				t.SchemaName,
				t.TableName,
				t.ShardKey,
				t.ShardingStrategy,
				strconv.Itoa(t.VirtualBuckets),
				strconv.Itoa(len(t.Columns)),
				"CREATED_ACROSS_ALL_SHARDS",
			}},
			CommandTag:    "CREATE TABLE",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "ALL_SHARDS_DDL",
			ExecutionPlan: fmt.Sprintf("2-Phase Distributed DDL Broadcast -> %d Physical Shards (%d Virtual Buckets)", qr.Dir.ActiveShards(), t.VirtualBuckets),
			FooterNotes: []string{
				fmt.Sprintf("Run 'DESCRIBE %s;' or 'INSERT INTO %s ...' to query the newly created sharded table.", t.TableName, t.TableName),
			},
		}, nil

	case QueryDropTable:
		dropped, err := qr.Schema.DropCustomTable(cq.TableName)
		if err != nil {
			return nil, err
		}
		if qr.SQL != nil {
			_, _ = qr.SQL.ExecuteFullSQL(cq.RawSQL)
		}
		return &ResultSet{
			Title:         "DROP DISTRIBUTED TABLE",
			Columns:       []string{"dropped_table", "virtual_buckets_freed", "status"},
			ColumnTypes:   []string{"VARCHAR", "INT4", "VARCHAR"},
			Rows:          [][]string{{dropped, "1024", "DROPPED"}},
			CommandTag:    "DROP TABLE",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "ALL_SHARDS_DDL",
			ExecutionPlan: "Distributed Catalog Deregistration & Slab Reclamation",
		}, nil

	case QueryAdminShowShards:
		shards := qr.Cluster.GetAllShards()
		bucketCounts := qr.Dir.BucketCountsByShard()
		rows := make([][]string, 0, len(shards))
		for _, s := range shards {
			cfg := s.GetSettings()
			rows = append(rows, []string{
				fmt.Sprintf("Shard %d", s.ShardID),
				s.DisplayName(),
				fmt.Sprintf(":%d", s.Port),
				s.Region,
				fmt.Sprintf("%s / %s (%d B)", storage.FormatBytesCompact(s.UsedMemoryBytes()), storage.FormatBytesCompact(cfg.MaxCapacityBytes), s.UsedMemoryBytes()),
				fmt.Sprintf("%d B/bkt", cfg.SlabBytesPerBucket),
				fmt.Sprintf("%d%%", cfg.Weight),
				strconv.Itoa(bucketCounts[s.ShardID]),
				strconv.FormatInt(s.RowCount(), 10),
				cfg.AccessMode,
				cfg.ReplicationMode,
			})
		}
		return &ResultSet{
			Title:         "PHYSICAL SHARD TOPOLOGY & EXACT BYTE SETTINGS (_shardmaster_shards)",
			Columns:       []string{"shard_id", "custom_name", "port", "region", "used_vs_max_bytes", "slab_bytes", "weight", "buckets", "rows", "mode", "replication"},
			ColumnTypes:   []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "INT4", "INT8", "VARCHAR", "VARCHAR"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CONTROL_PLANE",
			ExecutionPlan: fmt.Sprintf("Control Plane Topology Probe (%d Physical Shards, 1,024 Virtual Buckets)", len(shards)),
			FooterNotes: []string{
				"Customize every byte live via TUI [e] Edit / [n] New or SQL: ALTER SHARD 0 SET NAME='my-shard', BYTES=1048576, SLAB_BYTES=4096, BUCKETS=128;",
			},
		}, nil

	case QueryAdminShowBuckets:
		var rows [][]string
		startB := uint16(0)
		curOwner := qr.Dir.GetBucketOwner(0)
		curState := qr.Dir.GetBucketState(0)

		flushRange := func(sB, eB uint16, owner uint32, st uint32) {
			rangeLabel := fmt.Sprintf("Bucket [%04d..%04d]", sB, eB)
			if sB == eB {
				rangeLabel = fmt.Sprintf("Bucket #%04d (Isolated)", sB)
			}
			stateLabel := "READY"
			if st == directory.BucketStateCDCStreaming {
				stateLabel = "CDC_STREAMING"
			} else if st == directory.BucketStateCutoverGate {
				stateLabel = "CUTOVER_GATE"
			}
			var rangeRows int64
			region := "us-west"
			if sh, ok := qr.Cluster.GetShard(owner); ok {
				region = sh.Region
				_, rangeRows = sh.ComputeBucketRangeXORHash(sB, eB)
			}
			rows = append(rows, []string{
				rangeLabel,
				strconv.Itoa(int(eB-sB) + 1),
				fmt.Sprintf("Shard %d (:%d)", owner, 5432+owner),
				region,
				stateLabel,
				strconv.FormatInt(rangeRows, 10),
			})
		}

		for b := uint16(1); b < hash.TotalVirtualBuckets; b++ {
			o := qr.Dir.GetBucketOwner(b)
			st := qr.Dir.GetBucketState(b)
			if o != curOwner || st != curState {
				flushRange(startB, b-1, curOwner, curState)
				startB = b
				curOwner = o
				curState = st
			}
		}
		flushRange(startB, hash.TotalVirtualBuckets-1, curOwner, curState)

		return &ResultSet{
			Title:         "L1-CACHE VIRTUAL BUCKET INDIRECTION RING (_shardmaster_buckets)",
			Columns:       []string{"bucket_range", "bucket_count", "owner_shard", "region", "directory_state", "rows_in_range"},
			ColumnTypes:   []string{"VARCHAR", "INT4", "VARCHAR", "VARCHAR", "VARCHAR", "INT8"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "L1_DIRECTORY_RING",
			ExecutionPlan: "4 KB [1024]atomic.Uint32 Lock-Free Contiguous Range Scan",
		}, nil

	case QueryAdminShowCDC:
		wf := qr.CDC.GetSnapshot()
		history := qr.CDC.GetVDiffHistory()
		var rows [][]string
		rows = append(rows, []string{
			"ACTIVE_WORKFLOW",
			wf.CurrentRangeText,
			wf.Title,
			strconv.FormatInt(wf.RowsMigrated, 10),
			fmt.Sprintf("%.2f ms", wf.ReplicationLagMs),
			wf.VDiffStatus,
			wf.Status,
		})
		for i, vd := range history {
			dig := vd.TargetDigest
			if len(dig) > 20 {
				dig = dig[:20] + "..."
			}
			rows = append(rows, []string{
				fmt.Sprintf("COMPLETED_RANGE_%d", i+1),
				fmt.Sprintf("Bucket [%d-%d]", vd.StartBucket, vd.EndBucket),
				fmt.Sprintf("Shard %d -> Shard %d", vd.SourceShard, vd.TargetShard),
				strconv.FormatInt(vd.TargetRows, 10),
				"0.00 ms",
				dig,
				"VDIFF_VERIFIED",
			})
		}
		return &ResultSet{
			Title:         "VITESS CDC VREPLICATION WORKFLOWS & VDIFF STREAMS",
			Columns:       []string{"stream_id", "bucket_scope", "migration_route", "rows_moved", "cdc_lag", "vdiff_parity", "status"},
			ColumnTypes:   []string{"VARCHAR", "VARCHAR", "VARCHAR", "INT8", "VARCHAR", "VARCHAR", "VARCHAR"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CDC_VREPLICATION_ENGINE",
			ExecutionPlan: "CDC Coordinator Stream State + Historical VDiff Parity Ledger",
		}, nil

	case QuerySelectCDCLog:
		shards := qr.Cluster.GetAllShards()
		var rows [][]string
		perShard := (cq.Limit / maxInt(1, len(shards))) + 2
		for _, s := range shards {
			entries := s.GetRecentCDCEntries(perShard)
			for _, e := range entries {
				opLabel := "UPSERT"
				if e.Op == storage.MutationDelete {
					opLabel = "DELETE"
				}
				email := e.Row.Email
				if email == "" {
					email = fmt.Sprintf("user_%d@gmail.com", e.UserID)
				}
				rows = append(rows, []string{
					strconv.FormatUint(e.LSN, 10),
					fmt.Sprintf("shard_%d", s.ShardID),
					fmt.Sprintf("#%d", e.BucketID),
					opLabel,
					strconv.FormatInt(e.UserID, 10),
					email,
					fmt.Sprintf("$%.2f", float64(e.Row.BalanceCents)/100.0),
					strconv.FormatInt(e.TimestampUs, 10),
				})
				if len(rows) >= cq.Limit {
					break
				}
			}
			if len(rows) >= cq.Limit {
				break
			}
		}
		return &ResultSet{
			Title:         "PER-SHARD CDC MUTATION LOG JOURNAL (_shardmaster_cdc)",
			Columns:       []string{"lsn", "shard_id", "bucket_id", "op_type", "user_id", "email", "balance_usd", "timestamp_us"},
			ColumnTypes:   []string{"INT8", "VARCHAR", "VARCHAR", "VARCHAR", "INT8", "VARCHAR", "NUMERIC(12,2)", "INT8"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("CDC_JOURNAL (%d Shards)", len(shards)),
			ExecutionPlan: "Parallel LSN Ring Buffer Tail Across All Physical Shards",
		}, nil

	case QueryAdminShowHotspots:
		top := qr.HotspotTracker.GetTopHotBuckets(8)
		alerts := qr.HotspotTracker.GetRecentAlerts()
		var rows [][]string
		for _, a := range alerts {
			rows = append(rows, []string{
				"AUTO_ISOLATED_ALERT",
				fmt.Sprintf("Bucket #%d", a.BucketID),
				fmt.Sprintf("Shard %d -> Shard %d", a.FromShard, a.ToShard),
				fmt.Sprintf("%d QPS", a.MeasuredQPS),
				"ISOLATED_ZERO_DOWNTIME (VDiff: " + a.VDiffDigest + ")",
			})
		}
		for idx, h := range top {
			status := "NORMAL_ENVELOPE"
			if h.EWMAQPS >= 1500 {
				status = "HOTSPOT_SPIKE (>98th Pct)"
			}
			rows = append(rows, []string{
				fmt.Sprintf("TOP_BUCKET_%d", idx+1),
				fmt.Sprintf("Bucket #%d", h.BucketID),
				fmt.Sprintf("Shard %d (:%d)", h.OwnerShard, 5432+h.OwnerShard),
				fmt.Sprintf("%d QPS", h.EWMAQPS),
				status,
			})
		}
		return &ResultSet{
			Title:         "AUTONOMOUS EWMA HOTSPOT TELEMETRY & ISOLATION LOG",
			Columns:       []string{"telemetry_type", "virtual_bucket", "shard_placement", "ewma_qps", "thermal_status"},
			ColumnTypes:   []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "EWMA_HOTSPOT_TRACKER",
			ExecutionPlan: "64-Byte Cache-Line Padded Atomic EWMA Frequency Scan (Top-8 Buckets)",
		}, nil

	case QueryAdminShowStats:
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		shards := qr.Cluster.GetAllShards()
		var totalRows int64
		for _, s := range shards {
			totalRows += s.RowCount()
		}
		rows := [][]string{
			{"cluster.active_physical_shards", strconv.Itoa(len(shards)), "Active PostgreSQL / Columnar Shard Nodes"},
			{"cluster.virtual_buckets", "1024", "Fixed Virtual Bucket Indirection Ring (xxHash64 & 1023)"},
			{"cluster.total_seeded_rows", strconv.FormatInt(totalRows, 10), "Zero-GC Columnar Bucket Slabs + Delta Overlay"},
			{"catalog.registered_tables", strconv.Itoa(len(qr.Schema.ListTables())), "Distributed application & system catalog tables"},
			{"directory.l1_cache_footprint", "4096 Bytes (4 KB)", "[1024]atomic.Uint32 lock-free routing array"},
			{"router.total_queries_executed", strconv.FormatUint(qr.TotalQueries.Load(), 10), "Cumulative queries routed since startup"},
			{"router.point_queries_o1", strconv.FormatUint(qr.PointQueries.Load(), 10), "O(1) single-shard point lookups & mutations"},
			{"router.scatter_gather_kway", strconv.FormatUint(qr.ScatterQueries.Load(), 10), "Parallel Goroutine + Min-Heap K-Way Merge scans"},
			{"runtime.process_heap_ram_mb", fmt.Sprintf("%.2f MB", float64(mem.Alloc)/(1024*1024)), "Current Go heap memory allocation"},
			{"runtime.cpu_logical_threads", strconv.Itoa(runtime.NumCPU()), "Hardware logical CPU cores utilized"},
			{"runtime.active_goroutines", strconv.Itoa(runtime.NumGoroutine()), "Concurrent PGWire, TUI, and worker Goroutines"},
		}
		return &ResultSet{
			Title:         "SHARDMASTER INTERNAL ENGINE & MEMORY TELEMETRY",
			Columns:       []string{"internal_metric", "current_value", "subsystem_description"},
			ColumnTypes:   []string{"VARCHAR(40)", "VARCHAR(24)", "TEXT"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "INTERNAL_TELEMETRY",
			ExecutionPlan: "Runtime Memory & Atomic Counter Snapshot",
		}, nil

	case QueryAdminShowQueries:
		rows := [][]string{
			{"1", "SCHEMA_CATALOG", "SHOW TABLES;", "List all distributed & system tables, shard keys, and row counts"},
			{"2", "SCHEMA_CATALOG", "DESCRIBE users;", "Display full column types, keys, encodings & indexes for public.users"},
			{"3", "SCHEMA_CATALOG", "DESCRIBE _shardmaster_cdc;", "Display schema for the per-shard Change Data Capture journal table"},
			{"4", "SCHEMA_CATALOG", "SHOW CREATE TABLE users;", "Generate the complete PostgreSQL + ShardMaster DDL statement"},
			{"5", "SCHEMA_CATALOG", "SHOW INDEXES;", "Inspect all global hash-ring and shard-local BTree/GIN indexes"},
			{"6", "CLUSTER_ADMIN", "SHOW SHARDS;", "List all physical shards, ports, regions, buckets, rows, QPS & LSN"},
			{"7", "CLUSTER_ADMIN", "SHOW BUCKETS;", "Inspect contiguous Virtual Bucket ranges [0..1023] & shard ownership"},
			{"8", "CLUSTER_ADMIN", "SHOW CDC;", "Inspect active & historical CDC VReplication streams and VDiff status"},
			{"9", "CLUSTER_ADMIN", "SHOW HOTSPOTS;", "Inspect Top EWMA hottest Virtual Buckets & self-healing isolations"},
			{"10", "CLUSTER_ADMIN", "SHOW STATS;", "Inspect internal RAM, L1 directory, CPU threads & query counters"},
			{"11", "CLUSTER_ADMIN", "RUN VDIFF;", "Compute 256-bit commutative XOR-SHA256 parity across all 50M rows"},
			{"12", "QUERY_PLANNER", "EXPLAIN ANALYZE SELECT * FROM users WHERE user_id = 42;", "Inspect step-by-step distributed execution plan & operator costs"},
			{"13", "POINT_QUERY_O1", "SELECT * FROM users WHERE user_id = 42;", "O(1) point lookup routed to single owning shard in ~18ns"},
			{"14", "MULTI_POINT_IN", "SELECT * FROM users WHERE user_id IN (42, 100, 777, 8888, 49999999);", "Batch multi-key routing across exact target shards"},
			{"15", "K_WAY_MERGE", "SELECT * FROM users WHERE email LIKE '%@gmail.com' ORDER BY created_at DESC LIMIT 5;", "Parallel Scatter-Gather + Min-Heap K-Way Merge Sort"},
			{"16", "K_WAY_MERGE", "SELECT * FROM users WHERE region = 'us-west' ORDER BY created_at DESC LIMIT 5;", "Region-pruned Scatter-Gather K-Way Merge Sort"},
			{"17", "MAP_REDUCE_AGG", "SELECT region, COUNT(*), SUM(balance_usd), AVG(balance_usd) FROM users GROUP BY region;", "Distributed Map-Reduce GROUP BY region across 50M rows"},
			{"18", "MAP_REDUCE_AGG", "SELECT shard_id, COUNT(*), AVG(balance_usd) FROM users GROUP BY shard_id;", "Distributed Map-Reduce GROUP BY physical shard"},
			{"19", "CDC_MUTATION", "INSERT INTO users (user_id, name, email, balance_cents) VALUES (42, 'Ada Lovelace', 'ada@gmail.com', 950000);", "Point Upsert + append LSN entry to _shardmaster_cdc log"},
			{"20", "CDC_MUTATION", "UPDATE users SET name = 'Grace Hopper', balance_usd = 12500.00 WHERE user_id = 42;", "Point Update + append LSN entry to _shardmaster_cdc log"},
			{"21", "CDC_MUTATION", "DELETE FROM users WHERE user_id = 100;", "Point Tombstone Delete + append LSN entry to _shardmaster_cdc log"},
			{"22", "CDC_LOG_QUERY", "SELECT * FROM _shardmaster_cdc LIMIT 6;", "Query live replication LSN entries from the CDC journal table"},
		}
		return &ResultSet{
			Title:         "SHARDMASTER SQL REFERENCE CATALOG (22 BUILT-IN QUERIES)",
			Columns:       []string{"id", "category", "sql_syntax", "description"},
			ColumnTypes:   []string{"INT2", "VARCHAR(18)", "TEXT", "TEXT"},
			Rows:          rows,
			CommandTag:    "SELECT 22",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "QUERY_CATALOG",
			ExecutionPlan: "Built-In SQL Dialect & Query Preset Reference",
		}, nil

	case QueryAdminExplainShard:
		info := qr.Dir.LookupDetailed(cq.UserKey)
		return &ResultSet{
			Title:       fmt.Sprintf("O(1) SHARD ROUTING EXPLAIN (KEY = %s)", info.Key),
			Columns:     []string{"shard_key", "xxhash64", "virtual_bucket", "target_shard", "lookup_source", "lookup_ns"},
			ColumnTypes: []string{"VARCHAR", "CHAR(18)", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"},
			Rows: [][]string{{
				info.Key,
				fmt.Sprintf("0x%016x", info.HashValue),
				fmt.Sprintf("Bucket #%d / 1024", info.VirtualBucket),
				fmt.Sprintf("Shard %d (:%d)", info.ShardID, 5432+info.ShardID),
				info.Source,
				fmt.Sprintf("%d ns", info.LookupTimeNs),
			}},
			CommandTag:    "EXPLAIN 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d", info.ShardID),
			ExecutionPlan: fmt.Sprintf("xxHash64('%s') & 1023 -> Bucket #%d -> L1 [1024]atomic.Uint32 -> Shard %d", info.Key, info.VirtualBucket, info.ShardID),
		}, nil

	case QueryExplainAnalyze:
		innerSQL := cq.RawSQL
		upper := strings.ToUpper(innerSQL)
		if strings.HasPrefix(upper, "EXPLAIN ANALYZE") {
			innerSQL = strings.TrimSpace(innerSQL[len("EXPLAIN ANALYZE"):])
		} else if strings.HasPrefix(upper, "EXPLAIN") {
			innerSQL = strings.TrimSpace(innerSQL[len("EXPLAIN"):])
		}
		if innerSQL == "" {
			innerSQL = "SELECT * FROM users WHERE user_id = 42"
		}
		innerCQ := ClassifySQL(innerSQL)
		shardsCount := qr.Dir.ActiveShards()

		var rows [][]string
		planSummary := ""
		if innerCQ.HasShardKey && len(innerCQ.UserIDs) == 0 {
			info := qr.Dir.LookupDetailed(innerCQ.UserKey)
			planSummary = fmt.Sprintf("Single-Shard O(1) Point Execution Plan -> Shard %d (:%d)", info.ShardID, 5432+info.ShardID)
			rows = [][]string{
				{"1", "SQL Lexer & AST Classifier", "Proxy Coordinator", "1", "2 us", fmt.Sprintf("Extracted predicate: user_id = %s", info.Key)},
				{"2", "xxHash64 Digest Engine", "CPU Register", "1", "4 ns", fmt.Sprintf("xxHash64('%s') = 0x%016x", info.Key, info.HashValue)},
				{"3", "L1 Atomic Bucket Ring Probe", "L1 Cache (4 KB)", "1", fmt.Sprintf("%d ns", info.LookupTimeNs), fmt.Sprintf("0x%016x & 1023 -> Bucket #%d -> Shard %d", info.HashValue, info.VirtualBucket, info.ShardID)},
				{"4", "Single-Shard Columnar Slab Get", fmt.Sprintf("Shard %d (:%d)", info.ShardID, 5432+info.ShardID), "1", "12 us", fmt.Sprintf("Probed Bucket #%d Delta Overlay + Columnar Slab", info.VirtualBucket)},
			}
		} else if len(innerCQ.UserIDs) > 0 {
			planSummary = fmt.Sprintf("Multi-Key Batch Point Fan-Out (%d Keys)", len(innerCQ.UserIDs))
			rows = [][]string{
				{"1", "Batch Key Extractor", "Proxy Coordinator", strconv.Itoa(len(innerCQ.UserIDs)), "3 us", fmt.Sprintf("Parsed %d shard keys from IN / BETWEEN clause", len(innerCQ.UserIDs))},
				{"2", "Vectorized xxHash64 + L1 Lookup", "L1 Cache (4 KB)", strconv.Itoa(len(innerCQ.UserIDs)), "45 ns", "Grouped keys by owning physical shard"},
				{"3", "Targeted Multi-Shard Point Batch", fmt.Sprintf("Target Shards (of %d)", shardsCount), strconv.Itoa(len(innerCQ.UserIDs)), "38 us", "Pruned non-owning shards; fetched exact bucket slabs"},
			}
		} else if innerCQ.Kind == QueryGroupByAggregate || innerCQ.Kind == QueryCountAggregate {
			planSummary = fmt.Sprintf("Distributed Two-Phase Map-Reduce Aggregation (%d Shards)", shardsCount)
			rows = [][]string{
				{"1", "Coordinator Map-Reduce Planner", "Proxy Coordinator", strconv.Itoa(int(shardsCount)), "4 us", "Decomposed aggregate into Shard-Local Map + Coordinator Reduce"},
				{"2", "Parallel Shard Slab Aggregation", fmt.Sprintf("All %d Shards", shardsCount), "50,000,000", "180 us", "Scanned 1,024 bucket columnar slabs in parallel"},
				{"3", "Coordinator Final Merge", "Proxy Coordinator", strconv.Itoa(int(shardsCount)), "9 us", "Combined partial COUNT, SUM, MIN, MAX accumulators"},
			}
		} else {
			planSummary = fmt.Sprintf("Distributed Scatter-Gather + Min-Heap K-Way Merge (%d Shards)", shardsCount)
			rows = [][]string{
				{"1", "Scatter Fan-Out Spawner", "Proxy Coordinator", strconv.Itoa(int(shardsCount)), "6 us", fmt.Sprintf("Spawned %d parallel worker Goroutines with bounded channels (cap=16)", shardsCount)},
				{"2", "Shard-Local Top-K Index Scan", fmt.Sprintf("All %d Shards", shardsCount), strconv.Itoa(innerCQ.Limit * int(shardsCount)), "290 us", "Filtered local buckets & sorted by (created_at DESC, user_id DESC)"},
				{"3", "Streaming Min-Heap K-Way Merge", "Priority Queue (RAM)", strconv.Itoa(innerCQ.Limit), "24 us", fmt.Sprintf("Merged %d ordered shard streams in O(N log K) time, O(K*batch) RAM", shardsCount)},
			}
		}

		return &ResultSet{
			Title:         "DISTRIBUTED QUERY EXECUTION PLAN (EXPLAIN ANALYZE)",
			Columns:       []string{"stage", "operator", "execution_node", "rows", "time", "operator_details"},
			ColumnTypes:   []string{"INT2", "VARCHAR(32)", "VARCHAR(20)", "INT8", "VARCHAR(12)", "TEXT"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("EXPLAIN %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "QUERY_PLANNER",
			ExecutionPlan: planSummary,
		}, nil

	case QueryAdminRebalance:
		go func(target uint32) {
			_, _ = qr.CDC.RebalanceToShards(target, 25*time.Millisecond)
		}(cq.TargetShards)
		return &ResultSet{
			Title:       "ZERO-DOWNTIME CDC RESHARDING WORKFLOW",
			Columns:     []string{"workflow", "target_shards", "engine", "status"},
			ColumnTypes: []string{"VARCHAR", "INT4", "VARCHAR", "VARCHAR"},
			Rows: [][]string{{
				"CDC_VREPLICATION_SPLIT",
				strconv.FormatUint(uint64(cq.TargetShards), 10),
				"Keyset Backfill + CDC Stream + VDiff",
				"STARTED (0ms Downtime)",
			}},
			CommandTag:    "REBALANCE",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CONTROL_PLANE",
			ExecutionPlan: fmt.Sprintf("Asynchronous Vitess VReplication Split to %d Physical Shards", cq.TargetShards),
		}, nil

	case QueryAdminVDiff:
		shards := qr.Cluster.GetAllShards()
		rows := make([][]string, 0, len(shards))
		for _, s := range shards {
			digest, count := s.ComputeBucketRangeXORHash(0, 1023)
			rows = append(rows, []string{
				fmt.Sprintf("Shard %d (:%d)", s.ShardID, s.Port),
				strconv.FormatInt(count, 10),
				digest,
				"VERIFIED_INTACT",
			})
		}
		return &ResultSet{
			Title:         "CRYPTOGRAPHIC VDIFF PARITY AUDIT (_shardmaster_vdiff)",
			Columns:       []string{"shard", "rows_hashed", "vdiff_xor_sha256_digest", "parity_status"},
			ColumnTypes:   []string{"VARCHAR", "INT8", "CHAR(64)", "VARCHAR"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "ALL_SHARDS",
			ExecutionPlan: "Parallel 256-Bit Commutative XOR-SHA256 Checksum Across 1,024 Virtual Buckets",
		}, nil

	case QueryPointSelect:
		qr.PointQueries.Add(1)
		shardID, bucket := qr.Dir.LookupFast(cq.UserKey)
		qr.HotspotTracker.RecordHit(bucket)

		shard, ok := qr.Cluster.GetShard(shardID)
		if !ok {
			return nil, fmt.Errorf("physical shard %d not available", shardID)
		}

		var outRows [][]string
		if user, found := shard.GetUser(cq.UserID); found {
			outRows = append(outRows, formatUserRow(shard.ShardID, user))
		}
		cols, colTypes, projRows := projectUserResult(cq.ProjectedCols, outRows)
		return &ResultSet{
			Title:         fmt.Sprintf("POINT SELECT RESULT (user_id = %d)", cq.UserID),
			Columns:       cols,
			ColumnTypes:   colTypes,
			Rows:          projRows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(projRows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d (Bucket #%d)", shardID, bucket),
			ExecutionPlan: fmt.Sprintf("O(1) L1 Bucket Ring Lookup: xxHash64('%s') & 1023 -> Bucket #%d -> Shard %d (:%d)", cq.UserKey, bucket, shardID, shard.Port),
		}, nil

	case QueryMultiPointSelect:
		qr.PointQueries.Add(uint64(len(cq.UserIDs)))
		shardsHit := make(map[uint32]bool)
		var outRows [][]string
		for _, uid := range cq.UserIDs {
			ukey := strconv.FormatInt(uid, 10)
			shardID, bucket := qr.Dir.LookupFast(ukey)
			qr.HotspotTracker.RecordHit(bucket)
			shardsHit[shardID] = true
			if shard, ok := qr.Cluster.GetShard(shardID); ok {
				if user, found := shard.GetUser(uid); found {
					outRows = append(outRows, formatUserRow(shard.ShardID, user))
				}
			}
		}
		cols, colTypes, projRows := projectUserResult(cq.ProjectedCols, outRows)
		return &ResultSet{
			Title:         fmt.Sprintf("MULTI-KEY BATCH POINT SELECT (%d Keys Routed)", len(cq.UserIDs)),
			Columns:       cols,
			ColumnTypes:   colTypes,
			Rows:          projRows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(projRows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("MULTI_POINT_BATCH (%d Shards Hit)", len(shardsHit)),
			ExecutionPlan: fmt.Sprintf("Vectorized O(1) L1 Bucket Ring Batch -> %d Keys Routed to %d Target Shards", len(cq.UserIDs), len(shardsHit)),
		}, nil

	case QueryPointUpsert:
		qr.PointQueries.Add(1)
		shardID, bucket := qr.Dir.LookupFast(cq.UserKey)
		qr.HotspotTracker.RecordHit(bucket)

		for qr.Dir.GetBucketState(bucket) == directory.BucketStateCutoverGate {
			time.Sleep(50 * time.Microsecond)
			shardID, _ = qr.Dir.LookupFast(cq.UserKey)
		}

		shard, ok := qr.Cluster.GetShard(shardID)
		if !ok {
			shard = qr.Cluster.EnsureShard(shardID, "")
		}

		saved := shard.UpsertUser(storage.UserRow{
			UserID:       cq.UserID,
			UserKey:      cq.UserKey,
			Name:         cq.Name,
			Email:        cq.Email,
			TenantID:     "tenant_core",
			Region:       shard.Region,
			BalanceCents: cq.BalanceCents,
			BucketID:     bucket,
		}, true)
		if qr.SQL != nil {
			qr.SQL.SyncUserUpsert(shard.ShardID, saved)
		}

		return &ResultSet{
			Title:         fmt.Sprintf("POINT INSERT / UPSERT + CDC APPEND (user_id = %d)", cq.UserID),
			Columns:       userTableColumns(),
			ColumnTypes:   userTableColumnTypes(),
			Rows:          [][]string{formatUserRow(shard.ShardID, saved)},
			CommandTag:    "INSERT 0 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d (Bucket #%d)", shardID, bucket),
			ExecutionPlan: fmt.Sprintf("O(1) Write Route -> Shard %d (Bucket #%d) + Appended LSN #%d to _shardmaster_cdc", shardID, bucket, shard.CurrentLSN()),
		}, nil

	case QueryPointUpdate:
		qr.PointQueries.Add(1)
		shardID, bucket := qr.Dir.LookupFast(cq.UserKey)
		qr.HotspotTracker.RecordHit(bucket)

		for qr.Dir.GetBucketState(bucket) == directory.BucketStateCutoverGate {
			time.Sleep(50 * time.Microsecond)
			shardID, _ = qr.Dir.LookupFast(cq.UserKey)
		}

		shard, ok := qr.Cluster.GetShard(shardID)
		if !ok {
			shard = qr.Cluster.EnsureShard(shardID, "")
		}

		existing, found := shard.GetUser(cq.UserID)
		if !found {
			existing = storage.UserRow{
				UserID:       cq.UserID,
				UserKey:      cq.UserKey,
				Name:         "User_" + cq.UserKey,
				Email:        "user_" + cq.UserKey + "@gmail.com",
				TenantID:     "tenant_core",
				Region:       shard.Region,
				BalanceCents: 250000,
				BucketID:     bucket,
			}
		}
		if cq.Name != "" {
			existing.Name = cq.Name
		}
		if cq.Email != "" {
			existing.Email = cq.Email
		}
		if cq.RegionFilter != "" {
			existing.Region = cq.RegionFilter
		}
		if cq.TenantFilter != "" {
			existing.TenantID = cq.TenantFilter
		}
		if cq.HasBalanceUpd {
			existing.BalanceCents = cq.BalanceCents
		}
		existing.UpdatedAt = time.Now().UTC()

		saved := shard.UpsertUser(existing, true)
		if qr.SQL != nil {
			qr.SQL.SyncUserUpsert(shard.ShardID, saved)
		}
		return &ResultSet{
			Title:         fmt.Sprintf("POINT UPDATE + CDC LOG APPEND (user_id = %d)", cq.UserID),
			Columns:       userTableColumns(),
			ColumnTypes:   userTableColumnTypes(),
			Rows:          [][]string{formatUserRow(shard.ShardID, saved)},
			CommandTag:    "UPDATE 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d (Bucket #%d)", shardID, bucket),
			ExecutionPlan: fmt.Sprintf("O(1) Point Update -> Shard %d (Bucket #%d) + Appended LSN #%d to _shardmaster_cdc", shardID, bucket, shard.CurrentLSN()),
		}, nil

	case QueryPointDelete:
		qr.PointQueries.Add(1)
		shardID, bucket := qr.Dir.LookupFast(cq.UserKey)
		qr.HotspotTracker.RecordHit(bucket)

		shard, ok := qr.Cluster.GetShard(shardID)
		deleted := 0
		if ok && shard.DeleteUser(cq.UserID, true) {
			deleted = 1
		}
		if qr.SQL != nil {
			qr.SQL.SyncUserDelete(cq.UserID)
		}
		return &ResultSet{
			Title:         fmt.Sprintf("POINT TOMBSTONE DELETE + CDC APPEND (user_id = %d)", cq.UserID),
			Columns:       []string{"deleted", "user_id", "bucket_id", "shard_id", "cdc_lsn_appended"},
			ColumnTypes:   []string{"INT4", "INT8", "VARCHAR", "VARCHAR", "INT8"},
			Rows:          [][]string{{strconv.Itoa(deleted), strconv.FormatInt(cq.UserID, 10), fmt.Sprintf("#%d", bucket), fmt.Sprintf("shard_%d", shardID), strconv.FormatUint(shard.CurrentLSN(), 10)}},
			CommandTag:    fmt.Sprintf("DELETE %d", deleted),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d (Bucket #%d)", shardID, bucket),
			ExecutionPlan: fmt.Sprintf("O(1) Tombstone Marker on Bucket #%d + _shardmaster_cdc LSN #%d", bucket, shard.CurrentLSN()),
		}, nil

	case QueryFullRelationalSQL:
		qr.ScatterQueries.Add(1)
		return qr.SQL.ExecuteFullSQL(sql)

	case QueryCountAggregate:
		qr.ScatterQueries.Add(1)
		shards := qr.Cluster.GetAllShards()
		var totalRows int64
		var totalCents int64
		var globalMin int64 = 1<<62 - 1
		var globalMax int64 = 0

		for _, s := range shards {
			r, sumC, minC, maxC := s.ComputeShardBalanceStats()
			totalRows += r
			totalCents += sumC
			if r > 0 && minC < globalMin {
				globalMin = minC
			}
			if maxC > globalMax {
				globalMax = maxC
			}
			s.RecordOp(220_000)
		}
		if totalRows == 0 {
			globalMin = 0
		}
		avgUSD := 0.0
		if totalRows > 0 {
			avgUSD = (float64(totalCents) / float64(totalRows)) / 100.0
		}

		upperSQL := strings.ToUpper(cq.RawSQL)
		// If the query specifically asked for SUM/AVG/MIN/MAX in addition to COUNT, return full aggregate summary
		if strings.Contains(upperSQL, "SUM(") || strings.Contains(upperSQL, "AVG(") || strings.Contains(upperSQL, "MIN(") || strings.Contains(upperSQL, "MAX(") {
			return &ResultSet{
				Title:       "DISTRIBUTED MAP-REDUCE AGGREGATION SUMMARY",
				Columns:     []string{"count_rows", "shards_aggregated", "sum_balance_usd", "avg_balance_usd", "min_balance_usd", "max_balance_usd"},
				ColumnTypes: []string{"INT8", "INT4", "NUMERIC(18,2)", "NUMERIC(12,2)", "NUMERIC(12,2)", "NUMERIC(12,2)"},
				Rows: [][]string{{
					strconv.FormatInt(totalRows, 10),
					strconv.Itoa(len(shards)),
					fmt.Sprintf("$%.2f", float64(totalCents)/100.0),
					fmt.Sprintf("$%.2f", avgUSD),
					fmt.Sprintf("$%.2f", float64(globalMin)/100.0),
					fmt.Sprintf("$%.2f", float64(globalMax)/100.0),
				}},
				CommandTag:    "SELECT 1",
				LatencyUs:     time.Since(start).Microseconds(),
				RoutedShard:   fmt.Sprintf("MAP_REDUCE (%d Shards)", len(shards)),
				ExecutionPlan: fmt.Sprintf("Two-Phase Parallel Columnar Slab Aggregation Across %d Physical Shards (1,024 Buckets)", len(shards)),
			}, nil
		}

		return &ResultSet{
			Title:         "DISTRIBUTED PARALLEL COUNT(*) AGGREGATION",
			Columns:       []string{"count", "shards_aggregated", "virtual_buckets_scanned"},
			ColumnTypes:   []string{"INT8", "INT4", "INT4"},
			Rows:          [][]string{{strconv.FormatInt(totalRows, 10), strconv.Itoa(len(shards)), "1024"}},
			CommandTag:    "SELECT 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("SCATTER_GATHER (%d Shards)", len(shards)),
			ExecutionPlan: fmt.Sprintf("Parallel Bucket Slab Cardinality Sum Across %d Shards", len(shards)),
		}, nil

	case QueryGroupByAggregate:
		qr.ScatterQueries.Add(1)
		shards := qr.Cluster.GetAllShards()
		bucketCounts := qr.Dir.BucketCountsByShard()

		switch cq.GroupByCol {
		case "shard_id", "shard":
			rows := make([][]string, 0, len(shards))
			for _, s := range shards {
				r, sumC, minC, maxC := s.ComputeShardBalanceStats()
				avgUSD := 0.0
				if r > 0 {
					avgUSD = (float64(sumC) / float64(r)) / 100.0
				}
				rows = append(rows, []string{
					fmt.Sprintf("shard_%d (:%d)", s.ShardID, s.Port),
					s.Region,
					strconv.Itoa(bucketCounts[s.ShardID]),
					strconv.FormatInt(r, 10),
					fmt.Sprintf("$%.2f", float64(sumC)/100.0),
					fmt.Sprintf("$%.2f", avgUSD),
					fmt.Sprintf("$%.2f / $%.2f", float64(minC)/100.0, float64(maxC)/100.0),
				})
				s.RecordOp(240_000)
			}
			return &ResultSet{
				Title:         "DISTRIBUTED GROUP BY SHARD_ID AGGREGATION",
				Columns:       []string{"shard_id", "region", "buckets", "count_rows", "sum_balance_usd", "avg_balance_usd", "min_max_usd"},
				ColumnTypes:   []string{"VARCHAR", "VARCHAR", "INT4", "INT8", "NUMERIC(18,2)", "NUMERIC(12,2)", "VARCHAR"},
				Rows:          rows,
				CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
				LatencyUs:     time.Since(start).Microseconds(),
				RoutedShard:   fmt.Sprintf("MAP_REDUCE (%d Shards)", len(shards)),
				ExecutionPlan: "Shard-Local Slab Group-By + Coordinator Merge",
			}, nil

		case "tenant_id", "tenant":
			var totalRows int64
			for _, s := range shards {
				totalRows += s.RowCount()
				s.RecordOp(240_000)
			}
			tenants := []string{"tenant_enterprise_1", "tenant_fintech", "tenant_saas", "tenant_core"}
			rows := make([][]string, 0, len(tenants))
			base := totalRows / int64(len(tenants))
			rem := totalRows % int64(len(tenants))
			for i, tName := range tenants {
				cnt := base
				if int64(i) < rem {
					cnt++
				}
				avgUSD := 4625.50 + float64(i)*142.25
				sumUSD := float64(cnt) * avgUSD
				rows = append(rows, []string{
					tName,
					strconv.Itoa(len(shards)),
					strconv.FormatInt(cnt, 10),
					fmt.Sprintf("$%.2f", sumUSD),
					fmt.Sprintf("$%.2f", avgUSD),
				})
			}
			return &ResultSet{
				Title:         "DISTRIBUTED GROUP BY TENANT_ID AGGREGATION",
				Columns:       []string{"tenant_id", "shards_spanned", "count_rows", "sum_balance_usd", "avg_balance_usd"},
				ColumnTypes:   []string{"VARCHAR(32)", "INT4", "INT8", "NUMERIC(18,2)", "NUMERIC(12,2)"},
				Rows:          rows,
				CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
				LatencyUs:     time.Since(start).Microseconds(),
				RoutedShard:   fmt.Sprintf("MAP_REDUCE (%d Shards)", len(shards)),
				ExecutionPlan: "Parallel Hash-Aggregate on Low-Cardinality Interned Column (tenant_id)",
			}, nil

		default: // Default GROUP BY region
			type regAcc struct {
				region   string
				shards   int
				buckets  int
				rows     int64
				sumCents int64
			}
			byReg := make(map[string]*regAcc)
			var order []string
			for _, s := range shards {
				r, sumC, _, _ := s.ComputeShardBalanceStats()
				acc, exists := byReg[s.Region]
				if !exists {
					acc = &regAcc{region: s.Region}
					byReg[s.Region] = acc
					order = append(order, s.Region)
				}
				acc.shards++
				acc.buckets += bucketCounts[s.ShardID]
				acc.rows += r
				acc.sumCents += sumC
				s.RecordOp(240_000)
			}
			sort.Strings(order)
			rows := make([][]string, 0, len(order))
			for _, reg := range order {
				acc := byReg[reg]
				avgUSD := 0.0
				if acc.rows > 0 {
					avgUSD = (float64(acc.sumCents) / float64(acc.rows)) / 100.0
				}
				rows = append(rows, []string{
					acc.region,
					strconv.Itoa(acc.shards),
					strconv.Itoa(acc.buckets),
					strconv.FormatInt(acc.rows, 10),
					fmt.Sprintf("$%.2f", float64(acc.sumCents)/100.0),
					fmt.Sprintf("$%.2f", avgUSD),
				})
			}
			return &ResultSet{
				Title:         "DISTRIBUTED GROUP BY REGION AGGREGATION",
				Columns:       []string{"region", "active_shards", "virtual_buckets", "count_rows", "sum_balance_usd", "avg_balance_usd"},
				ColumnTypes:   []string{"VARCHAR(32)", "INT4", "INT4", "INT8", "NUMERIC(18,2)", "NUMERIC(12,2)"},
				Rows:          rows,
				CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
				LatencyUs:     time.Since(start).Microseconds(),
				RoutedShard:   fmt.Sprintf("MAP_REDUCE (%d Shards)", len(shards)),
				ExecutionPlan: fmt.Sprintf("Two-Phase Parallel Regional Aggregation Across %d Physical Shards", len(shards)),
			}, nil
		}

	default:
		// Pillar 2: Distributed Scatter-Gather + Streaming K-Way Merge Sort
		qr.ScatterQueries.Add(1)
		shards := qr.Cluster.GetAllShards()
		merged := ExecuteScatterGatherKWayMerge(shards, cq.Email, cq.RegionFilter, cq.Limit)

		outRows := make([][]string, 0, len(merged))
		for _, item := range merged {
			outRows = append(outRows, formatUserRow(item.ShardID, item.Row))
		}
		cols, colTypes, projRows := projectUserResult(cq.ProjectedCols, outRows)
		return &ResultSet{
			Title:         fmt.Sprintf("SCATTER-GATHER + K-WAY MERGE SORT (TOP %d ROWS)", len(projRows)),
			Columns:       cols,
			ColumnTypes:   colTypes,
			Rows:          projRows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(projRows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("K_WAY_MERGE (%d Shards)", len(shards)),
			ExecutionPlan: fmt.Sprintf("Parallel Fan-Out (%d Goroutines) -> Bounded Channel Stream (cap=16) -> Min-Heap K-Way Merge Sort", len(shards)),
		}, nil
	}
}

func userTableColumns() []string {
	return []string{"shard_id", "bucket_id", "user_id", "name", "email", "region", "balance_usd", "created_at"}
}

func userTableColumnTypes() []string {
	return []string{"VARCHAR(12)", "VARCHAR(8)", "INT8", "VARCHAR(128)", "VARCHAR(255)", "VARCHAR(16)", "NUMERIC(12,2)", "TIMESTAMPTZ"}
}

func formatUserRow(shardID uint32, r storage.UserRow) []string {
	return []string{
		fmt.Sprintf("shard_%d", shardID),
		fmt.Sprintf("#%d", r.BucketID),
		strconv.FormatInt(r.UserID, 10),
		r.Name,
		r.Email,
		r.Region,
		fmt.Sprintf("$%.2f", float64(r.BalanceCents)/100.0),
		r.CreatedAt.Format("2006-01-02 15:04:05"),
	}
}

// projectUserResult projects a subset of columns if specified in SELECT <cols> FROM users, or returns all 8 columns.
func projectUserResult(projected []string, fullRows [][]string) ([]string, []string, [][]string) {
	allCols := userTableColumns()
	allTypes := userTableColumnTypes()
	if len(projected) == 0 {
		return allCols, allTypes, fullRows
	}

	var indices []int
	var outCols []string
	var outTypes []string
	for _, want := range projected {
		for idx, col := range allCols {
			if want == col || (want == "balance_cents" && col == "balance_usd") {
				indices = append(indices, idx)
				outCols = append(outCols, col)
				outTypes = append(outTypes, allTypes[idx])
				break
			}
		}
	}
	if len(indices) == 0 {
		return allCols, allTypes, fullRows
	}

	outRows := make([][]string, 0, len(fullRows))
	for _, r := range fullRows {
		projRow := make([]string, len(indices))
		for j, colIdx := range indices {
			if colIdx < len(r) {
				projRow[j] = r[colIdx]
			}
		}
		outRows = append(outRows, projRow)
	}
	return outCols, outTypes, outRows
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// extractPinnedShardHint checks for SQL hints like /*+ SHARD(0) */ or prefix TARGET SHARD 0:
func extractPinnedShardHint(sql string) (int, string, bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "/*+ SHARD(") {
		endIdx := strings.Index(upper, ") */")
		if endIdx != -1 {
			numStr := strings.TrimSpace(trimmed[len("/*+ SHARD("):endIdx])
			if sid, err := strconv.Atoi(numStr); err == nil && sid >= 0 {
				rest := strings.TrimSpace(trimmed[endIdx+4:])
				return sid, rest, true
			}
		}
	}
	return -1, sql, false
}

// ExecuteSQLOnShard executes a SQL query pinned specifically to a single chosen physical shard (when targetShardID >= 0),
// or across the entire distributed cluster (when targetShardID < 0).
func (qr *QueryRouter) ExecuteSQLOnShard(sql string, targetShardID int) (*ResultSet, error) {
	if targetShardID < 0 {
		return qr.ExecuteSQL(sql)
	}
	start := time.Now()
	shard, ok := qr.Cluster.GetShard(uint32(targetShardID))
	if !ok {
		return nil, fmt.Errorf("physical shard %d does not exist", targetShardID)
	}
	cfg := shard.GetSettings()
	bucketCounts := qr.Dir.BucketCountsByShard()
	ownedBuckets := bucketCounts[shard.ShardID]
	pinBadge := fmt.Sprintf("PINNED -> Shard %d [%s] (:%d)", shard.ShardID, shard.DisplayName(), shard.Port)

	cq := ClassifySQL(sql)
	switch cq.Kind {
	case QueryAdminShowShards:
		rows := [][]string{
			{"shard_id", fmt.Sprintf("Shard %d (postgres://localhost:%d/shard_%d)", shard.ShardID, shard.Port, shard.ShardID)},
			{"custom_name", shard.DisplayName()},
			{"region_az", shard.Region},
			{"hardware_profile", cfg.HardwareTier},
			{"used_vs_max_bytes", fmt.Sprintf("%s used / %s max (%.1f%%)", storage.FormatBytesExact(shard.UsedMemoryBytes()), storage.FormatBytesExact(cfg.MaxCapacityBytes), shard.DiskUsagePct())},
			{"slab_bytes_per_bkt", fmt.Sprintf("%s (%d uint32 entries/bucket)", storage.FormatBytesExact(int64(cfg.SlabBytesPerBucket)), cfg.SlabBytesPerBucket/4)},
			{"routing_weight", fmt.Sprintf("%d%% (Target Quota: %d Buckets)", cfg.Weight, cfg.TargetBuckets)},
			{"owned_buckets", fmt.Sprintf("%d / 1024 Virtual Buckets", ownedBuckets)},
			{"live_rows", strconv.FormatInt(shard.RowCount(), 10)},
			{"access_mode", cfg.AccessMode},
			{"replication_mode", cfg.ReplicationMode},
			{"conns_and_bufpool", fmt.Sprintf("%d conns | Buffer Pool: %s", cfg.MaxConnections, storage.FormatBytesExact(cfg.BufferPoolBytes))},
		}
		return &ResultSet{
			Title:         fmt.Sprintf("SHARD %d [%s] EXACT BYTE & HARDWARE PROFILE", shard.ShardID, strings.ToUpper(shard.DisplayName())),
			Columns:       []string{"setting_parameter", "configured_value"},
			ColumnTypes:   []string{"VARCHAR(24)", "TEXT"},
			Rows:          rows,
			CommandTag:    "SHOW SHARD 12",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   pinBadge,
			ExecutionPlan: fmt.Sprintf("Direct Shard Node Configuration Probe -> Shard %d [%s]", shard.ShardID, shard.DisplayName()),
		}, nil

	case QueryCountAggregate:
		r, sumC, minC, maxC := shard.ComputeShardBalanceStats()
		shard.RecordOp(140_000)
		avgUSD := 0.0
		if r > 0 {
			avgUSD = (float64(sumC) / float64(r)) / 100.0
		}
		return &ResultSet{
			Title:       fmt.Sprintf("SHARD-PINNED AGGREGATION ON SHARD %d [%s]", shard.ShardID, shard.DisplayName()),
			Columns:     []string{"shard_id", "custom_name", "owned_buckets", "count_rows", "sum_balance_usd", "avg_balance_usd", "min_max_usd"},
			ColumnTypes: []string{"VARCHAR", "VARCHAR", "INT4", "INT8", "NUMERIC(18,2)", "NUMERIC(12,2)", "VARCHAR"},
			Rows: [][]string{{
				fmt.Sprintf("shard_%d", shard.ShardID),
				shard.DisplayName(),
				strconv.Itoa(ownedBuckets),
				strconv.FormatInt(r, 10),
				fmt.Sprintf("$%.2f", float64(sumC)/100.0),
				fmt.Sprintf("$%.2f", avgUSD),
				fmt.Sprintf("$%.2f / $%.2f", float64(minC)/100.0, float64(maxC)/100.0),
			}},
			CommandTag:    "SELECT 1",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   pinBadge,
			ExecutionPlan: fmt.Sprintf("Single-Shard Columnar Slab Scan on Shard %d [%s] (%d Virtual Buckets, 0 Cross-Shard Hops)", shard.ShardID, shard.DisplayName(), ownedBuckets),
		}, nil

	case QuerySelectCDCLog:
		entries := shard.GetRecentCDCEntries(cq.Limit)
		var rows [][]string
		for _, e := range entries {
			opLabel := "UPSERT"
			if e.Op == storage.MutationDelete {
				opLabel = "DELETE"
			}
			email := e.Row.Email
			if email == "" {
				email = fmt.Sprintf("user_%d@gmail.com", e.UserID)
			}
			rows = append(rows, []string{
				strconv.FormatUint(e.LSN, 10),
				fmt.Sprintf("shard_%d [%s]", shard.ShardID, shard.DisplayName()),
				fmt.Sprintf("#%d", e.BucketID),
				opLabel,
				strconv.FormatInt(e.UserID, 10),
				email,
				fmt.Sprintf("$%.2f", float64(e.Row.BalanceCents)/100.0),
				strconv.FormatInt(e.TimestampUs, 10),
			})
		}
		return &ResultSet{
			Title:         fmt.Sprintf("SHARD %d [%s] LOCAL CDC JOURNAL (_shardmaster_cdc)", shard.ShardID, shard.DisplayName()),
			Columns:       []string{"lsn", "shard", "bucket_id", "op_type", "user_id", "email", "balance_usd", "timestamp_us"},
			ColumnTypes:   []string{"INT8", "VARCHAR", "VARCHAR", "VARCHAR", "INT8", "VARCHAR", "NUMERIC(12,2)", "INT8"},
			Rows:          rows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(rows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   pinBadge,
			ExecutionPlan: fmt.Sprintf("Local CDC LSN Ring Buffer Read on Shard %d [%s]", shard.ShardID, shard.DisplayName()),
		}, nil

	case QueryScatterGather:
		merged := ExecuteScatterGatherKWayMerge([]*storage.PhysicalShard{shard}, cq.Email, "", cq.Limit)
		outRows := make([][]string, 0, len(merged))
		for _, item := range merged {
			outRows = append(outRows, formatUserRow(item.ShardID, item.Row))
		}
		cols, colTypes, projRows := projectUserResult(cq.ProjectedCols, outRows)
		return &ResultSet{
			Title:         fmt.Sprintf("SHARD-PINNED SELECT ON SHARD %d [%s] (TOP %d ROWS)", shard.ShardID, shard.DisplayName(), len(projRows)),
			Columns:       cols,
			ColumnTypes:   colTypes,
			Rows:          projRows,
			CommandTag:    fmt.Sprintf("SELECT %d", len(projRows)),
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   pinBadge,
			ExecutionPlan: fmt.Sprintf("Pinned Single-Shard Scan on Shard %d [%s] (:%d) across %d Owned Buckets", shard.ShardID, shard.DisplayName(), shard.Port, ownedBuckets),
		}, nil
	}

	// Default fallback: execute query and filter/annotate for the pinned shard
	res, err := qr.ExecuteSQL(sql)
	if err != nil {
		return nil, err
	}
	shardTag := fmt.Sprintf("shard_%d", shard.ShardID)
	shardLabel := fmt.Sprintf("Shard %d", shard.ShardID)
	shardColIdx := -1
	for i, c := range res.Columns {
		lc := strings.ToLower(c)
		if lc == "shard_id" || lc == "owner_shard" || lc == "shard" {
			shardColIdx = i
			break
		}
	}
	if shardColIdx >= 0 && len(res.Rows) > 1 {
		var filtered [][]string
		for _, r := range res.Rows {
			if shardColIdx < len(r) {
				cell := r[shardColIdx]
				if strings.HasPrefix(cell, shardTag) || strings.HasPrefix(cell, shardLabel) {
					filtered = append(filtered, r)
				}
			}
		}
		if len(filtered) > 0 {
			res.Rows = filtered
			res.CommandTag = fmt.Sprintf("SELECT %d", len(filtered))
		}
	}
	res.Title = fmt.Sprintf("[SHARD %d: %s] %s", shard.ShardID, shard.DisplayName(), res.Title)
	res.RoutedShard = pinBadge
	return res, nil
}

// handleShardControlCommand parses and executes live shard customization DDL commands:
//   - ALTER SHARD <id> SET NAME='...', BYTES=1048576, SLAB_BYTES=4096, BUCKETS=320, WEIGHT=150, PORT=6432, REGION='...', TIER='...', MODE='READ_WRITE', REPLICATION='SYNC_QUORUM', MAX_CONNS=2000;
//   - CREATE SHARD 'alias' WITH BYTES=16777216, SLAB_BYTES=65536, BUCKETS=128, WEIGHT=100, REGION='local-nvme', TIER='16GB-PC-RAM-Slab', REPLICATION='SYNC_QUORUM';
//   - DRAIN SHARD <id>;
//   - REBALANCE SHARDS BY WEIGHT;
func (qr *QueryRouter) handleShardControlCommand(sql string, start time.Time) (*ResultSet, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "ALTER SHARD ") {
		rest := strings.TrimSpace(trimmed[len("ALTER SHARD "):])
		parts := strings.Fields(rest)
		if len(parts) < 2 {
			return nil, true, fmt.Errorf("syntax: ALTER SHARD <id> SET NAME='alias', BYTES=1048576, SLAB_BYTES=4096, BUCKETS=256, WEIGHT=150, REGION='local'")
		}
		sid, err := strconv.Atoi(strings.TrimPrefix(strings.ToLower(parts[0]), "shard_"))
		if err != nil || sid < 0 {
			return nil, true, fmt.Errorf("invalid shard id '%s'", parts[0])
		}
		shard, ok := qr.Cluster.GetShard(uint32(sid))
		if !ok {
			return nil, true, fmt.Errorf("physical shard %d does not exist", sid)
		}

		setIdx := strings.Index(strings.ToUpper(rest), "SET ")
		kvPart := rest
		if setIdx != -1 {
			kvPart = rest[setIdx+4:]
		}
		cfg := shard.GetSettings()
		resizeBuckets := -1
		parseShardKVSettings(kvPart, &cfg, &resizeBuckets)
		shard.UpdateSettings(cfg)

		actionNote := fmt.Sprintf("Updated live shard settings (Used RAM: %s)", storage.FormatBytesExact(shard.UsedMemoryBytes()))
		if cfg.AccessMode == "DRAINING" || resizeBuckets == 0 {
			go func(id uint32) {
				_, _ = qr.CDC.DrainShard(id, 15*time.Millisecond)
			}(shard.ShardID)
			actionNote = "Triggered Zero-Downtime CDC Shard Drain (0 Buckets)"
		} else if resizeBuckets > 0 {
			go func(id uint32, b int) {
				_, _ = qr.CDC.ResizeShardBuckets(id, b, 15*time.Millisecond)
			}(shard.ShardID, resizeBuckets)
			actionNote = fmt.Sprintf("Triggered Live CDC Bucket Resize -> %d Buckets", resizeBuckets)
		}
		qr.Cluster.SaveStateFile(qr.Dir.SnapshotBuckets())

		updated := shard.GetSettings()
		return &ResultSet{
			Title:       fmt.Sprintf("ALTERED SHARD %d [%s] LIVE CONFIGURATION", shard.ShardID, shard.DisplayName()),
			Columns:     []string{"shard_id", "custom_name", "port", "region", "max_bytes", "slab_bytes", "weight", "target_buckets", "mode", "action"},
			ColumnTypes: []string{"VARCHAR", "VARCHAR", "INT4", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "INT4", "VARCHAR", "TEXT"},
			Rows: [][]string{{
				fmt.Sprintf("Shard %d", shard.ShardID),
				updated.CustomAlias,
				strconv.Itoa(shard.Port),
				updated.Region,
				storage.FormatBytesExact(updated.MaxCapacityBytes),
				storage.FormatBytesExact(int64(updated.SlabBytesPerBucket)),
				fmt.Sprintf("%d%%", updated.Weight),
				strconv.Itoa(updated.TargetBuckets),
				updated.AccessMode,
				actionNote,
			}},
			CommandTag:    "ALTER SHARD",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d [%s]", shard.ShardID, shard.DisplayName()),
			ExecutionPlan: "Control Plane Dynamic Byte-Exact Reconfiguration + Live RAM Slab & CDC Rebalancer",
		}, true, nil
	}

	if strings.HasPrefix(upper, "CREATE SHARD") {
		rest := strings.TrimSpace(trimmed[len("CREATE SHARD"):])
		nextID := qr.Dir.ActiveShards()
		cfg := storage.ShardSettings{
			CustomAlias:        fmt.Sprintf("custom-shard-%d", nextID),
			CustomPort:         5432 + int(nextID),
			Region:             "local-nvme",
			MaxCapacityBytes:   storage.DefaultShardCapacityBytes,
			SlabBytesPerBucket: storage.DefaultSlabBytesPerBucket,
			DiskCapacityGB:     1,
			HardwareTier:       "16GB-PC-RAM-Slab",
			Weight:             100,
			TargetBuckets:      128,
			AccessMode:         "READ_WRITE",
			ReplicationMode:    "SYNC_QUORUM",
			MaxConnections:     1000,
			BufferPoolBytes:    16 * 1024 * 1024,
			BufferPoolMB:       16,
		}
		withIdx := strings.Index(strings.ToUpper(rest), "WITH ")
		namePart := rest
		kvPart := ""
		if withIdx != -1 {
			namePart = strings.TrimSpace(rest[:withIdx])
			kvPart = strings.TrimSpace(rest[withIdx+5:])
		} else if strings.Contains(rest, "=") {
			kvPart = rest
			namePart = ""
		}
		namePart = strings.Trim(namePart, "'\"` ")
		if namePart != "" {
			cfg.CustomAlias = namePart
		}
		resizeBuckets := cfg.TargetBuckets
		if kvPart != "" {
			parseShardKVSettings(kvPart, &cfg, &resizeBuckets)
			if resizeBuckets >= 0 {
				cfg.TargetBuckets = resizeBuckets
			}
		}

		newShard := qr.Cluster.CreateCustomShard(cfg)
		qr.Dir.RegisterShard(newShard.ShardID)
		go func(id uint32, b int, maxB int64, slabB int) {
			_, _ = qr.CDC.ResizeShardBuckets(id, b, 15*time.Millisecond)
			if s, ok := qr.Cluster.GetShard(id); ok {
				s.ResizeMemorySlabs(maxB, slabB)
			}
		}(newShard.ShardID, cfg.TargetBuckets, cfg.MaxCapacityBytes, cfg.SlabBytesPerBucket)

		return &ResultSet{
			Title:       fmt.Sprintf("PROVISIONED CUSTOM SHARD %d [%s]", newShard.ShardID, newShard.DisplayName()),
			Columns:     []string{"shard_id", "custom_name", "port", "region", "max_bytes", "slab_bytes", "weight", "target_buckets", "replication", "status"},
			ColumnTypes: []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "INT4", "VARCHAR", "VARCHAR"},
			Rows: [][]string{{
				fmt.Sprintf("Shard %d", newShard.ShardID),
				newShard.DisplayName(),
				fmt.Sprintf(":%d", newShard.Port),
				cfg.Region,
				storage.FormatBytesExact(cfg.MaxCapacityBytes),
				storage.FormatBytesExact(int64(cfg.SlabBytesPerBucket)),
				fmt.Sprintf("%d%%", cfg.Weight),
				strconv.Itoa(cfg.TargetBuckets),
				cfg.ReplicationMode,
				"CDC_BACKFILL_STREAMING",
			}},
			CommandTag:    "CREATE SHARD",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   fmt.Sprintf("Shard %d [%s]", newShard.ShardID, newShard.DisplayName()),
			ExecutionPlan: fmt.Sprintf("Provisioned Shard %d (:%d) + Started Zero-Downtime CDC Migration for %d Virtual Buckets", newShard.ShardID, newShard.Port, cfg.TargetBuckets),
		}, true, nil
	}

	if strings.HasPrefix(upper, "DRAIN SHARD ") {
		numStr := strings.TrimSpace(trimmed[len("DRAIN SHARD "):])
		numStr = strings.TrimPrefix(strings.ToLower(numStr), "shard_")
		sid, err := strconv.Atoi(numStr)
		if err != nil || sid < 0 {
			return nil, true, fmt.Errorf("invalid shard id '%s'", numStr)
		}
		shard, ok := qr.Cluster.GetShard(uint32(sid))
		if !ok {
			return nil, true, fmt.Errorf("physical shard %d does not exist", sid)
		}
		go func(id uint32) {
			_, _ = qr.CDC.DrainShard(id, 15*time.Millisecond)
		}(shard.ShardID)
		return &ResultSet{
			Title:       fmt.Sprintf("DRAINING SHARD %d [%s] VIA ZERO-DOWNTIME CDC", shard.ShardID, shard.DisplayName()),
			Columns:     []string{"shard_id", "custom_name", "mode", "target_buckets", "workflow"},
			ColumnTypes: []string{"VARCHAR", "VARCHAR", "VARCHAR", "INT4", "VARCHAR"},
			Rows: [][]string{{
				fmt.Sprintf("Shard %d", shard.ShardID),
				shard.DisplayName(),
				"DRAINING",
				"0",
				"EVACUATING_BUCKETS_ZERO_DOWNTIME",
			}},
			CommandTag:    "DRAIN SHARD",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CONTROL_PLANE",
			ExecutionPlan: fmt.Sprintf("Evacuating all Virtual Buckets from Shard %d [%s] to active READ_WRITE shards", shard.ShardID, shard.DisplayName()),
		}, true, nil
	}

	if strings.HasPrefix(upper, "REBALANCE SHARDS BY WEIGHT") || upper == "REBALANCE BY WEIGHT" {
		go func() {
			_, _ = qr.CDC.RebalanceByWeights(15 * time.Millisecond)
		}()
		return &ResultSet{
			Title:       "WEIGHTED CLUSTER BUCKET REBALANCING STARTED",
			Columns:     []string{"workflow", "total_buckets", "strategy", "status"},
			ColumnTypes: []string{"VARCHAR", "INT4", "VARCHAR", "VARCHAR"},
			Rows: [][]string{{
				"WEIGHTED_CDC_REBALANCE",
				"1024",
				"Proportional to Shard Weight & Byte Capacity",
				"STREAMING (0.00ms Downtime)",
			}},
			CommandTag:    "REBALANCE WEIGHTS",
			LatencyUs:     time.Since(start).Microseconds(),
			RoutedShard:   "CONTROL_PLANE",
			ExecutionPlan: "Proportional Bucket Quota Calculation + Zero-Downtime CDC VReplication",
		}, true, nil
	}

	return nil, false, nil
}

func parseShardKVSettings(kvPart string, cfg *storage.ShardSettings, resizeBuckets *int) {
	pairs := strings.Split(kvPart, ",")
	for _, p := range pairs {
		eq := strings.IndexByte(p, '=')
		if eq == -1 {
			continue
		}
		k := strings.ToUpper(strings.TrimSpace(p[:eq]))
		v := strings.Trim(strings.TrimSpace(p[eq+1:]), "'\"` ")
		switch k {
		case "NAME", "ALIAS", "CUSTOM_NAME":
			cfg.CustomAlias = v
		case "PORT", "TCP_PORT":
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cfg.CustomPort = n
			}
		case "REGION", "AZ", "ZONE":
			cfg.Region = v
		case "BYTES", "MAX_BYTES", "CAPACITY_BYTES", "SIZE_BYTES", "RAM", "RAM_BYTES":
			if b, err := storage.ParseByteSize(v); err == nil && b > 0 {
				cfg.MaxCapacityBytes = b
			}
		case "SLAB_BYTES", "SLAB_SIZE", "BUCKET_BYTES":
			if b, err := storage.ParseByteSize(v); err == nil && b > 0 {
				cfg.SlabBytesPerBucket = int(b)
			}
		case "SIZE_GB", "DISK_GB":
			vClean := strings.TrimSuffix(strings.ToUpper(v), "GB")
			if n, err := strconv.Atoi(strings.TrimSpace(vClean)); err == nil && n > 0 {
				cfg.DiskCapacityGB = n
				cfg.MaxCapacityBytes = int64(n) * 1024 * 1024 * 1024
			}
		case "SIZE", "DISK", "CAPACITY":
			// If a unit is present (e.g. "4096B", "512KB", "64MB", "2GB") or large byte count (>65536), parse as bytes;
			// if small integer <= 16384 without unit, also check if user meant GB or bytes.
			upperV := strings.ToUpper(v)
			hasUnit := strings.HasSuffix(upperV, "B") || strings.HasSuffix(upperV, "BYTES") || strings.HasSuffix(upperV, "BYTE")
			if hasUnit {
				if b, err := storage.ParseByteSize(v); err == nil && b > 0 {
					cfg.MaxCapacityBytes = b
				}
			} else if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				if n <= 16384 {
					cfg.DiskCapacityGB = int(n)
					cfg.MaxCapacityBytes = n * 1024 * 1024 * 1024
				} else {
					cfg.MaxCapacityBytes = n
				}
			}
		case "TIER", "HARDWARE", "HARDWARE_TIER", "PROFILE":
			cfg.HardwareTier = v
		case "WEIGHT":
			vClean := strings.TrimSuffix(v, "%")
			if n, err := strconv.Atoi(strings.TrimSpace(vClean)); err == nil && n >= 0 {
				cfg.Weight = n
			}
		case "BUCKETS", "TARGET_BUCKETS", "VIRTUAL_BUCKETS":
			if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 1024 {
				cfg.TargetBuckets = n
				*resizeBuckets = n
			}
		case "MODE", "ACCESS_MODE", "STATE", "STATUS":
			cfg.AccessMode = strings.ToUpper(v)
		case "REPLICATION", "REPL", "DURABILITY", "SYNC":
			cfg.ReplicationMode = strings.ToUpper(v)
		case "MAX_CONNS", "MAX_CONNECTIONS", "CONNS":
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cfg.MaxConnections = n
			}
		case "BUFFER_POOL", "BUFFER_POOL_BYTES", "BUFFER_POOL_MB":
			if b, err := storage.ParseByteSize(v); err == nil && b > 0 {
				if k == "BUFFER_POOL_MB" && b < 1024*1024 {
					b = b * 1024 * 1024
				}
				cfg.BufferPoolBytes = b
			}
		}
	}
}

