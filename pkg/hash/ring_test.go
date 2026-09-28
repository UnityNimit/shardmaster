package hash_test

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"

	"shardmaster/pkg/cdc"
	"shardmaster/pkg/directory"
	"shardmaster/pkg/hash"
	"shardmaster/pkg/hotspot"
	"shardmaster/pkg/pgwire"
	"shardmaster/pkg/router"
	"shardmaster/pkg/storage"
	"shardmaster/pkg/tui"
)

func TestVirtualBucketRingAndSplit(t *testing.T) {
	b1 := hash.ComputeBucket("user_42")
	b2 := hash.ComputeBucket("user_42")
	if b1 != b2 {
		t.Fatalf("expected deterministic bucket, got %d != %d", b1, b2)
	}
	if b1 >= hash.TotalVirtualBuckets {
		t.Fatalf("bucket %d out of range [0, 1023]", b1)
	}

	init4 := hash.InitialBucketAssignment(4)
	next5, ranges := hash.ComputeOptimalSplit(init4, 5)
	if len(ranges) == 0 {
		t.Fatalf("expected non-empty migration ranges for 4 -> 5 split")
	}

	counts := make(map[uint32]int)
	for _, owner := range next5 {
		counts[owner]++
	}
	for s := uint32(0); s < 5; s++ {
		if counts[s] < 204 || counts[s] > 205 {
			t.Fatalf("expected balanced 204-205 buckets on shard %d, got %d", s, counts[s])
		}
	}
}

func TestCDCAndVDiffZeroDowntime(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(2000, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)

	snap, err := cdcEngine.RebalanceToShards(8, 0)
	if err != nil {
		t.Fatalf("RebalanceToShards failed: %v", err)
	}
	if snap.RowsMigrated == 0 {
		t.Fatalf("expected rows migrated > 0")
	}

	history := cdcEngine.GetVDiffHistory()
	if len(history) == 0 {
		t.Fatalf("expected VDiff verification reports")
	}
	for _, vd := range history {
		if !vd.Matched {
			t.Fatalf("VDiff mismatch on bucket [%d-%d]: %s != %s",
				vd.StartBucket, vd.EndBucket, vd.SourceDigest, vd.TargetDigest)
		}
	}

	// Verify total rows across all 8 shards is still exactly 2,000 (zero data loss!)
	var totalAfter int64
	for _, s := range cluster.GetAllShards() {
		totalAfter += s.RowCount()
	}
	if totalAfter != 2000 {
		t.Fatalf("expected 2000 total rows after 4->8 split, got %d", totalAfter)
	}
}

func TestScatterGatherKWayMergeAndHotspot(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(1000, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// Test Pillar 2: Distributed Scatter-Gather + K-Way Merge Sort
	res, err := qr.ExecuteSQL("SELECT * FROM users WHERE email LIKE '%@gmail.com' ORDER BY created_at DESC LIMIT 10;")
	if err != nil {
		t.Fatalf("Scatter-gather failed: %v", err)
	}
	if len(res.Rows) != 10 {
		t.Fatalf("expected 10 merged rows, got %d", len(res.Rows))
	}

	// Test Pillar 5: Autonomous EWMA Hotspot Detection on Bucket #412
	tracker.InjectBucketTrafficSpike(412, 7500)
	alert := tracker.TickAndEvaluate(1.0)
	if alert == nil {
		t.Fatalf("expected autonomous hotspot alert on Bucket #412")
	}
	if alert.BucketID != 412 {
		t.Fatalf("expected Bucket #412, got #%d", alert.BucketID)
	}
}

func TestPGWireProtocolServer(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(500, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	srv := pgwire.NewServer("127.0.0.1:16000", "", qr)
	go func() {
		_ = srv.Start()
	}()
	defer srv.Close()
	time.Sleep(80 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:16000", time.Second)
	if err != nil {
		t.Fatalf("failed to connect to PGWire server: %v", err)
	}
	defer conn.Close()

	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
	if err := fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "admin", "database": "shardmaster"},
	}); err != nil {
		t.Fatalf("send startup failed: %v", err)
	}

	// Wait for ReadyForQuery
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive startup response failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// Send SQL point query over PGWire v3.0
	if err := fe.Send(&pgproto3.Query{String: "SELECT * FROM users WHERE user_id = 42;"}); err != nil {
		t.Fatalf("send query failed: %v", err)
	}

	gotRows := 0
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive query response failed: %v", err)
		}
		if _, ok := msg.(*pgproto3.DataRow); ok {
			gotRows++
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if gotRows != 1 {
		t.Fatalf("expected 1 DataRow over PGWire for user_id=42, got %d", gotRows)
	}
}

func TestSQLEngineSchemasAndQueries(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(2000, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// 1. SHOW TABLES (users, orders, payments, vip_users_view + _shardmaster_* tables)
	res, err := qr.ExecuteSQL("SHOW TABLES;")
	if err != nil || len(res.Rows) < 8 {
		t.Fatalf("expected >= 8 tables from SHOW TABLES, got err=%v rows=%d", err, len(res.Rows))
	}

	// 2. DESCRIBE users (11 cols), DESCRIBE orders (11 cols), DESCRIBE payments (9 cols)
	res, err = qr.ExecuteSQL("DESCRIBE users;")
	if err != nil || len(res.Rows) != 11 {
		t.Fatalf("expected 11 columns from DESCRIBE users, got err=%v rows=%d", err, len(res.Rows))
	}
	res, err = qr.ExecuteSQL("DESCRIBE orders;")
	if err != nil || len(res.Rows) != 11 {
		t.Fatalf("expected 11 columns from DESCRIBE orders, got err=%v rows=%d", err, len(res.Rows))
	}
	res, err = qr.ExecuteSQL("DESCRIBE payments;")
	if err != nil || len(res.Rows) != 9 {
		t.Fatalf("expected 9 columns from DESCRIBE payments, got err=%v rows=%d", err, len(res.Rows))
	}

	// 3. Full Custom DDL + DML: CREATE TABLE + INSERT + ALTER TABLE + SELECT + DESCRIBE + DROP TABLE
	_, err = qr.ExecuteSQL("CREATE TABLE invoices (invoice_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, amount_usd NUMERIC(12,2) DEFAULT 99.50);")
	if err != nil {
		t.Fatalf("CREATE TABLE invoices failed: %v", err)
	}
	_, err = qr.ExecuteSQL("INSERT INTO invoices (invoice_id, user_id, amount_usd) VALUES (9001, 42, 1450.75), (9002, 777, 3200.00);")
	if err != nil {
		t.Fatalf("INSERT INTO invoices failed: %v", err)
	}
	_, err = qr.ExecuteSQL("ALTER TABLE invoices ADD COLUMN status VARCHAR(32) DEFAULT 'PAID';")
	if err != nil {
		t.Fatalf("ALTER TABLE invoices failed: %v", err)
	}
	res, err = qr.ExecuteSQL("DESCRIBE invoices;")
	if err != nil || len(res.Rows) != 4 {
		t.Fatalf("expected 4 columns from DESCRIBE invoices after ALTER TABLE, got err=%v rows=%d", err, len(res.Rows))
	}
	res, err = qr.ExecuteSQL("SELECT i.invoice_id, u.name, i.amount_usd, i.status FROM invoices i INNER JOIN users u ON u.user_id = i.user_id ORDER BY i.invoice_id;")
	if err != nil || len(res.Rows) != 2 {
		t.Fatalf("expected 2 joined rows from invoices JOIN users, got err=%v rows=%d", err, len(res.Rows))
	}
	_, err = qr.ExecuteSQL("DROP TABLE invoices;")
	if err != nil {
		t.Fatalf("DROP TABLE invoices failed: %v", err)
	}

	// 4. Column projection + Multi-Key Batch IN (...)
	res, err = qr.ExecuteSQL("SELECT user_id, name, email, balance_usd FROM users WHERE user_id IN (42, 100, 777);")
	if err != nil || len(res.Rows) != 3 || len(res.Columns) != 4 {
		t.Fatalf("expected 3 rows and 4 projected cols, got err=%v rows=%d cols=%d", err, len(res.Rows), len(res.Columns))
	}

	// 5. Distributed GROUP BY region
	res, err = qr.ExecuteSQL("SELECT region, COUNT(*), SUM(balance_usd), AVG(balance_usd) FROM users GROUP BY region;")
	if err != nil || len(res.Rows) != 4 {
		t.Fatalf("expected 4 regional groups, got err=%v rows=%d", err, len(res.Rows))
	}

	// 6. 3-Table Co-Located INNER JOIN (users + orders + payments)
	res, err = qr.ExecuteSQL("SELECT u.shard_id, u.user_id, u.name, o.order_id, o.product_name, o.amount_usd, p.payment_method FROM users u INNER JOIN orders o ON u.user_id = o.user_id INNER JOIN payments p ON o.order_id = p.order_id ORDER BY o.amount_usd DESC LIMIT 5;")
	if err != nil || len(res.Rows) != 5 {
		t.Fatalf("expected 5 rows from 3-table INNER JOIN, got err=%v rows=%d", err, len(res.Rows))
	}

	// 7. Window Function RANK() OVER (PARTITION BY region ORDER BY balance_cents DESC)
	res, err = qr.ExecuteSQL("SELECT shard_id, user_id, name, region, balance_usd, RANK() OVER (PARTITION BY region ORDER BY balance_cents DESC) AS regional_rank FROM users LIMIT 8;")
	if err != nil || len(res.Rows) != 8 {
		t.Fatalf("expected 8 rows from Window Function query, got err=%v rows=%d", err, len(res.Rows))
	}

	// 8. Common Table Expression (WITH CTE + GROUP BY + HAVING)
	res, err = qr.ExecuteSQL("WITH high_value AS (SELECT * FROM users WHERE balance_cents >= 500000) SELECT region, COUNT(*) AS vip_users, ROUND(AVG(balance_usd), 2) AS avg_vip_usd, MAX(balance_usd) AS max_vip_usd FROM high_value GROUP BY region HAVING COUNT(*) >= 5 ORDER BY avg_vip_usd DESC;")
	if err != nil || len(res.Rows) == 0 {
		t.Fatalf("expected rows from WITH CTE + HAVING query, got err=%v rows=%d", err, len(res.Rows))
	}

	// 9. Subquery + Custom Scalar Functions xxhash64(), virtual_bucket(), target_shard()
	res, err = qr.ExecuteSQL("SELECT user_id, name, balance_usd, xxhash64(user_id) AS xxhash64_hex, virtual_bucket(user_id) AS bucket_id, target_shard(user_id) AS routed_shard FROM users WHERE balance_cents > (SELECT AVG(balance_cents) FROM users) ORDER BY balance_cents DESC LIMIT 6;")
	if err != nil || len(res.Rows) != 6 {
		t.Fatalf("expected 6 rows from Subquery + custom hash functions, got err=%v rows=%d", err, len(res.Rows))
	}

	// 10. UPDATE + CDC Journal Query
	_, err = qr.ExecuteSQL("UPDATE users SET name = 'Grace Hopper', balance_usd = 12500.00 WHERE user_id = 42;")
	if err != nil {
		t.Fatalf("UPDATE users failed: %v", err)
	}
	res, err = qr.ExecuteSQL("SELECT * FROM _shardmaster_cdc LIMIT 5;")
	if err != nil || len(res.Rows) == 0 {
		t.Fatalf("expected CDC journal entries, got err=%v rows=%d", err, len(res.Rows))
	}

	// 11. Multi-Line Indented SQL (Shift+Enter newlines + Tab indentation + Multi-Statement Batch)
	multiLineScript := `
		-- Multi-statement script with tabs and newlines
		CREATE TABLE public.departments (
			dept_id		BIGSERIAL PRIMARY KEY,
			dept_name	VARCHAR(64) NOT NULL,
			lead_user_id	BIGINT NOT NULL,
			budget_usd	NUMERIC(12,2) DEFAULT 250000.00
		);
		INSERT INTO public.departments (dept_id, dept_name, lead_user_id, budget_usd)
		VALUES
			(10, 'Distributed Storage', 42, 750000.00),
			(20, 'Query Optimizer', 777, 540000.00);
		SELECT
			d.dept_id,
			d.dept_name,
			u.name AS lead_engineer,
			CONCAT(LEFT(d.dept_name, 4), '-', SPLIT_PART(u.email, '@', 1)) AS dept_code,
			d.budget_usd::numeric(12,2) AS budget_usd
		FROM
			public.departments d
			INNER JOIN public.users u
				ON u.user_id = d.lead_user_id
		WHERE
			d.dept_name ILIKE '%storage%' OR d.budget_usd >= 500000
		ORDER BY
			d.budget_usd DESC;
	`
	res, err = qr.ExecuteSQL(multiLineScript)
	if err != nil || len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from multi-line indented SQL script, got err=%v rows=%d", err, len(res.Rows))
	}
	_, err = qr.ExecuteSQL("DROP TABLE departments;")
	if err != nil {
		t.Fatalf("DROP TABLE departments failed: %v", err)
	}
}

func TestCustomShardResizeDrainAndPinnedSQL(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(4096, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// 1. Provision a custom shard with custom name, disk GB, hardware tier, weight, and 200 target buckets
	customCfg := storage.ShardSettings{
		CustomAlias:     "eu-gdpr-vault-04",
		Region:          "eu-central-1a",
		DiskCapacityGB:  1024,
		HardwareTier:    "Enterprise-XL 64vCPU/256GB",
		Weight:          200,
		TargetBuckets:   200,
		AccessMode:      "READ_WRITE",
		ReplicationMode: "SYNC_QUORUM",
		MaxConnections:  5000,
		BufferPoolMB:    65536,
	}
	newShard, _, err := cdcEngine.ProvisionCustomShard(customCfg, 0)
	if err != nil {
		t.Fatalf("ProvisionCustomShard failed: %v", err)
	}
	if newShard.DisplayName() != "eu-gdpr-vault-04" {
		t.Fatalf("expected custom alias eu-gdpr-vault-04, got %s", newShard.DisplayName())
	}
	counts := dir.BucketCountsByShard()
	if counts[newShard.ShardID] != 200 {
		t.Fatalf("expected new custom shard to own 200 buckets, got %d", counts[newShard.ShardID])
	}

	// 2. Live resize Shard 0 to 320 buckets while running
	_, err = cdcEngine.ResizeShardBuckets(0, 320, 0)
	if err != nil {
		t.Fatalf("ResizeShardBuckets(0, 320) failed: %v", err)
	}
	counts = dir.BucketCountsByShard()
	if counts[0] != 320 {
		t.Fatalf("expected Shard 0 to own 320 buckets after live resize, got %d", counts[0])
	}

	// 3. Drain Shard 3 (evacuate 100% of its buckets to 0)
	_, err = cdcEngine.DrainShard(3, 0)
	if err != nil {
		t.Fatalf("DrainShard(3) failed: %v", err)
	}
	counts = dir.BucketCountsByShard()
	if counts[3] != 0 || ShardRowCount(cluster, 3) != 0 {
		t.Fatalf("expected drained Shard 3 to have 0 buckets and 0 rows, got buckets=%d rows=%d", counts[3], ShardRowCount(cluster, 3))
	}

	// Verify total rows across all shards still equals 4096
	var totalRows int64
	for _, s := range cluster.GetAllShards() {
		totalRows += s.RowCount()
	}
	if totalRows != 4096 {
		t.Fatalf("expected 4096 total rows preserved across live resize and drain, got %d", totalRows)
	}

	// 4. Execute Shard-Pinned SQL (ExecuteSQLOnShard and SQL hint /*+ SHARD(0) */)
	pinnedRes, err := qr.ExecuteSQLOnShard("SELECT COUNT(*), SUM(balance_usd) FROM users;", int(newShard.ShardID))
	if err != nil || len(pinnedRes.Rows) != 1 {
		t.Fatalf("ExecuteSQLOnShard failed: %v", err)
	}
	hintRes, err := qr.ExecuteSQL("/*+ SHARD(0) */ SHOW SHARDS;")
	if err != nil || len(hintRes.Rows) != 12 {
		t.Fatalf("expected 12 profile rows for /*+ SHARD(0) */ SHOW SHARDS, got err=%v rows=%d", err, len(hintRes.Rows))
	}

	// 5. Execute SQL Control Plane ALTER SHARD with exact byte sizing
	// Shrinking Shard 0 to 4096 B (2 buckets @ 2048 B/bucket) must evacuate 318 buckets via CDC without losing a single row!
	_, err = qr.ExecuteSQL("ALTER SHARD 0 SET NAME='us-west-ultra', BYTES=4096, SLAB_BYTES=8, PORT=6500, WEIGHT=300, MODE='READ_WRITE';")
	if err != nil {
		t.Fatalf("ALTER SHARD 0 failed: %v", err)
	}
	s0, _ := cluster.GetShard(0)
	if s0.DisplayName() != "us-west-ultra" || s0.GetSettings().MaxCapacityBytes != 4096 || s0.Port != 6500 {
		t.Fatalf("expected Shard 0 alias=us-west-ultra, max_bytes=4096, port=6500, got %s, %d, %d",
			s0.DisplayName(), s0.GetSettings().MaxCapacityBytes, s0.Port)
	}
	if s0.UsedMemoryBytes() > 4096 {
		t.Fatalf("expected Shard 0 real RAM usage <= 4096 bytes after bucket evacuation, got %d bytes", s0.UsedMemoryBytes())
	}

	// Verify 0 rows were truncated across the cluster when Shard 0 shrunk to 4096 bytes!
	totalRows = 0
	for _, s := range cluster.GetAllShards() {
		totalRows += s.RowCount()
	}
	if totalRows != 4096 {
		t.Fatalf("expected all 4096 rows preserved after shrinking Shard 0 to 4096 B, got %d", totalRows)
	}
}

func TestEdgeCase1_ShrinkBytesEvacuatesOrRejectsWithoutDataLoss(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(4096, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// Upsert a custom user row on Shard 0 so it has DeltaOverrides in addition to SlabBalances
	_, err := qr.ExecuteSQL("INSERT INTO users (user_id, name, email, balance_cents) VALUES (1, 'Satoshi Nakamoto', 'satoshi@gmx.com', 99999900);")
	if err != nil {
		t.Fatalf("INSERT user 1 failed: %v", err)
	}

	s0, _ := cluster.GetShard(0)
	initialUsed := s0.UsedMemoryBytes()
	if initialUsed <= 2048 {
		t.Fatalf("expected Shard 0 initial used bytes > 2048, got %d", initialUsed)
	}

	// 1A. Shrink Shard 0 BYTES to 2048 B (without specifying BUCKETS) -> must evacuate excess buckets via CDC, preserving all data
	_, err = qr.ExecuteSQL("ALTER SHARD 0 SET BYTES=2048;")
	if err != nil {
		t.Fatalf("ALTER SHARD 0 SET BYTES=2048 failed: %v", err)
	}
	if s0.UsedMemoryBytes() > 2048 {
		t.Fatalf("expected Shard 0 used bytes <= 2048 after evacuation, got %d", s0.UsedMemoryBytes())
	}

	// Verify user_id = 1 and total row count (4096) are 100% intact
	res, err := qr.ExecuteSQL("SELECT user_id, name, balance_usd FROM users WHERE user_id = 1;")
	if err != nil || len(res.Rows) != 1 || res.Rows[0][1] != "Satoshi Nakamoto" {
		t.Fatalf("expected Satoshi Nakamoto preserved after byte shrink evacuation, got err=%v rows=%v", err, res.Rows)
	}
	var totalRows int64
	for _, s := range cluster.GetAllShards() {
		totalRows += s.RowCount()
	}
	if totalRows != 4096 {
		t.Fatalf("expected 4096 rows preserved after Shard 0 byte shrink, got %d", totalRows)
	}

	// 1B. Attempt to set explicit BUCKETS=250 with BYTES=1024 on Shard 0 -> must reject with ERR_INSUFFICIENT_CLUSTER_CAPACITY
	_, err = qr.ExecuteSQL("ALTER SHARD 0 SET BYTES=1024, BUCKETS=250;")
	if err == nil {
		t.Fatalf("expected ALTER SHARD 0 SET BYTES=1024, BUCKETS=250 to be rejected")
	}

	// 1C. Cap Shards 1, 2, 3 to their current used bytes so the cluster has 0 free bytes to absorb Shard 0's remaining buckets,
	// then try to shrink Shard 0 to 16 bytes -> must reject with ERR_INSUFFICIENT_CLUSTER_CAPACITY!
	for sid := uint32(1); sid <= 3; sid++ {
		sh, _ := cluster.GetShard(sid)
		cfg := sh.GetSettings()
		cfg.MaxCapacityBytes = sh.UsedMemoryBytes()
		sh.UpdateSettings(cfg)
	}
	s0BeforeBuckets := dir.BucketCountsByShard()[0]
	_, err = qr.ExecuteSQL("ALTER SHARD 0 SET BYTES=16;")
	if err == nil {
		t.Fatalf("expected ALTER SHARD 0 SET BYTES=16 to fail when other shards have 0 free capacity")
	}
	if dir.BucketCountsByShard()[0] != s0BeforeBuckets {
		t.Fatalf("expected Shard 0 bucket count unchanged (%d) after rejected ALTER SHARD, got %d",
			s0BeforeBuckets, dir.BucketCountsByShard()[0])
	}
}

func TestEdgeCase2_WriteAt100PercentByteCapacityAutoEvacuatesOrRejects53100(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(2048, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// Find a new user_id (> 100000) that routes to Shard 0
	var uidOnS0 int64 = 100001
	for {
		sid, _ := dir.LookupInt64Fast(uidOnS0)
		if sid == 0 {
			break
		}
		uidOnS0++
	}

	// 2A. Cap Shard 0 to its exact current UsedMemoryBytes() while Shards 1..3 have 64 MB free.
	// Inserting uidOnS0 must trigger AutoEvacuateForWrite (evacuating a non-active bucket from Shard 0) and succeed!
	s0, _ := cluster.GetShard(0)
	cfg0 := s0.GetSettings()
	cfg0.MaxCapacityBytes = s0.UsedMemoryBytes()
	s0.UpdateSettings(cfg0)
	bucketsBefore := dir.BucketCountsByShard()[0]

	_, err := qr.ExecuteSQL(
		"INSERT INTO users (user_id, name, email, balance_cents) VALUES (" +
			strconv.FormatInt(uidOnS0, 10) + ", 'AutoSplit User', 'autosplit@gmail.com', 50000);")
	if err != nil {
		t.Fatalf("expected INSERT on full Shard 0 to auto-evacuate a non-active bucket and succeed, got err: %v", err)
	}
	if s0.UsedMemoryBytes() > s0.GetSettings().MaxCapacityBytes {
		t.Fatalf("Shard 0 exceeded MaxCapacityBytes: used=%d max=%d", s0.UsedMemoryBytes(), s0.GetSettings().MaxCapacityBytes)
	}
	if dir.BucketCountsByShard()[0] >= bucketsBefore {
		t.Fatalf("expected Shard 0 to have evacuated at least 1 bucket during auto-split, before=%d after=%d",
			bucketsBefore, dir.BucketCountsByShard()[0])
	}

	// 2B. Now lock down ALL shards to their exact UsedMemoryBytes() so the entire cluster is at 100% byte capacity.
	// Any new INSERT that requires additional bytes MUST be rejected with SQLSTATE 53100 (ERR_SHARD_CAPACITY_EXCEEDED)!
	for _, s := range cluster.GetAllShards() {
		c := s.GetSettings()
		c.MaxCapacityBytes = s.UsedMemoryBytes()
		s.UpdateSettings(c)
	}

	_, err = qr.ExecuteSQL("INSERT INTO users (user_id, name, email, balance_cents) VALUES (999999, 'OOM User', 'oom@gmail.com', 10000);")
	if err == nil || !strings.Contains(err.Error(), "53100") {
		t.Fatalf("expected SQLSTATE 53100 (ERR_SHARD_CAPACITY_EXCEEDED) when all shards are at 100%% byte capacity, got: %v", err)
	}

	// Verify user 999999 was NOT partially inserted into either physical shards or SQLite
	res, err := qr.ExecuteSQL("SELECT * FROM users WHERE user_id = 999999;")
	if err != nil || len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows for rejected user 999999, got err=%v rows=%d", err, len(res.Rows))
	}
	for _, s := range cluster.GetAllShards() {
		if s.UsedMemoryBytes() > s.GetSettings().MaxCapacityBytes {
			t.Fatalf("shard %d exceeded MaxCapacityBytes after rejected write: %d > %d",
				s.ShardID, s.UsedMemoryBytes(), s.GetSettings().MaxCapacityBytes)
		}
	}
}

func TestEdgeCase3_MigrateAndDrainSkipSmallShardsAndRejectInsufficientClusterCapacity(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(4096, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// Cap Shard 1 to its current used bytes (0 free bytes), while Shards 2 & 3 have 64 MB free.
	s1, _ := cluster.GetShard(1)
	cfg1 := s1.GetSettings()
	cfg1.MaxCapacityBytes = s1.UsedMemoryBytes()
	s1.UpdateSettings(cfg1)
	s1BucketsBefore := dir.BucketCountsByShard()[1]

	// Drain Shard 0 -> all 256 buckets from Shard 0 must go ONLY to Shards 2 & 3, skipping Shard 1 completely!
	_, err := qr.ExecuteSQL("DRAIN SHARD 0;")
	if err != nil {
		t.Fatalf("DRAIN SHARD 0 failed: %v", err)
	}
	counts := dir.BucketCountsByShard()
	if counts[0] != 0 {
		t.Fatalf("expected Shard 0 to have 0 buckets after drain, got %d", counts[0])
	}
	if counts[1] != s1BucketsBefore {
		t.Fatalf("expected full Shard 1 to receive 0 buckets during drain (before=%d, after=%d)", s1BucketsBefore, counts[1])
	}
	if s1.UsedMemoryBytes() > s1.GetSettings().MaxCapacityBytes {
		t.Fatalf("Shard 1 exceeded MaxCapacityBytes: %d > %d", s1.UsedMemoryBytes(), s1.GetSettings().MaxCapacityBytes)
	}

	// Now cap Shards 2 & 3 to their current used bytes as well, and try to drain Shard 1 -> must fail with ERR_INSUFFICIENT_CLUSTER_CAPACITY!
	for _, sid := range []uint32{2, 3} {
		sh, _ := cluster.GetShard(sid)
		c := sh.GetSettings()
		c.MaxCapacityBytes = sh.UsedMemoryBytes()
		sh.UpdateSettings(c)
	}
	_, err = qr.ExecuteSQL("DRAIN SHARD 1;")
	if err == nil || !strings.Contains(err.Error(), "ERR_INSUFFICIENT_CLUSTER_CAPACITY") {
		t.Fatalf("expected DRAIN SHARD 1 to fail with ERR_INSUFFICIENT_CLUSTER_CAPACITY, got: %v", err)
	}
	if s1.GetSettings().AccessMode != "READ_WRITE" {
		t.Fatalf("expected Shard 1 AccessMode rolled back to READ_WRITE after failed drain, got %s", s1.GetSettings().AccessMode)
	}
}

func TestEdgeCase4_SlabBalancesNeverTruncatedAndVDiffVerifiesUnOverriddenMutations(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(4096, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)

	// Find which shard and bucket own user_id = 500
	shardID, bucket := dir.LookupInt64Fast(500)
	shard, _ := cluster.GetShard(shardID)

	// Mutate user_id = 500 directly in the contiguous SlabBalances array (no DeltaOverrides entry!)
	if !shard.UpdateSlabBalanceInPlace(500, 777777) {
		t.Fatalf("expected UpdateSlabBalanceInPlace(500) to succeed")
	}

	digestBefore, rowsBefore := shard.ComputeBucketRangeXORHash(bucket, bucket)

	// Shrink SlabBytesPerBucket setting on `shard` to 8 bytes -> must NOT truncate populated bucket `bucket`!
	shard.ResizeMemorySlabs(shard.GetSettings().MaxCapacityBytes, 8)
	u500, found := shard.GetUser(500)
	if !found || u500.BalanceCents != 777777 {
		t.Fatalf("expected un-overridden slab balance 777777 preserved after ResizeMemorySlabs, got found=%v bal=%d", found, u500.BalanceCents)
	}

	// Migrate buckets via RebalanceToShards(5) and verify user 500's un-overridden slab balance & VDiff digest match 100%
	_, err := cdcEngine.RebalanceToShards(5, 0)
	if err != nil {
		t.Fatalf("RebalanceToShards(5) failed: %v", err)
	}
	newOwnerID, _ := dir.LookupInt64Fast(500)
	newOwner, _ := cluster.GetShard(newOwnerID)
	u500After, foundAfter := newOwner.GetUser(500)
	if !foundAfter || u500After.BalanceCents != 777777 {
		t.Fatalf("expected user 500 un-overridden slab balance 777777 after VReplication migration, got found=%v bal=%d",
			foundAfter, u500After.BalanceCents)
	}
	digestAfter, rowsAfter := newOwner.ComputeBucketRangeXORHash(bucket, bucket)
	if digestBefore != digestAfter || rowsBefore != rowsAfter {
		t.Fatalf("expected VDiff bucket digest %s (%d rows) == %s (%d rows)", digestBefore, rowsBefore, digestAfter, rowsAfter)
	}
}

func TestEdgeCase5_CustomSQLTablesByteAccountingQuotaVDiffAndCDCMigration(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(2048, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	var usedBefore int64
	for _, s := range cluster.GetAllShards() {
		usedBefore += s.UsedMemoryBytes()
	}

	// 1. Create custom table and insert 4 rows
	_, err := qr.ExecuteSQL("CREATE TABLE crypto_vaults (vault_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, asset VARCHAR(16), amount_usd NUMERIC(12,2));")
	if err != nil {
		t.Fatalf("CREATE TABLE crypto_vaults failed: %v", err)
	}
	_, err = qr.ExecuteSQL("INSERT INTO crypto_vaults (vault_id, user_id, asset, amount_usd) VALUES (101, 42, 'BTC', 95000.00), (102, 777, 'ETH', 4200.50), (103, 1024, 'SOL', 850.25), (104, 2000, 'USDC', 10000.00);")
	if err != nil {
		t.Fatalf("INSERT INTO crypto_vaults failed: %v", err)
	}

	var usedAfterInsert int64
	var customRowsTotal int
	for _, s := range cluster.GetAllShards() {
		usedAfterInsert += s.UsedMemoryBytes()
		_, rCount := s.GetCustomTableTotalBytesAndRows("crypto_vaults")
		customRowsTotal += rCount
	}
	if usedAfterInsert <= usedBefore {
		t.Fatalf("expected cluster UsedMemoryBytes to increase after custom table INSERT (%d <= %d)", usedAfterInsert, usedBefore)
	}
	if customRowsTotal != 4 {
		t.Fatalf("expected 4 custom rows stored in physical BucketSlab.CustomTables across shards, got %d", customRowsTotal)
	}

	// 2. Migrate buckets via RebalanceToShards(6) and verify all 4 custom table rows migrate with their buckets and pass VDiff!
	_, err = cdcEngine.RebalanceToShards(6, 0)
	if err != nil {
		t.Fatalf("RebalanceToShards(6) with custom table rows failed: %v", err)
	}
	customRowsAfterSplit := 0
	for _, s := range cluster.GetAllShards() {
		_, rCount := s.GetCustomTableTotalBytesAndRows("crypto_vaults")
		customRowsAfterSplit += rCount
	}
	if customRowsAfterSplit != 4 {
		t.Fatalf("expected 4 custom rows preserved across 6 shards after CDC resharding, got %d", customRowsAfterSplit)
	}

	// 3. Enforce byte quota (SQLSTATE 53100) on custom table INSERT when all shards are at 100% byte capacity
	for _, s := range cluster.GetAllShards() {
		c := s.GetSettings()
		c.MaxCapacityBytes = s.UsedMemoryBytes()
		s.UpdateSettings(c)
	}
	_, err = qr.ExecuteSQL("INSERT INTO crypto_vaults (vault_id, user_id, asset, amount_usd) VALUES (999, 555, 'BTC', 1000000.00);")
	if err == nil || !strings.Contains(err.Error(), "53100") {
		t.Fatalf("expected custom table INSERT to fail with SQLSTATE 53100 when shards are full, got: %v", err)
	}
	// Verify atomic rollback in SQLite (still 4 rows, not 5!)
	res, err := qr.ExecuteSQL("SELECT COUNT(*) FROM crypto_vaults;")
	if err != nil || len(res.Rows) != 1 || res.Rows[0][0] != "4" {
		t.Fatalf("expected SQLite crypto_vaults count to remain 4 after rejected INSERT, got err=%v rows=%v", err, res.Rows)
	}

	// 4. DROP TABLE crypto_vaults -> must free all custom table bytes across all shards
	_, err = qr.ExecuteSQL("DROP TABLE crypto_vaults;")
	if err != nil {
		t.Fatalf("DROP TABLE crypto_vaults failed: %v", err)
	}
	for _, s := range cluster.GetAllShards() {
		bBytes, rCount := s.GetCustomTableTotalBytesAndRows("crypto_vaults")
		if bBytes != 0 || rCount != 0 {
			t.Fatalf("expected 0 bytes and 0 rows for dropped table on shard %d, got %d B, %d rows", s.ShardID, bBytes, rCount)
		}
	}
}

func TestACIDPropertiesAndConcurrentReshardingZeroDeadlock(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(4096, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// 1. ATOMICITY: Explicit BEGIN -> INSERT + UPDATE + DELETE -> ROLLBACK restores 100% of physical shard & SQL state
	var totalBeforeTx int64
	var sumCentsBeforeTx int64
	for _, s := range cluster.GetAllShards() {
		r, sumC, _, _ := s.ComputeShardBalanceStats()
		totalBeforeTx += r
		sumCentsBeforeTx += sumC
	}

	if _, err := qr.ExecuteSQL("BEGIN;"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("INSERT INTO users (user_id, name, email, balance_cents) VALUES (888888, 'Temp Tx User', 'tx@gmail.com', 500000);"); err != nil {
		t.Fatalf("INSERT in tx failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("UPDATE users SET balance_cents = 9999999 WHERE user_id = 42;"); err != nil {
		t.Fatalf("UPDATE in tx failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("DELETE FROM users WHERE user_id = 100;"); err != nil {
		t.Fatalf("DELETE in tx failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("ROLLBACK;"); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	var totalAfterRollback int64
	var sumCentsAfterRollback int64
	for _, s := range cluster.GetAllShards() {
		r, sumC, _, _ := s.ComputeShardBalanceStats()
		totalAfterRollback += r
		sumCentsAfterRollback += sumC
	}
	if totalAfterRollback != totalBeforeTx || sumCentsAfterRollback != sumCentsBeforeTx {
		t.Fatalf("ROLLBACK failed to restore exact shard state: before=(%d rows, %d cents), after=(%d rows, %d cents)",
			totalBeforeTx, sumCentsBeforeTx, totalAfterRollback, sumCentsAfterRollback)
	}
	res888, _ := qr.ExecuteSQL("SELECT * FROM users WHERE user_id = 888888;")
	if len(res888.Rows) != 0 {
		t.Fatalf("expected rolled-back user 888888 to not exist, got %d rows", len(res888.Rows))
	}

	// 2. CONSISTENCY + ISOLATION + CONCURRENCY: 8 concurrent workers performing point writes & reads
	// while live CDC resharding (4 -> 6 shards -> weighted rebalance) executes in parallel with ZERO deadlocks and ZERO lost writes!
	doneCh := make(chan struct{})
	errCh := make(chan error, 32)
	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			uid := int64(50000 + workerID)
			for iter := 0; iter < 15; iter++ {
				select {
				case <-doneCh:
					return
				default:
				}
				sqlUpsert := "INSERT INTO users (user_id, name, email, balance_cents) VALUES (" +
					strconv.FormatInt(uid, 10) + ", 'Concurrent_" + strconv.Itoa(workerID) + "', 'c@gmail.com', 100000);"
				if _, err := qr.ExecuteSQL(sqlUpsert); err != nil {
					errCh <- err
					return
				}
				if _, err := qr.ExecuteSQL("SELECT * FROM users WHERE user_id = " + strconv.FormatInt(uid, 10) + ";"); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	// Run live CDC resharding 4 -> 6 shards and weighted rebalance concurrently with the writers
	if _, err := cdcEngine.RebalanceToShards(6, 0); err != nil {
		t.Fatalf("concurrent RebalanceToShards(6) failed: %v", err)
	}
	if _, err := cdcEngine.RebalanceByWeights(0); err != nil {
		t.Fatalf("concurrent RebalanceByWeights failed: %v", err)
	}
	close(doneCh)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("concurrent worker error during live resharding: %v", e)
	}

	// 3. DURABILITY: Save cluster state to disk and reload into a fresh ClusterStorage + ShardDirectory
	cluster.SaveStateFile(dir.SnapshotBuckets())
	reloadedCluster := storage.NewClusterStorage(4, cluster.DataDir())
	reloadedDir := directory.NewShardDirectory(4)
	reloadedCluster.SeedCluster(2000, reloadedDir.GetBucketOwner)
	if !reloadedCluster.LoadStateFile(func(b uint16, sid uint32) {
		reloadedDir.AtomicCutoverBucket(b, sid)
	}) {
		t.Fatalf("expected LoadStateFile to succeed")
	}
	if len(reloadedCluster.GetAllShards()) != 6 {
		t.Fatalf("expected 6 durably persisted shards after reload, got %d", len(reloadedCluster.GetAllShards()))
	}
}

func TestRealSeededRowsAndFullDiskPersistence(t *testing.T) {
	dataDir := t.TempDir()
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, dataDir)
	cluster.SeedCluster(2000, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	// 1. Verify every seeded user_id 1..2000 exists on its exact owning shard, and non-seeded IDs (e.g. 2001, 49999999) do NOT exist
	for uid := int64(1); uid <= 2000; uid++ {
		sid, _ := dir.LookupInt64Fast(uid)
		sh, ok := cluster.GetShard(sid)
		if !ok {
			t.Fatalf("missing shard %d", sid)
		}
		if _, found := sh.GetUser(uid); !found {
			t.Fatalf("expected seeded user_id %d to exist on shard %d", uid, sid)
		}
	}
	for _, nonExistentID := range []int64{2001, 99999, 49999999} {
		sid, _ := dir.LookupInt64Fast(nonExistentID)
		sh, _ := cluster.GetShard(sid)
		if _, found := sh.GetUser(nonExistentID); found {
			t.Fatalf("expected non-seeded user_id %d to NOT exist before INSERT, but found=true", nonExistentID)
		}
	}

	// 2. Execute real SQL INSERT, UPDATE, and DELETE, and rebalance 4 -> 6 shards
	if _, err := qr.ExecuteSQL("INSERT INTO users (user_id, name, email, balance_cents) VALUES (777777, 'NVMe Persisted', 'nvme@gmail.com', 888800);"); err != nil {
		t.Fatalf("INSERT 777777 failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("UPDATE users SET balance_usd = 9999.00 WHERE user_id = 42;"); err != nil {
		t.Fatalf("UPDATE 42 failed: %v", err)
	}
	if _, err := qr.ExecuteSQL("DELETE FROM users WHERE user_id = 10;"); err != nil {
		t.Fatalf("DELETE 10 failed: %v", err)
	}
	if _, err := cdcEngine.RebalanceToShards(6, 0); err != nil {
		t.Fatalf("RebalanceToShards(6) failed: %v", err)
	}

	// 3. Simulate process restart: create a brand-new ShardDirectory + ClusterStorage from dataDir
	dir2 := directory.NewShardDirectory(4)
	cluster2 := storage.NewClusterStorage(4, dataDir)
	cluster2.SeedCluster(2000, dir2.GetBucketOwner)
	if !cluster2.LoadStateFile(dir2.AtomicCutoverBucket) {
		t.Fatalf("expected LoadStateFile to restore cluster_state.json")
	}

	// Verify 777777 was persisted and restored on its post-split owning shard
	sid777, _ := dir2.LookupFast("777777")
	sh777, _ := cluster2.GetShard(sid777)
	u777, found777 := sh777.GetUser(777777)
	if !found777 || u777.BalanceCents != 888800 || u777.Name != "NVMe Persisted" {
		t.Fatalf("expected restored user 777777 with 888800 cents, got found=%v row=%+v", found777, u777)
	}

	// Verify user 42 update was persisted and restored
	sid42, _ := dir2.LookupFast("42")
	sh42, _ := cluster2.GetShard(sid42)
	u42, found42 := sh42.GetUser(42)
	if !found42 || u42.BalanceCents != 999900 {
		t.Fatalf("expected restored user 42 with 999900 cents, got found=%v cents=%d", found42, u42.BalanceCents)
	}

	// Verify user 10 deletion tombstone was persisted and restored
	sid10, _ := dir2.LookupFast("10")
	sh10, _ := cluster2.GetShard(sid10)
	if _, found10 := sh10.GetUser(10); found10 {
		t.Fatalf("expected deleted user 10 to remain deleted after restart, got found=true")
	}
}

func TestTUIDashboardLabelsAndLocalZones(t *testing.T) {
	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, t.TempDir())
	cluster.SeedCluster(10000, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	_, err := cdcEngine.RebalanceToShards(8, 0)
	if err != nil {
		t.Fatalf("RebalanceToShards(8) failed: %v", err)
	}

	model := tui.NewDashboardModel(qr)
	viewOut := model.View()

	for _, forbidden := range []string{"50M Rows", "0.0M", "eu-central", "ap-south"} {
		if strings.Contains(viewOut, forbidden) {
			t.Fatalf("expected TUI View() to NOT contain fake string %q, got:\n%s", forbidden, viewOut)
		}
	}
	for _, required := range []string{"10,000", "SHARD NAME", "LOCAL ZONE", "BUCKETS", "RAM USED / MAX", "ROWS", "QPS", "shard-0", "local-node-0", "128/1024", "SELECTED:"} {
		if !strings.Contains(viewOut, required) {
			t.Fatalf("expected TUI View() to contain %q, got:\n%s", required, viewOut)
		}
	}
}

func ShardRowCount(cs *storage.ClusterStorage, id uint32) int64 {
	if s, ok := cs.GetShard(id); ok {
		return s.RowCount()
	}
	return -1
}

func BenchmarkZeroAllocRouting(b *testing.B) {
	dir := directory.NewShardDirectory(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var id int64 = 1
		for pb.Next() {
			_, _ = dir.LookupInt64Fast(id)
			id++
		}
	})
}
