package hash_test

import (
	"net"
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

	// 5. Execute SQL Control Plane ALTER SHARD
	_, err = qr.ExecuteSQL("ALTER SHARD 0 SET NAME='us-west-ultra', SIZE=2048, WEIGHT=300, MODE='READ_WRITE';")
	if err != nil {
		t.Fatalf("ALTER SHARD 0 failed: %v", err)
	}
	s0, _ := cluster.GetShard(0)
	if s0.DisplayName() != "us-west-ultra" || s0.GetSettings().DiskCapacityGB != 2048 {
		t.Fatalf("expected Shard 0 alias=us-west-ultra and size=2048, got %s and %d", s0.DisplayName(), s0.GetSettings().DiskCapacityGB)
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


