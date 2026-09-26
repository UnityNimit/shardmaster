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

	// 1. SHOW TABLES
	res, err := qr.ExecuteSQL("SHOW TABLES;")
	if err != nil || len(res.Rows) < 5 {
		t.Fatalf("expected >= 5 tables from SHOW TABLES, got err=%v rows=%d", err, len(res.Rows))
	}

	// 2. DESCRIBE users
	res, err = qr.ExecuteSQL("DESCRIBE users;")
	if err != nil || len(res.Rows) != 11 {
		t.Fatalf("expected 11 columns from DESCRIBE users, got err=%v rows=%d", err, len(res.Rows))
	}

	// 3. CREATE TABLE + DESCRIBE + DROP TABLE
	_, err = qr.ExecuteSQL("CREATE TABLE orders (order_id BIGINT PRIMARY KEY, user_id BIGINT, amount_cents BIGINT);")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}
	res, err = qr.ExecuteSQL("DESCRIBE orders;")
	if err != nil || len(res.Rows) != 3 {
		t.Fatalf("expected 3 columns from DESCRIBE orders, got err=%v rows=%d", err, len(res.Rows))
	}
	_, err = qr.ExecuteSQL("DROP TABLE orders;")
	if err != nil {
		t.Fatalf("DROP TABLE orders failed: %v", err)
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

	// 6. UPDATE + CDC Journal Query
	_, err = qr.ExecuteSQL("UPDATE users SET name = 'Grace Hopper', balance_usd = 12500.00 WHERE user_id = 42;")
	if err != nil {
		t.Fatalf("UPDATE users failed: %v", err)
	}
	res, err = qr.ExecuteSQL("SELECT * FROM _shardmaster_cdc LIMIT 5;")
	if err != nil || len(res.Rows) == 0 {
		t.Fatalf("expected CDC journal entries, got err=%v rows=%d", err, len(res.Rows))
	}
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

