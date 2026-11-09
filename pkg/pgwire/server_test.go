package pgwire_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"
	"shardmaster/pkg/cdc"
	"shardmaster/pkg/directory"
	"shardmaster/pkg/hotspot"
	"shardmaster/pkg/pgwire"
	"shardmaster/pkg/router"
	"shardmaster/pkg/storage"
)

func setupTestPGWireServer(t *testing.T, port int) (*pgwire.Server, *router.QueryRouter, string) {
	tempDir, err := os.MkdirTemp("", "shardmaster_pgwire_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dir := directory.NewShardDirectory(4)
	cluster := storage.NewClusterStorage(4, filepath.Join(tempDir, "data"))
	cluster.SeedCluster(100, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := pgwire.NewServer(addr, "", qr)

	go func() {
		_ = srv.Start()
	}()

	// Wait for server to bind port
	for i := 0; i < 20; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	return srv, qr, tempDir
}

func performHandshake(t *testing.T, conn net.Conn) *pgproto3.Frontend {
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	// Send startup message
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "shardmaster_admin",
			"database": "shardmaster",
		},
	}
	if err := frontend.Send(startupMsg); err != nil {
		t.Fatalf("failed to send startup message: %v", err)
	}

	// Consume until ReadyForQuery
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("handshake error: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	return frontend
}

func TestPGWireSimpleQueryAndExtendedProtocol(t *testing.T) {
	srv, _, tempDir := setupTestPGWireServer(t, 16432)
	defer srv.Close()
	defer os.RemoveAll(tempDir)

	conn, err := net.Dial("tcp", "127.0.0.1:16432")
	if err != nil {
		t.Fatalf("failed to connect to PGWire: %v", err)
	}
	defer conn.Close()

	frontend := performHandshake(t, conn)

	// 1. Test Simple Query: SET client_encoding = 'UTF8'
	// Should return CommandComplete followed by ReadyForQuery WITHOUT RowDescription
	if err := frontend.Send(&pgproto3.Query{String: "SET client_encoding = 'UTF8';"}); err != nil {
		t.Fatalf("failed to send SET query: %v", err)
	}
	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("failed to receive response for SET: %v", err)
	}
	cc, ok := msg.(*pgproto3.CommandComplete)
	if !ok {
		t.Fatalf("expected CommandComplete for SET, got %T", msg)
	}
	if string(cc.CommandTag) != "SET" {
		t.Errorf("expected CommandTag 'SET', got '%s'", string(cc.CommandTag))
	}
	rfq, err := frontend.Receive()
	if err != nil || rfq == nil {
		t.Fatalf("expected ReadyForQuery after CommandComplete: %v", err)
	}

	// 2. Test Extended Query Protocol (Parse, Describe, Bind, Execute, Sync)
	// Parse prepared statement with parameter $1
	if err := frontend.Send(&pgproto3.Parse{
		Name:          "user_point_query",
		Query:         "SELECT * FROM users WHERE user_id = $1;",
		ParameterOIDs: []uint32{20},
	}); err != nil {
		t.Fatalf("failed to send Parse: %v", err)
	}

	// Describe statement
	if err := frontend.Send(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       "user_point_query",
	}); err != nil {
		t.Fatalf("failed to send Describe: %v", err)
	}

	// Bind parameter $1 = 42
	if err := frontend.Send(&pgproto3.Bind{
		DestinationPortal: "user_portal",
		PreparedStatement: "user_point_query",
		Parameters:        [][]byte{[]byte("42")},
	}); err != nil {
		t.Fatalf("failed to send Bind: %v", err)
	}

	// Execute portal
	if err := frontend.Send(&pgproto3.Execute{
		Portal:  "user_portal",
		MaxRows: 0,
	}); err != nil {
		t.Fatalf("failed to send Execute: %v", err)
	}

	// Sync
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("failed to send Sync: %v", err)
	}

	// Consume and verify responses
	parseResp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive ParseComplete failed: %v", err)
	}
	if _, ok := parseResp.(*pgproto3.ParseComplete); !ok {
		t.Fatalf("expected ParseComplete, got %T", parseResp)
	}

	paramResp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive ParameterDescription failed: %v", err)
	}
	if _, ok := paramResp.(*pgproto3.ParameterDescription); !ok {
		t.Fatalf("expected ParameterDescription, got %T", paramResp)
	}

	rowDescResp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive RowDescription failed: %v", err)
	}
	if _, ok := rowDescResp.(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected RowDescription, got %T", rowDescResp)
	}

	bindResp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive BindComplete failed: %v", err)
	}
	if _, ok := bindResp.(*pgproto3.BindComplete); !ok {
		t.Fatalf("expected BindComplete, got %T", bindResp)
	}

	// Read DataRows and CommandComplete
	var receivedRows int
	for {
		m, err := frontend.Receive()
		if err != nil {
			t.Fatalf("error receiving execute response: %v", err)
		}
		if _, ok := m.(*pgproto3.DataRow); ok {
			receivedRows++
		}
		if _, ok := m.(*pgproto3.CommandComplete); ok {
			break
		}
	}
	if receivedRows != 1 {
		t.Errorf("expected 1 row for user_id = 42, got %d", receivedRows)
	}

	// Verify ReadyForQuery from Sync
	syncResp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("error receiving ReadyForQuery from Sync: %v", err)
	}
	if _, ok := syncResp.(*pgproto3.ReadyForQuery); !ok {
		t.Fatalf("expected ReadyForQuery, got %T", syncResp)
	}
}

func TestPGWireStreamingCopyFromStdin(t *testing.T) {
	srv, qr, tempDir := setupTestPGWireServer(t, 16433)
	defer srv.Close()
	defer os.RemoveAll(tempDir)

	conn, err := net.Dial("tcp", "127.0.0.1:16433")
	if err != nil {
		t.Fatalf("failed to connect to PGWire: %v", err)
	}
	defer conn.Close()

	frontend := performHandshake(t, conn)

	// Send COPY products FROM STDIN (used by psql \copy and DBeaver CSV Import Wizard)
	copySQL := "COPY products FROM STDIN WITH (FORMAT CSV, HEADER);"
	if err := frontend.Send(&pgproto3.Query{String: copySQL}); err != nil {
		t.Fatalf("failed to send COPY FROM STDIN: %v", err)
	}

	// Server should reply with CopyInResponse
	resp, err := frontend.Receive()
	if err != nil {
		t.Fatalf("error receiving CopyInResponse: %v", err)
	}
	if _, ok := resp.(*pgproto3.CopyInResponse); !ok {
		t.Fatalf("expected CopyInResponse, got %T", resp)
	}

	// Client streams CopyData chunks
	csvData := `product_id,product_name,category,price_usd,stock_qty
501,High-Performance SSD,Storage,149.99,120
502,DDR5 ECC RAM 32GB,Memory,199.50,85
503,PCIe 5.0 NIC 100G,Networking,499.00,40
`
	if err := frontend.Send(&pgproto3.CopyData{Data: []byte(csvData)}); err != nil {
		t.Fatalf("failed to send CopyData: %v", err)
	}

	// Client sends CopyDone
	if err := frontend.Send(&pgproto3.CopyDone{}); err != nil {
		t.Fatalf("failed to send CopyDone: %v", err)
	}

	// Server should reply with CommandComplete("COPY 3") and ReadyForQuery
	ccMsg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("failed to receive CommandComplete for COPY: %v", err)
	}
	cc, ok := ccMsg.(*pgproto3.CommandComplete)
	if !ok {
		t.Fatalf("expected CommandComplete, got %T", ccMsg)
	}
	if string(cc.CommandTag) != "COPY 3" {
		t.Errorf("expected 'COPY 3', got '%s'", string(cc.CommandTag))
	}

	rfqMsg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("failed to receive ReadyForQuery: %v", err)
	}
	if _, ok := rfqMsg.(*pgproto3.ReadyForQuery); !ok {
		t.Fatalf("expected ReadyForQuery, got %T", rfqMsg)
	}

	// Verify imported table is immediately queryable
	qRes, err := qr.ExecuteSQL("SELECT COUNT(*) FROM products;")
	if err != nil {
		t.Fatalf("SELECT COUNT(*) FROM products failed: %v", err)
	}
	if len(qRes.Rows) == 0 || qRes.Rows[0][0] != "3" {
		t.Errorf("expected 3 rows in products table, got %v", qRes.Rows)
	}
}
