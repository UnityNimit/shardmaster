package router_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"shardmaster/pkg/cdc"
	"shardmaster/pkg/directory"
	"shardmaster/pkg/hotspot"
	"shardmaster/pkg/router"
	"shardmaster/pkg/storage"
)

func setupTestRouter(t *testing.T, initialShards uint32) (*router.QueryRouter, string) {
	tempDir, err := os.MkdirTemp("", "shardmaster_csv_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dir := directory.NewShardDirectory(initialShards)
	cluster := storage.NewClusterStorage(initialShards, filepath.Join(tempDir, "data"))
	cluster.SeedCluster(100, dir.GetBucketOwner)
	cdcEngine := cdc.NewEngine(dir, cluster)
	tracker := hotspot.NewTracker(dir, cluster, cdcEngine)
	qr := router.NewQueryRouter(dir, cluster, cdcEngine, tracker)

	return qr, tempDir
}

func TestCSVImportAndExportEngine(t *testing.T) {
	qr, tempDir := setupTestRouter(t, 4)
	defer os.RemoveAll(tempDir)

	// Create a sample CSV file
	csvContent := `customer_id,full_name,email,tier,spend_usd,country
1001,Alice Smith,alice@example.com,Enterprise,12500.50,US
1002,Bob Jones,bob@example.com,Pro,450.00,CA
1003,Charlie Brown,charlie@example.com,Free,0.00,UK
1004,Diana Prince,diana@example.com,Enterprise,9800.75,US
1005,Evan Wright,evan@example.com,Pro,620.25,DE
`
	csvPath := filepath.Join(tempDir, "customers.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	// 1. Test ImportCSVFile with auto-detected delimiter and types
	opts := router.CSVOptions{
		HasHeader: true,
	}
	res, err := qr.ImportCSVFile(csvPath, "customers", "", opts)
	if err != nil {
		t.Fatalf("ImportCSVFile failed: %v", err)
	}

	if res.RowsImported != 5 {
		t.Errorf("expected 5 rows imported, got %d", res.RowsImported)
	}
	if res.TableName != "customers" {
		t.Errorf("expected table name 'customers', got '%s'", res.TableName)
	}
	if res.ShardKey != "customer_id" {
		t.Errorf("expected shard key 'customer_id', got '%s'", res.ShardKey)
	}
	if len(res.Columns) != 6 {
		t.Errorf("expected 6 columns, got %d", len(res.Columns))
	}

	// 2. Query the imported table via Full SQL Engine
	sqlRes, err := qr.ExecuteSQL("SELECT * FROM customers ORDER BY customer_id ASC;")
	if err != nil {
		t.Fatalf("SELECT from imported table failed: %v", err)
	}
	if len(sqlRes.Rows) != 5 {
		t.Errorf("expected 5 rows in query result, got %d", len(sqlRes.Rows))
	}

	// 3. Test GROUP BY aggregation on imported CSV data
	aggRes, err := qr.ExecuteSQL("SELECT tier, COUNT(*) AS count, SUM(spend_usd) AS total_spend FROM customers GROUP BY tier ORDER BY count DESC;")
	if err != nil {
		t.Fatalf("aggregation on imported table failed: %v", err)
	}
	if len(aggRes.Rows) != 3 {
		t.Errorf("expected 3 tier groups, got %d", len(aggRes.Rows))
	}

	// 4. Test SQL COPY ... TO export
	exportPath := filepath.Join(tempDir, "exported_customers.csv")
	copySQL := "COPY customers TO '" + strings.ReplaceAll(exportPath, "\\", "/") + "' WITH (FORMAT CSV, HEADER);"
	copyRes, err := qr.ExecuteSQL(copySQL)
	if err != nil {
		t.Fatalf("COPY TO query failed: %v", err)
	}
	if !strings.HasPrefix(copyRes.CommandTag, "COPY 5") {
		t.Errorf("expected CommandTag 'COPY 5', got '%s'", copyRes.CommandTag)
	}

	// Verify exported content exists on disk and has 6 lines (1 header + 5 rows)
	exportedBytes, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("failed to read exported CSV file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(exportedBytes)), "\n")
	if len(lines) != 6 {
		t.Errorf("expected 6 lines in exported file, got %d", len(lines))
	}

	// 5. Test IMPORT CSV SQL syntax
	importSQLPath := filepath.Join(tempDir, "orders_import.csv")
	ordersCSV := `id|order_ref|amount|status
201|ORD-A|99.50|COMPLETED
202|ORD-B|199.00|PENDING
203|ORD-C|49.00|COMPLETED
`
	if err := os.WriteFile(importSQLPath, []byte(ordersCSV), 0644); err != nil {
		t.Fatalf("failed to write pipe CSV: %v", err)
	}
	importSQL := "IMPORT CSV '" + strings.ReplaceAll(importSQLPath, "\\", "/") + "' INTO imported_orders DELIMITER '|';"
	importRes, err := qr.ExecuteSQL(importSQL)
	if err != nil {
		t.Fatalf("IMPORT CSV statement failed: %v", err)
	}
	if !strings.HasPrefix(importRes.CommandTag, "COPY 3") {
		t.Errorf("expected CommandTag 'COPY 3', got '%s'", importRes.CommandTag)
	}
}

func TestExternalToolCompatibilityAndSystemCatalog(t *testing.T) {
	qr, tempDir := setupTestRouter(t, 4)
	defer os.RemoveAll(tempDir)

	testCases := []struct {
		name        string
		sql         string
		expectedTag string
		minRows     int
	}{
		{
			name:        "PostgreSQL Version Function",
			sql:         "SELECT version();",
			expectedTag: "SELECT 1",
			minRows:     1,
		},
		{
			name:        "Current Database and Schema",
			sql:         "SELECT current_database(), current_schema(), current_user;",
			expectedTag: "SELECT 1",
			minRows:     1,
		},
		{
			name:        "DBeaver SET Client Encoding",
			sql:         "SET client_encoding = 'UTF8';",
			expectedTag: "SET",
			minRows:     0,
		},
		{
			name:        "DBeaver SET Extra Float Digits",
			sql:         "SET extra_float_digits = 3;",
			expectedTag: "SET",
			minRows:     0,
		},
		{
			name:        "SHOW Search Path",
			sql:         "SHOW search_path;",
			expectedTag: "SHOW",
			minRows:     1,
		},
		{
			name:        "SHOW Transaction Isolation Level",
			sql:         "SHOW transaction isolation level;",
			expectedTag: "SHOW",
			minRows:     1,
		},
		{
			name:        "SHOW ALL Session Variables",
			sql:         "SHOW ALL;",
			expectedTag: "SHOW",
			minRows:     5,
		},
		{
			name:        "DISCARD ALL",
			sql:         "DISCARD ALL;",
			expectedTag: "DISCARD",
			minRows:     0,
		},
		{
			name:        "RESET ALL",
			sql:         "RESET ALL;",
			expectedTag: "RESET",
			minRows:     0,
		},
		{
			name:        "DBeaver pg_catalog.pg_tables query",
			sql:         "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public';",
			expectedTag: "SELECT",
			minRows:     1,
		},
		{
			name:        "DBeaver pg_catalog.pg_database query",
			sql:         "SELECT datname FROM pg_catalog.pg_database;",
			expectedTag: "SELECT",
			minRows:     1,
		},
		{
			name:        "DataGrip information_schema.tables query",
			sql:         "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
			expectedTag: "SELECT",
			minRows:     1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := qr.ExecuteSQL(tc.sql)
			if err != nil {
				t.Fatalf("query '%s' failed: %v", tc.sql, err)
			}
			if !strings.HasPrefix(res.CommandTag, tc.expectedTag) {
				t.Errorf("expected tag starting with '%s', got '%s'", tc.expectedTag, res.CommandTag)
			}
			if len(res.Rows) < tc.minRows {
				t.Errorf("expected at least %d rows, got %d", tc.minRows, len(res.Rows))
			}
		})
	}
}
