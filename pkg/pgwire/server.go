package pgwire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"shardmaster/pkg/router"
)

// Server implements Pillar 1: Native PostgreSQL v3.0 Wire Protocol Frontend (Port 6000)
// plus the PDF-specification HTTP compatibility endpoint (/shard?user_id=123).
type Server struct {
	ListenAddr string
	HTTPAddr   string
	Router     *router.QueryRouter
	listener   net.Listener
}

func NewServer(listenAddr, httpAddr string, qr *router.QueryRouter) *Server {
	if listenAddr == "" {
		listenAddr = ":6000"
	}
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	return &Server{
		ListenAddr: listenAddr,
		HTTPAddr:   httpAddr,
		Router:     qr,
	}
}

// Start launches the TCP PGWire server on :6000 and the HTTP Admin/Directory API on :8080.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind PGWire TCP port %s: %w", s.ListenAddr, err)
	}
	s.listener = ln

	if s.HTTPAddr != "" {
		go s.startHTTPBridge()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				return nil
			}
			continue
		}
		go s.handlePGClient(conn)
	}
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

type preparedStmt struct {
	Name      string
	Query     string
	ParamOIDs []uint32
}

type boundPortal struct {
	Name     string
	BoundSQL string
}

func (s *Server) handlePGClient(conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// Step 1: PostgreSQL v3.0 Startup & SSL Negotiation
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	if _, isSSL := startupMsg.(*pgproto3.SSLRequest); isSSL {
		// Decline SSL cleanly with 'N' so psql/DBeaver proceeds with StartupMessage
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return
		}
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}

	switch startupMsg.(type) {
	case *pgproto3.StartupMessage:
		// Send AuthenticationOk, ParameterStatus headers, BackendKeyData, ReadyForQuery
		_ = backend.Send(&pgproto3.AuthenticationOk{})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0-ShardMaster-PGWire"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
		_ = backend.Send(&pgproto3.BackendKeyData{ProcessID: 6000, SecretKey: 424242})
		if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
			return
		}
	default:
		return
	}

	stmts := make(map[string]preparedStmt)
	portals := make(map[string]boundPortal)

	// Step 2: Command Loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			s.handleSimpleQuery(backend, m.String)

		case *pgproto3.Parse:
			stmts[m.Name] = preparedStmt{
				Name:      m.Name,
				Query:     m.Query,
				ParamOIDs: m.ParameterOIDs,
			}
			_ = backend.Send(&pgproto3.ParseComplete{})

		case *pgproto3.Describe:
			s.handleDescribe(backend, m, stmts, portals)

		case *pgproto3.Bind:
			s.handleBind(backend, m, stmts, portals)

		case *pgproto3.Execute:
			s.handleExecute(backend, m, portals)

		case *pgproto3.Sync:
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

		case *pgproto3.Close:
			if m.ObjectType == 'S' {
				delete(stmts, m.Name)
			} else if m.ObjectType == 'P' {
				delete(portals, m.Name)
			}
			_ = backend.Send(&pgproto3.CloseComplete{})

		case *pgproto3.Flush:
			// Flushed automatically by chunk writer

		case *pgproto3.Terminate:
			return

		default:
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

func (s *Server) handleSimpleQuery(backend *pgproto3.Backend, sql string) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		_ = backend.Send(&pgproto3.EmptyQueryResponse{})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	cq := router.ClassifySQL(trimmed)

	// Handle PostgreSQL COPY ... FROM STDIN streaming data protocol
	if cq.Kind == router.QueryCopy && cq.IsFrom && cq.IsStdin {
		s.handleCopyFromStdin(backend, cq)
		return
	}

	// Handle PostgreSQL COPY ... TO STDOUT streaming data protocol
	if cq.Kind == router.QueryCopy && !cq.IsFrom && cq.IsStdout {
		s.handleCopyToStdout(backend, cq)
		return
	}

	res, err := s.Router.ExecuteSQL(trimmed)
	if err != nil {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	// Only send RowDescription and DataRows if the query returns columns (e.g. SELECT, SHOW, EXPLAIN)
	if len(res.Columns) > 0 {
		fields := make([]pgproto3.FieldDescription, len(res.Columns))
		for i, colName := range res.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(colName),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25, // TEXT OID
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}
		_ = backend.Send(&pgproto3.RowDescription{Fields: fields})

		for _, row := range res.Rows {
			vals := make([][]byte, len(row))
			for i, cell := range row {
				vals[i] = []byte(cell)
			}
			_ = backend.Send(&pgproto3.DataRow{Values: vals})
		}
	}

	cmdTag := res.CommandTag
	if cmdTag == "" {
		cmdTag = "OK"
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(cmdTag)})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *Server) handleCopyFromStdin(backend *pgproto3.Backend, cq router.ClassifiedQuery) {
	// Send CopyInResponse to tell client to begin streaming CopyData chunks
	_ = backend.Send(&pgproto3.CopyInResponse{
		OverallFormat:     0,
		ColumnFormatCodes: make([]uint16, 0),
	})

	var copyBuf bytes.Buffer
	copyAborted := false

	for {
		cMsg, err := backend.Receive()
		if err != nil {
			return
		}
		switch cm := cMsg.(type) {
		case *pgproto3.CopyData:
			copyBuf.Write(cm.Data)
		case *pgproto3.CopyDone:
			goto processCopy
		case *pgproto3.CopyFail:
			copyAborted = true
			goto processCopy
		case *pgproto3.Sync:
			// Client sync
		}
	}

processCopy:
	if copyAborted {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "57014",
			Message:  "COPY from STDIN was aborted by the client",
		})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	opts := router.CSVOptions{
		Delimiter:   cq.CSVDelimiter,
		HasHeader:   cq.CSVHasHeader,
		TargetTable: cq.TableName,
		ShardKey:    cq.ShardKeyCol,
	}

	res, err := s.Router.ImportCSV(&copyBuf, cq.TableName, opts)
	if err != nil {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", res.RowsImported))})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *Server) handleCopyToStdout(backend *pgproto3.Backend, cq router.ClassifiedQuery) {
	_ = backend.Send(&pgproto3.CopyOutResponse{
		OverallFormat:     0,
		ColumnFormatCodes: make([]uint16, 0),
	})

	var exportBuf bytes.Buffer
	opts := router.CSVOptions{
		Delimiter:   cq.CSVDelimiter,
		HasHeader:   cq.CSVHasHeader,
		TargetTable: cq.TableName,
	}

	res, err := s.Router.ExportCSV(&exportBuf, cq.TableName, opts)
	if err != nil {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	_ = backend.Send(&pgproto3.CopyData{Data: exportBuf.Bytes()})
	_ = backend.Send(&pgproto3.CopyDone{})
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", res.RowsExported))})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *Server) handleDescribe(backend *pgproto3.Backend, m *pgproto3.Describe, stmts map[string]preparedStmt, portals map[string]boundPortal) {
	var targetSQL string
	if m.ObjectType == 'S' {
		st, exists := stmts[m.Name]
		if !exists {
			_ = backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "26000", Message: fmt.Sprintf("prepared statement '%s' does not exist", m.Name)})
			return
		}
		_ = backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: st.ParamOIDs})
		targetSQL = st.Query
	} else if m.ObjectType == 'P' {
		po, exists := portals[m.Name]
		if !exists {
			_ = backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "34000", Message: fmt.Sprintf("portal '%s' does not exist", m.Name)})
			return
		}
		targetSQL = po.BoundSQL
	}

	clean := strings.TrimSpace(targetSQL)
	upper := strings.ToUpper(clean)

	// If query is non-returning DDL/DML/session control, send NoData
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") ||
		strings.HasPrefix(upper, "CREATE ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "INSERT ") ||
		strings.HasPrefix(upper, "UPDATE ") ||
		strings.HasPrefix(upper, "DELETE ") {
		_ = backend.Send(&pgproto3.NoData{})
		return
	}

	res, err := s.Router.ExecuteSQL(clean)
	if err != nil || len(res.Columns) == 0 {
		_ = backend.Send(&pgproto3.NoData{})
		return
	}

	fields := make([]pgproto3.FieldDescription, len(res.Columns))
	for i, colName := range res.Columns {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(colName),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25, // TEXT OID
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
	}
	_ = backend.Send(&pgproto3.RowDescription{Fields: fields})
}

func (s *Server) handleBind(backend *pgproto3.Backend, m *pgproto3.Bind, stmts map[string]preparedStmt, portals map[string]boundPortal) {
	st, exists := stmts[m.PreparedStatement]
	if !exists {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "26000",
			Message:  fmt.Sprintf("prepared statement '%s' does not exist", m.PreparedStatement),
		})
		return
	}

	boundSQL := substituteParameters(st.Query, m.Parameters)
	portals[m.DestinationPortal] = boundPortal{
		Name:     m.DestinationPortal,
		BoundSQL: boundSQL,
	}
	_ = backend.Send(&pgproto3.BindComplete{})
}

func (s *Server) handleExecute(backend *pgproto3.Backend, m *pgproto3.Execute, portals map[string]boundPortal) {
	po, exists := portals[m.Portal]
	if !exists {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "34000",
			Message:  fmt.Sprintf("portal '%s' does not exist", m.Portal),
		})
		return
	}

	res, err := s.Router.ExecuteSQL(po.BoundSQL)
	if err != nil {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		})
		return
	}

	if len(res.Columns) > 0 {
		for _, row := range res.Rows {
			vals := make([][]byte, len(row))
			for i, cell := range row {
				vals[i] = []byte(cell)
			}
			_ = backend.Send(&pgproto3.DataRow{Values: vals})
		}
	}

	cmdTag := res.CommandTag
	if cmdTag == "" {
		cmdTag = "OK"
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(cmdTag)})
}

var paramRegex = regexp.MustCompile(`\$([0-9]+)`)

func substituteParameters(sql string, params [][]byte) string {
	if len(params) == 0 {
		return sql
	}
	return paramRegex.ReplaceAllStringFunc(sql, func(m string) string {
		idxStr := m[1:]
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 1 || idx > len(params) {
			return m
		}
		raw := params[idx-1]
		if raw == nil {
			return "NULL"
		}
		val := string(raw)
		if _, err := strconv.ParseFloat(val, 64); err == nil && !strings.Contains(val, " ") {
			return val
		}
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'"
	})
}

func (s *Server) startHTTPBridge() {
	mux := http.NewServeMux()

	// Exact V1 PDF Spec: GET /shard?user_id=123 -> returns "shard_2"
	mux.HandleFunc("/shard", func(w http.ResponseWriter, r *http.Request) {
		userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
		if userID == "" {
			userID = "123"
		}
		info := s.Router.Dir.LookupDetailed(userID)
		w.Header().Set("X-Virtual-Bucket", fmt.Sprintf("%d", info.VirtualBucket))
		w.Header().Set("X-Lookup-Time-Ns", fmt.Sprintf("%d", info.LookupTimeNs))
		_, _ = w.Write([]byte(info.ShardName))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		shards := s.Router.Cluster.GetAllShards()
		buckets := s.Router.Dir.BucketCountsByShard()
		type shardDTO struct {
			ID      uint32  `json:"shard_id"`
			Port    int     `json:"port"`
			Region  string  `json:"region"`
			Buckets int     `json:"buckets"`
			Rows    int64   `json:"rows"`
			QPS     uint64  `json:"qps"`
			LatMs   float64 `json:"latency_ms"`
		}
		list := make([]shardDTO, 0, len(shards))
		for _, sh := range shards {
			list = append(list, shardDTO{
				ID:      sh.ShardID,
				Port:    sh.Port,
				Region:  sh.Region,
				Buckets: buckets[sh.ShardID],
				Rows:    sh.RowCount(),
				QPS:     sh.CurrentQPS(),
				LatMs:   sh.AvgLatencyMs(),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"shards":   list,
			"workflow": s.Router.CDC.GetSnapshot(),
			"alerts":   s.Router.HotspotTracker.GetRecentAlerts(),
		})
	})

	_ = http.ListenAndServe(s.HTTPAddr, mux)
}
