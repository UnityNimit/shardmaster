package pgwire

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
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

func (s *Server) handlePGClient(conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// Step 1: PostgreSQL v3.0 Startup & SSL Negotiation
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	if _, isSSL := startupMsg.(*pgproto3.SSLRequest); isSSL {
		// Decline SSL cleanly with 'N' so psql proceeds with StartupMessage
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
		_ = backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		_ = backend.Send(&pgproto3.BackendKeyData{ProcessID: 6000, SecretKey: 424242})
		if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
			return
		}
	default:
		return
	}

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
			s.handleQueryMessage(backend, m.String)
		case *pgproto3.Parse:
			_ = backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Sync:
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Terminate:
			return
		default:
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

func (s *Server) handleQueryMessage(backend *pgproto3.Backend, sql string) {
	if strings.TrimSpace(sql) == "" {
		_ = backend.Send(&pgproto3.EmptyQueryResponse{})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	res, err := s.Router.ExecuteSQL(sql)
	if err != nil {
		_ = backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}

	// Build RowDescription frame (OID 25 = TEXT)
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

	// Stream DataRow frames
	for _, row := range res.Rows {
		vals := make([][]byte, len(row))
		for i, cell := range row {
			vals[i] = []byte(cell)
		}
		_ = backend.Send(&pgproto3.DataRow{Values: vals})
	}

	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(res.CommandTag)})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
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
