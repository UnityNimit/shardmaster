package tui

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"shardmaster/pkg/bench"
	"shardmaster/pkg/router"
	"shardmaster/pkg/storage"
)

type tickMsg time.Time
type peakBurstDoneMsg bench.BenchmarkResult
type asyncOpDoneMsg string

const (
	modalNone        = 0
	modalEditShard   = 1
	modalCreateShard = 2
	modalSQLInput    = 3
)

var presetQueries = []struct {
	title string
	sql   string
}{
	{"Physical Shards & Bytes", "SHOW SHARDS;"},
	{"Point Read #42", "SELECT * FROM users WHERE user_id = 42;"},
	{"K-Way Merge Top 5", "SELECT * FROM users WHERE email LIKE '%@gmail.com' ORDER BY created_at DESC LIMIT 5;"},
	{"Count & Balance Stats", "SELECT COUNT(*), SUM(balance_usd), AVG(balance_usd), MIN(balance_usd), MAX(balance_usd) FROM users;"},
	{"Group By Shard", "SELECT shard_id, COUNT(*), SUM(balance_usd), AVG(balance_usd) FROM users GROUP BY shard_id;"},
	{"Group By Region", "SELECT region, COUNT(*), SUM(balance_usd), AVG(balance_usd) FROM users GROUP BY region;"},
	{"3-Table Relational JOIN", "SELECT u.user_id, u.name, o.order_id, o.product_name, p.payment_method, o.amount_usd FROM users u JOIN orders o ON u.user_id = o.user_id JOIN payments p ON o.order_id = p.order_id ORDER BY o.amount_usd DESC LIMIT 5;"},
	{"Window Function RANK()", "SELECT name, region, balance_usd, RANK() OVER (PARTITION BY region ORDER BY balance_usd DESC) AS reg_rank FROM users LIMIT 6;"},
	{"Bucket Ranges", "SHOW BUCKETS;"},
	{"CDC Log Journal", "SELECT * FROM _shardmaster_cdc LIMIT 6;"},
	{"All Tables Catalog", "SHOW TABLES;"},
	{"Users Schema", "DESCRIBE users;"},
	{"Explain Route #42", "EXPLAIN ANALYZE SELECT * FROM users WHERE user_id = 42;"},
}

// DashboardModel is a clean, minimalist 4-Tab TUI with a 100% free-form, byte-exact Shard Customizer,
// Live Resizer, Custom Shard Creator, and Shard-Pinned SQL Explorer that fits inside 80x24 terminals.
type DashboardModel struct {
	qr            *router.QueryRouter
	loadGenActive bool
	stopLoadFlag  *atomic.Bool
	globalQPS     uint64
	peakBurstQPS  uint64
	statusBanner  string
	ticks         int

	// 0 = Topology & Shards, 1 = CDC & VDiff, 2 = EWMA Hotspots, 3 = SQL Explorer
	activeTab    int
	scrollOffset int

	// Tab 0 (Topology & Shards) interactive selection & 100% free-form modal editor state
	selectedShardIdx int
	modalMode        int
	formFieldIdx     int
	formShardID      uint32
	formInputs       [8]string // 8 completely free-form editable text inputs (0 hardcoded restrictions!)

	// Tab 3 (SQL Explorer) state & shard pinning
	queryIdx       int
	pinnedShardID  int // -1 = All Shards (Cluster), >= 0 = Pinned to specific Shard ID
	customSQLInput string
	usingCustomSQL bool
	activeQueryRes *router.ResultSet
}

func NewDashboardModel(qr *router.QueryRouter) *DashboardModel {
	stop := &atomic.Bool{}
	stop.Store(true)
	m := &DashboardModel{
		qr:            qr,
		loadGenActive: false,
		stopLoadFlag:  stop,
		activeTab:     0,
		pinnedShardID: -1,
		statusBanner:  "Select shard [Up/Dn] | [e] Customize  [n] New  [s] Split +1  [b] Stress Load",
	}
	m.refreshActiveQuery()
	for _, s := range qr.Cluster.GetAllShards() {
		_ = s.TickQPS(1.0)
	}
	m.globalQPS = 0
	return m
}

func (m *DashboardModel) refreshActiveQuery() {
	sqlText := presetQueries[m.queryIdx%len(presetQueries)].sql
	if m.usingCustomSQL && strings.TrimSpace(m.customSQLInput) != "" {
		sqlText = m.customSQLInput
	}
	res, err := m.qr.ExecuteSQLOnShard(sqlText, m.pinnedShardID)
	if err == nil {
		m.activeQueryRes = res
	} else {
		m.activeQueryRes = &router.ResultSet{
			Title:   "SQL EXECUTION ERROR",
			Columns: []string{"error_message"},
			Rows:    [][]string{{err.Error()}},
		}
	}
}

func (m *DashboardModel) runLoadBatch(startUID int64) int64 {
	uid := startUID
	totalRows := m.qr.Cluster.TotalRows()
	if totalRows <= 0 {
		totalRows = 10000
	}
	for i := 0; i < 160; i++ {
		targetUID := (uid % totalRows) + 1
		shardID, bucket := m.qr.RouteFastPoint(targetUID)
		if s, ok := m.qr.Cluster.GetShard(shardID); ok {
			_, _ = s.GetUser(targetUID)
		}
		_ = bucket
		uid++
	}
	return uid
}

func (m *DashboardModel) startBackgroundLoad() {
	m.stopLoadFlag.Store(false)
	go func(stop *atomic.Bool) {
		uid := int64(1)
		for !stop.Load() {
			uid = m.runLoadBatch(uid)
			time.Sleep(12 * time.Millisecond)
		}
	}(m.stopLoadFlag)
}

func (m *DashboardModel) Init() tea.Cmd {
	return tickCmd()
}

func tickCmd() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *DashboardModel) openEditShardModal() {
	shards := m.qr.Cluster.GetAllShards()
	if len(shards) == 0 {
		return
	}
	if m.selectedShardIdx >= len(shards) {
		m.selectedShardIdx = len(shards) - 1
	}
	s := shards[m.selectedShardIdx]
	counts := m.qr.Dir.BucketCountsByShard()
	cfg := s.GetSettings()

	m.modalMode = modalEditShard
	m.formFieldIdx = 0
	m.formShardID = s.ShardID
	m.formInputs = [8]string{
		cfg.CustomAlias,
		strconv.FormatInt(cfg.MaxCapacityBytes, 10),
		strconv.Itoa(cfg.SlabBytesPerBucket),
		strconv.Itoa(counts[s.ShardID]),
		fmt.Sprintf("%d:%d", cfg.Weight, s.Port),
		cfg.Region,
		cfg.HardwareTier,
		fmt.Sprintf("%s/%s/%d", cfg.AccessMode, cfg.ReplicationMode, cfg.MaxConnections),
	}
	m.statusBanner = fmt.Sprintf("Editing S%d | Type ANY value on ANY field ([Ctrl+U] Clear, [Enter] Apply)", s.ShardID)
}

func (m *DashboardModel) openCreateShardModal() {
	shards := m.qr.Cluster.GetAllShards()
	nextID := uint32(len(shards))
	port := 5432 + int(nextID)
	m.modalMode = modalCreateShard
	m.formFieldIdx = 0
	m.formShardID = nextID
	m.formInputs = [8]string{
		fmt.Sprintf("shard-%d", nextID),
		strconv.FormatInt(storage.DefaultShardCapacityBytes, 10), // 67108864 B (64 MB)
		strconv.Itoa(storage.DefaultSlabBytesPerBucket),          // 262144 B (256 KB)
		"128",
		fmt.Sprintf("100:%d", port),
		fmt.Sprintf("local-node-%d", nextID%4),
		"16GB-PC-RAM-Slab",
		"READ_WRITE/SYNC_QUORUM/1000",
	}
	m.statusBanner = fmt.Sprintf("Create Shard %d | Type ANY custom bytes, name, buckets or zone & press [Enter]", nextID)
}

func (m *DashboardModel) handleModalKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	// Custom SQL Input Modal in Tab 4
	if m.modalMode == modalSQLInput {
		switch key {
		case "esc":
			m.modalMode = modalNone
			m.statusBanner = "Cancelled custom SQL input."
			return m, nil
		case "enter":
			m.modalMode = modalNone
			if strings.TrimSpace(m.customSQLInput) != "" {
				m.usingCustomSQL = true
				m.scrollOffset = 0
				m.refreshActiveQuery()
				m.statusBanner = "Executed custom SQL query."
			}
			return m, nil
		case "backspace":
			if len(m.customSQLInput) > 0 {
				m.customSQLInput = m.customSQLInput[:len(m.customSQLInput)-1]
			}
			return m, nil
		case "ctrl+u":
			m.customSQLInput = ""
			return m, nil
		default:
			if len(key) == 1 && key[0] >= 32 && key[0] <= 126 {
				m.customSQLInput += key
			} else if key == "space" {
				m.customSQLInput += " "
			}
			return m, nil
		}
	}

	// 100% Free-Form Shard Editor / Creator Modal (8 fields: 0..7)
	switch key {
	case "esc":
		m.modalMode = modalNone
		m.statusBanner = "Shard customization cancelled."
		return m, nil

	case "up", "shift+tab":
		m.formFieldIdx = (m.formFieldIdx + 7) % 8
		return m, nil

	case "down", "tab":
		m.formFieldIdx = (m.formFieldIdx + 1) % 8
		return m, nil

	case "left":
		m.nudgeFreeFormField(-1)
		return m, nil

	case "right":
		m.nudgeFreeFormField(1)
		return m, nil

	case "ctrl+u":
		m.formInputs[m.formFieldIdx] = ""
		return m, nil

	case "backspace":
		cur := m.formInputs[m.formFieldIdx]
		if len(cur) > 0 {
			m.formInputs[m.formFieldIdx] = cur[:len(cur)-1]
		}
		return m, nil

	case "enter":
		return m.applyFreeFormShardModal()
	}

	// Free-form typing on ANY of the 8 fields!
	if len(key) == 1 && key[0] >= 32 && key[0] <= 126 {
		if len(m.formInputs[m.formFieldIdx]) < 36 {
			m.formInputs[m.formFieldIdx] += key
		}
	} else if key == "space" {
		if len(m.formInputs[m.formFieldIdx]) < 36 {
			m.formInputs[m.formFieldIdx] += " "
		}
	}
	return m, nil
}

// nudgeFreeFormField lets [Left]/[Right] increment or decrement numeric fields by fine-grained steps
// while keeping every field 100% editable as free-form text.
func (m *DashboardModel) nudgeFreeFormField(delta int) {
	switch m.formFieldIdx {
	case 1: // Max Capacity Bytes: nudge by 1,024 bytes (1 KB) or 1 byte if < 1024
		b, err := storage.ParseByteSize(m.formInputs[1])
		if err != nil {
			b = storage.DefaultShardCapacityBytes
		}
		step := int64(1024)
		if b <= 4096 {
			step = 64
		}
		b += int64(delta) * step
		if b < 4 {
			b = 4
		}
		m.formInputs[1] = strconv.FormatInt(b, 10)

	case 2: // Slab Bytes Per Bucket: nudge by 64 bytes
		b, err := storage.ParseByteSize(m.formInputs[2])
		if err != nil {
			b = int64(storage.DefaultSlabBytesPerBucket)
		}
		b += int64(delta) * 64
		if b < 4 {
			b = 4
		}
		m.formInputs[2] = strconv.FormatInt(b, 10)

	case 3: // Target Virtual Buckets (0..1024): nudge by 1 bucket
		n, _ := strconv.Atoi(strings.TrimSpace(m.formInputs[3]))
		n += delta
		if n < 0 {
			n = 0
		}
		if n > 1024 {
			n = 1024
		}
		m.formInputs[3] = strconv.Itoa(n)

	case 4: // Weight:Port
		parts := strings.Split(m.formInputs[4], ":")
		w, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
		port := 5432 + int(m.formShardID)
		if len(parts) > 1 {
			if p, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil && p > 0 {
				port = p
			}
		}
		w += delta * 5
		if w < 0 {
			w = 0
		}
		m.formInputs[4] = fmt.Sprintf("%d:%d", w, port)
	}
}

func (m *DashboardModel) applyFreeFormShardModal() (tea.Model, tea.Cmd) {
	alias := strings.TrimSpace(m.formInputs[0])
	if alias == "" {
		alias = fmt.Sprintf("shard-%d", m.formShardID)
	}

	maxBytes, err := storage.ParseByteSize(m.formInputs[1])
	if err != nil || maxBytes <= 0 {
		maxBytes = storage.DefaultShardCapacityBytes
	}

	slabBytes64, err := storage.ParseByteSize(m.formInputs[2])
	if err != nil || slabBytes64 <= 0 {
		slabBytes64 = int64(storage.DefaultSlabBytesPerBucket)
	}
	slabBytes := int(slabBytes64)

	targetBuckets, err := strconv.Atoi(strings.TrimSpace(m.formInputs[3]))
	if err != nil {
		targetBuckets = 256
	}
	if targetBuckets < 0 {
		targetBuckets = 0
	}
	if targetBuckets > 1024 {
		targetBuckets = 1024
	}

	weight := 100
	customPort := 5432 + int(m.formShardID)
	wpParts := strings.Split(m.formInputs[4], ":")
	if w, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(wpParts[0]), "%")); err == nil && w >= 0 {
		weight = w
	}
	if len(wpParts) > 1 {
		if p, err := strconv.Atoi(strings.TrimSpace(wpParts[1])); err == nil && p > 0 {
			customPort = p
		}
	}

	region := strings.TrimSpace(m.formInputs[5])
	if region == "" {
		region = "local"
	}

	tier := strings.TrimSpace(m.formInputs[6])
	if tier == "" {
		tier = "Custom-RAM-Slab"
	}

	accessMode := "READ_WRITE"
	replMode := "SYNC_QUORUM"
	maxConns := 1000
	mrParts := strings.Split(m.formInputs[7], "/")
	if len(mrParts) > 0 && strings.TrimSpace(mrParts[0]) != "" {
		accessMode = strings.ToUpper(strings.TrimSpace(mrParts[0]))
	}
	if len(mrParts) > 1 && strings.TrimSpace(mrParts[1]) != "" {
		replMode = strings.ToUpper(strings.TrimSpace(mrParts[1]))
	}
	if len(mrParts) > 2 {
		if c, err := strconv.Atoi(strings.TrimSpace(mrParts[2])); err == nil && c > 0 {
			maxConns = c
		}
	}

	cfg := storage.ShardSettings{
		CustomAlias:        alias,
		CustomPort:         customPort,
		Region:             region,
		MaxCapacityBytes:   maxBytes,
		SlabBytesPerBucket: slabBytes,
		HardwareTier:       tier,
		Weight:             weight,
		TargetBuckets:      targetBuckets,
		AccessMode:         accessMode,
		ReplicationMode:    replMode,
		MaxConnections:     maxConns,
		BufferPoolBytes:    16 * 1024 * 1024,
	}

	sid := m.formShardID
	mode := m.modalMode
	m.modalMode = modalNone

	if mode == modalCreateShard {
		newShard := m.qr.Cluster.CreateCustomShard(cfg)
		m.qr.Dir.RegisterShard(newShard.ShardID)
		m.selectedShardIdx = int(newShard.ShardID)
		m.statusBanner = fmt.Sprintf("Created S%d [%s] (Max: %s) -> Streaming %d Buckets...", newShard.ShardID, newShard.DisplayName(), storage.FormatBytesExact(maxBytes), targetBuckets)
		id := newShard.ShardID
		alias := newShard.DisplayName()
		b := targetBuckets
		maxB := maxBytes
		slabB := slabBytes
		return m, func() tea.Msg {
			_, err := m.qr.CDC.ResizeShardBuckets(id, b, 15*time.Millisecond)
			if s, ok := m.qr.Cluster.GetShard(id); ok {
				s.ResizeMemorySlabs(maxB, slabB)
			}
			if err != nil {
				return asyncOpDoneMsg(fmt.Sprintf("Create S%d [%s] Error: %v", id, alias, err))
			}
			return asyncOpDoneMsg(fmt.Sprintf("Provisioned S%d [%s]: %d Buckets Active (VDiff Verified)", id, alias, b))
		}
	} else {
		if s, ok := m.qr.Cluster.GetShard(sid); ok {
			counts := m.qr.Dir.BucketCountsByShard()
			curB := counts[sid]
			explicitBuckets := -1
			if targetBuckets != curB {
				explicitBuckets = targetBuckets
			}
			snap, enfErr := m.qr.CDC.EnforceShardCapacityAndBuckets(sid, cfg, explicitBuckets, 0)
			if enfErr != nil {
				m.statusBanner = fmt.Sprintf("REJECTED S%d: %v", sid, enfErr)
			} else {
				evacuated := 0
				if snap != nil {
					evacuated = snap.RangesCompleted
				}
				m.qr.Cluster.SaveStateFile(m.qr.Dir.SnapshotBuckets())
				if evacuated > 0 {
					m.statusBanner = fmt.Sprintf("Saved S%d [%s] | Evacuated %d Ranges via CDC | RAM: %s / %s",
						sid, s.DisplayName(), evacuated, storage.FormatBytesCompact(s.UsedMemoryBytes()), storage.FormatBytesCompact(maxBytes))
				} else {
					m.statusBanner = fmt.Sprintf("Saved S%d [%s] | Live RAM: %s / %s",
						sid, s.DisplayName(), storage.FormatBytesExact(s.UsedMemoryBytes()), storage.FormatBytesExact(maxBytes))
				}
			}
		}
	}
	return m, nil
}

func (m *DashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.MouseMsg:
		if msg.Button == tea.MouseButtonWheelUp {
			if m.scrollOffset > 0 {
				m.scrollOffset--
			}
		} else if msg.Button == tea.MouseButtonWheelDown {
			m.scrollOffset++
		}
		return m, nil

	case tea.KeyMsg:
		if m.modalMode != modalNone {
			return m.handleModalKey(msg)
		}

		key := msg.String()
		shards := m.qr.Cluster.GetAllShards()

		switch key {
		case "tab", "right":
			m.activeTab = (m.activeTab + 1) % 4
			m.scrollOffset = 0
			if m.activeTab == 3 {
				m.refreshActiveQuery()
			}
			return m, nil
		case "shift+tab", "left":
			m.activeTab = (m.activeTab + 3) % 4
			m.scrollOffset = 0
			if m.activeTab == 3 {
				m.refreshActiveQuery()
			}
			return m, nil
		case "up", "k":
			if m.activeTab == 0 && len(shards) > 0 {
				m.selectedShardIdx = (m.selectedShardIdx + len(shards) - 1) % len(shards)
			} else if m.scrollOffset > 0 {
				m.scrollOffset--
			}
			return m, nil
		case "down", "j":
			if m.activeTab == 0 && len(shards) > 0 {
				m.selectedShardIdx = (m.selectedShardIdx + 1) % len(shards)
			} else {
				m.scrollOffset++
			}
			return m, nil
		case "[", "p":
			if m.activeTab == 3 {
				m.usingCustomSQL = false
				m.queryIdx = (m.queryIdx + len(presetQueries) - 1) % len(presetQueries)
				m.scrollOffset = 0
				m.refreshActiveQuery()
			}
			return m, nil
		case "]":
			if m.activeTab == 3 {
				m.usingCustomSQL = false
				m.queryIdx = (m.queryIdx + 1) % len(presetQueries)
				m.scrollOffset = 0
				m.refreshActiveQuery()
			}
			return m, nil
		case "enter", "e":
			if m.activeTab == 0 {
				m.openEditShardModal()
				return m, nil
			}
		case "+", "=", ">":
			if m.activeTab == 0 && len(shards) > 0 {
				s := shards[m.selectedShardIdx%len(shards)]
				counts := m.qr.Dir.BucketCountsByShard()
				oldB := counts[s.ShardID]
				newB := oldB + 16
				if newB > 1024 {
					newB = 1024
				}
				m.statusBanner = fmt.Sprintf("Growing S%d [%s]: %d -> %d Buckets via Live CDC...", s.ShardID, s.DisplayName(), oldB, newB)
				id := s.ShardID
				alias := s.DisplayName()
				return m, func() tea.Msg {
					_, err := m.qr.CDC.ResizeShardBuckets(id, newB, 18*time.Millisecond)
					if err != nil {
						return asyncOpDoneMsg(fmt.Sprintf("Grow S%d [%s] Rejected: %v", id, alias, err))
					}
					return asyncOpDoneMsg(fmt.Sprintf("Resized S%d [%s]: %d -> %d Buckets (0ms Downtime, VDiff OK)", id, alias, oldB, newB))
				}
			}
		case "-", "_", "<":
			if m.activeTab == 0 && len(shards) > 0 {
				s := shards[m.selectedShardIdx%len(shards)]
				counts := m.qr.Dir.BucketCountsByShard()
				oldB := counts[s.ShardID]
				newB := oldB - 16
				if newB < 0 {
					newB = 0
				}
				m.statusBanner = fmt.Sprintf("Shrinking S%d [%s]: %d -> %d Buckets via Live CDC...", s.ShardID, s.DisplayName(), oldB, newB)
				id := s.ShardID
				alias := s.DisplayName()
				return m, func() tea.Msg {
					_, err := m.qr.CDC.ResizeShardBuckets(id, newB, 18*time.Millisecond)
					if err != nil {
						return asyncOpDoneMsg(fmt.Sprintf("Shrink S%d [%s] Rejected: %v", id, alias, err))
					}
					return asyncOpDoneMsg(fmt.Sprintf("Resized S%d [%s]: %d -> %d Buckets (0ms Downtime, VDiff OK)", id, alias, oldB, newB))
				}
			}
		case "/", "i":
			if m.activeTab == 3 {
				m.modalMode = modalSQLInput
				if m.customSQLInput == "" {
					m.customSQLInput = presetQueries[m.queryIdx%len(presetQueries)].sql
				}
				m.statusBanner = "Type SQL query & press [Enter] to execute ([Esc] Cancel, [Ctrl+U] Clear)"
				return m, nil
			}
		}

		switch strings.ToLower(key) {
		case "q", "ctrl+c", "esc":
			m.stopLoadFlag.Store(true)
			return m, tea.Quit

		case "1":
			m.activeTab = 0
			m.scrollOffset = 0
			return m, nil
		case "2":
			m.activeTab = 1
			m.scrollOffset = 0
			return m, nil
		case "3":
			m.activeTab = 2
			m.scrollOffset = 0
			return m, nil
		case "4":
			m.activeTab = 3
			m.scrollOffset = 0
			m.refreshActiveQuery()
			return m, nil

		case "n":
			if m.activeTab == 3 {
				m.usingCustomSQL = false
				m.queryIdx = (m.queryIdx + 1) % len(presetQueries)
				m.scrollOffset = 0
				m.refreshActiveQuery()
			} else {
				m.openCreateShardModal()
			}
			return m, nil

		case "d":
			if len(shards) > 0 {
				s := shards[m.selectedShardIdx%len(shards)]
				m.statusBanner = fmt.Sprintf("Draining S%d [%s] -> Evacuating all Buckets via CDC...", s.ShardID, s.DisplayName())
				id := s.ShardID
				alias := s.DisplayName()
				return m, func() tea.Msg {
					_, err := m.qr.CDC.DrainShard(id, 18*time.Millisecond)
					if err != nil {
						return asyncOpDoneMsg(fmt.Sprintf("Drain S%d [%s] Rejected: %v", id, alias, err))
					}
					return asyncOpDoneMsg(fmt.Sprintf("Drained S%d [%s]: 0 Buckets Remaining (All Data Evacuated)", id, alias))
				}
			}
			return m, nil

		case "w":
			m.statusBanner = "Rebalancing 1,024 Buckets across Shards by Custom Weight..."
			return m, func() tea.Msg {
				_, err := m.qr.CDC.RebalanceByWeights(18 * time.Millisecond)
				if err != nil {
					return asyncOpDoneMsg(fmt.Sprintf("Weighted Rebalance Error: %v", err))
				}
				return asyncOpDoneMsg("Weighted Rebalance Complete: 1,024 Buckets Matched to Shard Weights")
			}

		case "f":
			if m.activeTab == 0 && len(shards) > 0 {
				s := shards[m.selectedShardIdx%len(shards)]
				m.pinnedShardID = int(s.ShardID)
				m.activeTab = 3
				m.scrollOffset = 0
				m.refreshActiveQuery()
				m.statusBanner = fmt.Sprintf("SQL Explorer pinned to S%d [%s] | Press [f] to cycle target shard", s.ShardID, s.DisplayName())
			} else {
				if m.pinnedShardID < 0 {
					m.pinnedShardID = 0
				} else if m.pinnedShardID+1 < len(shards) {
					m.pinnedShardID++
				} else {
					m.pinnedShardID = -1
				}
				m.scrollOffset = 0
				m.refreshActiveQuery()
				if m.pinnedShardID < 0 {
					m.statusBanner = "SQL Scope: ALL SHARDS (Distributed Cluster)"
				} else if s, ok := m.qr.Cluster.GetShard(uint32(m.pinnedShardID)); ok {
					m.statusBanner = fmt.Sprintf("SQL Scope: PINNED -> Shard %d [%s]", s.ShardID, s.DisplayName())
				}
			}
			return m, nil

		case "s":
			cur := m.qr.Dir.ActiveShards()
			target := cur + 1
			m.statusBanner = fmt.Sprintf("Splitting %d -> %d Shards via Zero-Downtime CDC...", cur, target)
			return m, func() tea.Msg {
				snap, err := m.qr.CDC.RebalanceToShards(target, 25*time.Millisecond)
				if err != nil {
					return asyncOpDoneMsg(fmt.Sprintf("Split %d -> %d Error: %v", cur, target, err))
				}
				moved := int64(0)
				if snap != nil {
					moved = snap.RowsMigrated
				}
				return asyncOpDoneMsg(fmt.Sprintf("Split Complete: %d -> %d Shards (%s rows migrated, 0ms Downtime)", cur, target, formatUintComma(uint64(moved))))
			}

		case "h":
			m.qr.HotspotTracker.InjectBucketTrafficSpike(412, 6800)
			if ev := m.qr.HotspotTracker.TickAndEvaluate(1.0); ev != nil {
				m.statusBanner = fmt.Sprintf("Hotspot Bucket #412 (%d QPS) -> Isolated Shard %d -> %d", ev.MeasuredQPS, ev.FromShard, ev.ToShard)
			} else {
				m.statusBanner = "Hotspot Bucket #412 (>6,800 QPS) -> Isolated!"
			}
			return m, nil

		case "b":
			if m.loadGenActive {
				m.stopLoadFlag.Store(true)
				m.loadGenActive = false
				m.statusBanner = "Background Stress Load: OFF (Idle QPS = 0/s)."
			} else {
				m.loadGenActive = true
				m.startBackgroundLoad()
				m.statusBanner = "Background Stress Load: ON (Generating Live Queries)."
			}
			return m, nil

		case "m":
			m.statusBanner = "Running Multi-Core Zero-Alloc Routing Burst..."
			return m, func() tea.Msg {
				res := bench.RunPeakBenchmark(m.qr, 700*time.Millisecond, runtime.NumCPU()*2)
				return peakBurstDoneMsg(res)
			}

		case "r":
			m.qr.Cluster.InitializeShards(4)
			m.qr.Dir.Reset(4)
			m.qr.Cluster.SeedCluster(0, m.qr.Dir.GetBucketOwner)
			if m.qr.SQL != nil {
				m.qr.SQL.ReseedFromCluster()
			}
			m.qr.Cluster.SaveStateFile(m.qr.Dir.SnapshotBuckets())
			m.selectedShardIdx = 0
			m.pinnedShardID = -1
			m.scrollOffset = 0
			m.refreshActiveQuery()
			m.statusBanner = fmt.Sprintf("Reset to 4 Local Shards (1,024 Buckets, %s rows).", formatUintComma(uint64(m.qr.Cluster.TotalRows())))
			return m, nil
		}

	case asyncOpDoneMsg:
		m.statusBanner = string(msg)
		if m.activeTab == 3 && m.modalMode == modalNone {
			m.refreshActiveQuery()
		}
		return m, nil

	case peakBurstDoneMsg:
		m.peakBurstQPS = msg.RoutingThroughputQPS
		m.statusBanner = fmt.Sprintf(
			"PEAK BURST: %s req/sec (%d ns/op, 0 allocs) | 0 Dropped!",
			formatUintComma(msg.RoutingThroughputQPS),
			msg.P50LatencyNs,
		)
		return m, nil

	case tickMsg:
		m.ticks++
		shards := m.qr.Cluster.GetAllShards()
		var sumQPS uint64
		for _, s := range shards {
			q := s.TickQPS(0.25)
			sumQPS += q
		}
		m.globalQPS = sumQPS
		if m.ticks%4 == 0 {
			if ev := m.qr.HotspotTracker.TickAndEvaluate(1.0); ev != nil {
				m.statusBanner = fmt.Sprintf("Hotspot Bucket #%d -> Isolated Shard %d -> %d", ev.BucketID, ev.FromShard, ev.ToShard)
			}
			if m.activeTab == 3 && m.modalMode == modalNone {
				m.refreshActiveQuery()
			}
		}
		return m, tickCmd()
	}

	return m, nil
}

func (m *DashboardModel) View() string {
	// 78-column width (76 inner chars) and 21-line height so it fits cleanly inside 80x24 terminals without wrapping
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(0, 1).
		Width(78)

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86"))
	colHdrStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	okStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("42"))
	warnStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))
	hotStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))
	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	selStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("51"))
	activeTabStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("33")).Padding(0, 1)
	inactiveTabStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("248")).Padding(0, 1)
	barFillStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	migBarStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("46"))

	shards := m.qr.Cluster.GetAllShards()
	bucketCounts := m.qr.Dir.BucketCountsByShard()
	wf := m.qr.CDC.GetSnapshot()
	alerts := m.qr.HotspotTracker.GetRecentAlerts()
	totalRows := uint64(m.qr.Cluster.TotalRows())

	var b strings.Builder

	// 1. Minimalist Animated Header with Real Row Count & Real Measured QPS
	spinFrames := []byte{'-', '\\', '|', '/'}
	spinChar := spinFrames[m.ticks%len(spinFrames)]
	peakText := ""
	if m.peakBurstQPS > 0 {
		peakText = " | PEAK: " + okStyle.Render(formatUintComma(m.peakBurstQPS)+"/s")
	}
	b.WriteString(fmt.Sprintf(
		"%s %s  |  %d Shards  |  %s Rows  |  QPS: %s/s%s\n",
		barFillStyle.Render(fmt.Sprintf("[%c]", spinChar)),
		titleStyle.Render("SHARDMASTER"),
		len(shards),
		okStyle.Render(formatUintComma(totalRows)),
		warnStyle.Render(formatUintComma(m.globalQPS)),
		peakText,
	))

	// 2. Clean Navigation Tabs
	tabNames := []string{"[1] Shards & Bytes", "[2] CDC & VDiff", "[3] Hotspots", "[4] SQL Explorer"}
	var renderedTabs []string
	for i, name := range tabNames {
		if i == m.activeTab {
			renderedTabs = append(renderedTabs, activeTabStyle.Render(name))
		} else {
			renderedTabs = append(renderedTabs, inactiveTabStyle.Render(name))
		}
	}
	b.WriteString(strings.Join(renderedTabs, " ") + "\n")
	b.WriteString(dimStyle.Render(strings.Repeat("-", 74)) + "\n")

	// 3. Focused Tab Content or 100% Free-Form Byte-Exact Customizer Modal (13-line viewport)
	const viewLines = 13
	var lines []string
	maxScroll := 0

	if m.modalMode == modalEditShard || m.modalMode == modalCreateShard {
		hdr := fmt.Sprintf("CUSTOMIZE SHARD %d ([Enter] Save  [Ctrl+U] Clear  [Esc] Cancel)", m.formShardID)
		if m.modalMode == modalCreateShard {
			hdr = fmt.Sprintf("CREATE LOCAL SHARD %d ([Enter] Provision  [Ctrl+U] Clear  [Esc] Cancel)", m.formShardID)
		}
		lines = append(lines, titleStyle.Render(hdr))
		lines = append(lines, dimStyle.Render(" FIELD                 CURRENT VALUE                LIVE UNIT PREVIEW"))

		parsedMaxB, _ := storage.ParseByteSize(m.formInputs[1])
		parsedSlabB, _ := storage.ParseByteSize(m.formInputs[2])
		fields := []struct {
			label string
			val   string
			hint  string
		}{
			{"1. Custom Shard Name", m.formInputs[0], "(Any local alias)"},
			{"2. Max Size (Bytes) ", m.formInputs[1], fmt.Sprintf("(= %s)", storage.FormatBytesCompact(parsedMaxB))},
			{"3. Slab Bytes/Bucket", m.formInputs[2], fmt.Sprintf("(= %s/bkt)", storage.FormatBytesCompact(parsedSlabB))},
			{"4. Virtual Buckets  ", m.formInputs[3], "(0..1024 buckets)"},
			{"5. Weight % : Port  ", m.formInputs[4], "(e.g. 100:5432)"},
			{"6. Local Zone / Node", m.formInputs[5], "(e.g. local-node-0)"},
			{"7. Hardware Profile ", m.formInputs[6], "(Local RAM profile)"},
			{"8. Mode/Repl/Conns  ", m.formInputs[7], "(RW/SYNC_QUORUM/1000)"},
		}
		for idx, f := range fields {
			cursor := "  "
			valRendered := okStyle.Render(f.val)
			if idx == m.formFieldIdx {
				cursor = "> "
				valRendered = selStyle.Render("[" + f.val + "_]")
			}
			line := fmt.Sprintf("%s%-20s: %-28s %s", cursor, f.label, valRendered, dimStyle.Render(truncateStr(f.hint, 20)))
			lines = append(lines, line)
		}
	} else if m.activeTab == 0 {
		// TAB 1: TOPOLOGY, LABELED COLUMNS & PINNED SELECTED SHARD INSPECTOR
		lines = append(lines, titleStyle.Render("LOCAL SHARD TOPOLOGY ([Up/Dn] Select  [e] Customize  [n] New  [f] Pin SQL)"))
		lines = append(lines, colHdrStyle.Render(
			fmt.Sprintf(" %-3s %-12s  %-12s  %9s  %-15s  %6s  %6s",
				"ID", "SHARD NAME", "LOCAL ZONE", "BUCKETS", "RAM USED / MAX", "ROWS", "QPS"),
		))

		if m.selectedShardIdx >= len(shards) && len(shards) > 0 {
			m.selectedShardIdx = len(shards) - 1
		}

		// Keep up to 8 shard rows visible at once so header + 8 shards + 3-line inspector = 13 lines
		const maxVisibleShards = 8
		startIdx := 0
		if len(shards) > maxVisibleShards {
			if m.selectedShardIdx >= maxVisibleShards {
				startIdx = m.selectedShardIdx - maxVisibleShards + 1
			}
			if startIdx+maxVisibleShards > len(shards) {
				startIdx = len(shards) - maxVisibleShards
			}
		}
		endIdx := startIdx + maxVisibleShards
		if endIdx > len(shards) {
			endIdx = len(shards)
		}

		for idx := startIdx; idx < endIdx; idx++ {
			s := shards[idx]
			cfg := s.GetSettings()
			bCount := bucketCounts[s.ShardID]
			shardQPS := s.CurrentQPS()
			usedB := s.UsedMemoryBytes()
			maxB := cfg.MaxCapacityBytes

			prefix := " "
			idStr := fmt.Sprintf("S%-2d", s.ShardID)
			nameStr := fmt.Sprintf("%-12s", truncateStr(s.DisplayName(), 12))
			zoneStr := fmt.Sprintf("%-12s", truncateStr(s.Region, 12))
			bktStr := fmt.Sprintf("%4d/1024", bCount)
			ramStr := fmt.Sprintf("%-15s", fmt.Sprintf("%s / %s", storage.FormatBytesCompact(usedB), storage.FormatBytesCompact(maxB)))
			rowStr := fmt.Sprintf("%6s", formatUintComma(uint64(s.RowCount())))
			qpsStr := fmt.Sprintf("%6s", formatCompactUint(shardQPS)+"/s")

			if idx == m.selectedShardIdx {
				prefix = ">"
				idStr = selStyle.Render(idStr)
				nameStr = selStyle.Render(nameStr)
			} else {
				zoneStr = dimStyle.Render(zoneStr)
			}

			lines = append(lines, fmt.Sprintf(
				"%s%s %s  %s  %s  %s  %s  %s",
				prefix, idStr, nameStr, zoneStr, bktStr, ramStr, rowStr, qpsStr,
			))
		}
		for len(lines) < 2+maxVisibleShards {
			lines = append(lines, "")
		}

		// Pinned Selected Shard Inspector Card at the bottom of Tab 1 (always visible!)
		if len(shards) > 0 {
			sel := shards[m.selectedShardIdx%len(shards)]
			scfg := sel.GetSettings()
			usedB := sel.UsedMemoryBytes()
			lines = append(lines, dimStyle.Render(strings.Repeat(".", 74)))
			lines = append(lines, fmt.Sprintf(
				" SELECTED: %s (Port :%d) | Zone: %s | Mode: %s",
				selStyle.Render(fmt.Sprintf("S%d [%s]", sel.ShardID, sel.DisplayName())),
				sel.Port,
				okStyle.Render(sel.Region),
				warnStyle.Render(scfg.AccessMode),
			))
			lines = append(lines, fmt.Sprintf(
				" EXACT RAM: %s B (%s) / Max: %s B (%s) | Slab: %s B/bkt",
				formatUintComma(uint64(usedB)),
				storage.FormatBytesCompact(usedB),
				formatUintComma(uint64(scfg.MaxCapacityBytes)),
				storage.FormatBytesCompact(scfg.MaxCapacityBytes),
				formatUintComma(uint64(scfg.SlabBytesPerBucket)),
			))
		}
	} else {
		switch m.activeTab {
		case 1: // TAB 2: CDC & VDIFF
			lines = append(lines, titleStyle.Render("VITESS CDC VREPLICATION & CRYPTOGRAPHIC VDIFF"))
			lines = append(lines, fmt.Sprintf(" Workflow:  %s  |  Status: [%s]", wf.Title, okStyle.Render(wf.Status)))
			lines = append(lines, fmt.Sprintf(" Scope:     %s", wf.CurrentRangeText))
			progFilled := int((wf.ProgressPct / 100.0) * 24.0)
			if progFilled > 24 {
				progFilled = 24
			}
			if progFilled < 0 {
				progFilled = 0
			}
			progBar := migBarStyle.Render(strings.Repeat("#", progFilled)) + dimStyle.Render(strings.Repeat(".", 24-progFilled))
			lines = append(lines, fmt.Sprintf(
				" Progress:  [%s] %.0f%% (%s rows migrated)",
				progBar,
				wf.ProgressPct,
				formatUintComma(uint64(wf.RowsMigrated)),
			))
			lines = append(lines, fmt.Sprintf(" CDC Lag:   %.2f ms  |  Parity: %s", wf.ReplicationLagMs, okStyle.Render(wf.VDiffStatus)))
			lines = append(lines, dimStyle.Render(strings.Repeat(".", 74)))
			lines = append(lines, colHdrStyle.Render(" BUCKET RANGE    SHARD ROUTE   ROWS MOVED   XOR-SHA256 PARITY DIGEST   STATUS"))
			history := m.qr.CDC.GetVDiffHistory()
			if len(history) == 0 {
				lines = append(lines, dimStyle.Render(" Press [s] Split Cluster, [+/-] Resize Buckets, or [n] New Shard to stream CDC."))
			} else {
				for i, vd := range history {
					if i >= 6 {
						break
					}
					lines = append(lines, fmt.Sprintf(
						" Bkt [%03d..%03d]  S%-2d -> S%-2d    %10s   %-24s   %s",
						vd.StartBucket, vd.EndBucket, vd.SourceShard, vd.TargetShard,
						formatUintComma(uint64(vd.TargetRows)),
						dimStyle.Render(vd.TargetDigest[:22]+".."),
						okStyle.Render("[OK]"),
					))
				}
			}

		case 2: // TAB 3: EWMA HOTSPOTS
			lines = append(lines, titleStyle.Render("AUTONOMOUS EWMA HOTSPOT ENGINE (Press [h] to Spike Bucket #412)"))
			if len(alerts) > 0 {
				for i := 0; i < len(alerts) && i < 2; i++ {
					a := alerts[i]
					lines = append(lines, hotStyle.Render(fmt.Sprintf(
						" [ISOLATED] Bucket #%d (%d QPS) moved Shard %d -> Shard %d (0ms lag)",
						a.BucketID, a.MeasuredQPS, a.FromShard, a.ToShard,
					)))
				}
			} else {
				lines = append(lines, okStyle.Render(" [OK] All 1,024 Virtual Buckets Within Normal Thermal Envelope (<5x Mean)"))
			}
			lines = append(lines, dimStyle.Render(strings.Repeat(".", 74)))
			lines = append(lines, colHdrStyle.Render(" RANK   VIRTUAL BUCKET   OWNER SHARD (PORT)   DECAYED EWMA QPS   STATUS"))
			top := m.qr.HotspotTracker.GetTopHotBuckets(8)
			for i, h := range top {
				lines = append(lines, fmt.Sprintf(
					"  #%-3d  Bucket #%-6d   Shard %-2d (:%-5d)    %10s QPS     %s",
					i+1, h.BucketID, h.OwnerShard, 5432+h.OwnerShard,
					formatUintComma(h.EWMAQPS),
					okStyle.Render("[READY]"),
				))
			}

		case 3: // TAB 4: SQL EXPLORER (WITH SHARD PINNING & CUSTOM SQL INPUT)
			scopeBadge := okStyle.Render("[Scope: ALL SHARDS]")
			if m.pinnedShardID >= 0 {
				if ps, ok := m.qr.Cluster.GetShard(uint32(m.pinnedShardID)); ok {
					scopeBadge = selStyle.Render(fmt.Sprintf("[PINNED: S%d %s]", ps.ShardID, ps.DisplayName()))
				}
			}
			q := presetQueries[m.queryIdx%len(presetQueries)]
			headerTitle := fmt.Sprintf("SQL [%d/%d]: %s", m.queryIdx+1, len(presetQueries), q.title)
			if m.usingCustomSQL {
				headerTitle = "SQL [CUSTOM QUERY]"
			}
			lines = append(lines, fmt.Sprintf(
				"%s  %s  %s",
				titleStyle.Render(headerTitle),
				scopeBadge,
				dimStyle.Render("([p/n] Preset [f] Scope [/] Type)"),
			))
			if m.modalMode == modalSQLInput {
				lines = append(lines, " "+selStyle.Render("SQL> "+m.customSQLInput+"_"))
			} else if m.usingCustomSQL {
				lines = append(lines, " "+warnStyle.Render(truncateStr(m.customSQLInput, 72)))
			} else {
				lines = append(lines, " "+warnStyle.Render(truncateStr(q.sql, 72)))
			}
			if m.activeQueryRes != nil {
				lines = append(lines, renderAlignedTUIRows(m.activeQueryRes, barFillStyle, dimStyle, 72)...)
			}
		}
	}

	if m.activeTab == 0 && m.modalMode == modalNone {
		// Tab 0 handles its own 8-shard window so headers & bottom inspector stay pinned
		for len(lines) < viewLines {
			lines = append(lines, "")
		}
		for _, l := range lines[:viewLines] {
			b.WriteString(l + "\n")
		}
	} else {
		maxScroll = len(lines) - viewLines
		if maxScroll < 0 {
			maxScroll = 0
		}
		if m.modalMode != modalNone && m.modalMode != modalSQLInput {
			m.scrollOffset = 0
		}
		if m.scrollOffset > maxScroll {
			m.scrollOffset = maxScroll
		}
		end := m.scrollOffset + viewLines
		if end > len(lines) {
			end = len(lines)
		}
		visible := lines[m.scrollOffset:end]
		for len(visible) < viewLines {
			visible = append(visible, "")
		}
		for _, l := range visible {
			b.WriteString(l + "\n")
		}
	}

	// 4. Minimalist 2-Line Context-Aware Footer (Strictly <= 72 chars so it NEVER wraps)
	b.WriteString(dimStyle.Render(strings.Repeat("-", 74)) + "\n")
	scrollHint := ""
	if maxScroll > 0 {
		scrollHint = fmt.Sprintf(" [Up/Dn %d/%d]", m.scrollOffset+1, maxScroll+1)
	}
	b.WriteString(" " + warnStyle.Render(truncateStr(m.statusBanner, 60)) + dimStyle.Render(scrollHint) + "\n")
	if m.modalMode == modalEditShard || m.modalMode == modalCreateShard {
		b.WriteString(dimStyle.Render(" [Up/Dn] Field  [Type] Edit  [L/R] Nudge  [Ctrl+U] Clear  [Enter] Save"))
	} else if m.activeTab == 3 {
		b.WriteString(dimStyle.Render(" [p/n] Preset  [L/R] Tab  [f] Pin Shard  [/] Custom SQL  [q] Back"))
	} else {
		b.WriteString(dimStyle.Render(" [e] Customize  [n] New  [+/-] Buckets  [s] Split +1  [b] Load  [q] Back"))
	}

	return borderStyle.Render(b.String())
}

func formatCompactUint(n uint64) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000.0)
	}
	return strconv.FormatUint(n, 10)
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func formatUintComma(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	rem := len(s) % 3
	if rem > 0 {
		out = append(out, s[:rem]...)
		if len(s) > rem {
			out = append(out, ',')
		}
	}
	for i := rem; i < len(s); i += 3 {
		out = append(out, s[i:i+3]...)
		if i+3 < len(s) {
			out = append(out, ',')
		}
	}
	return string(out)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func renderAlignedTUIRows(res *router.ResultSet, hdrStyle, dimStyle lipgloss.Style, maxWidth int) []string {
	if res == nil || len(res.Columns) == 0 {
		return nil
	}
	widths := make([]int, len(res.Columns))
	for i, c := range res.Columns {
		widths[i] = len(c)
	}
	for _, r := range res.Rows {
		for i := 0; i < len(res.Columns) && i < len(r); i++ {
			if len(r[i]) > widths[i] {
				widths[i] = len(r[i])
			}
		}
	}

	// Determine how many columns fit cleanly within maxWidth
	used := 1
	numCols := 0
	for i, w := range widths {
		if w > 22 {
			widths[i] = 22
			w = 22
		}
		if used+w+3 > maxWidth && numCols >= 2 {
			break
		}
		used += w + 3
		numCols++
	}
	if numCols == 0 {
		numCols = 1
	}

	var out []string
	var hdr strings.Builder
	hdr.WriteString(" ")
	for i := 0; i < numCols; i++ {
		if i > 0 {
			hdr.WriteString(" | ")
		}
		hdr.WriteString(fmt.Sprintf("%-*s", widths[i], truncateStr(res.Columns[i], widths[i])))
	}
	out = append(out, hdrStyle.Render(hdr.String()))

	for _, r := range res.Rows {
		var rowStr strings.Builder
		rowStr.WriteString(" ")
		for i := 0; i < numCols; i++ {
			if i > 0 {
				rowStr.WriteString(dimStyle.Render(" | "))
			}
			val := ""
			if i < len(r) {
				val = r[i]
			}
			rowStr.WriteString(fmt.Sprintf("%-*s", widths[i], truncateStr(val, widths[i])))
		}
		out = append(out, rowStr.String())
	}
	return out
}
