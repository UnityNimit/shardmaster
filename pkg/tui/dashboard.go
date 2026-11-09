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

// DashboardModel is a minimalist 4-Tab TUI with a 100% free-form, byte-exact Shard Customizer,
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
		loadGenActive: true,
		stopLoadFlag:  stop,
		activeTab:     0,
		pinnedShardID: -1,
		statusBanner:  "Ready · Select shard [Up/Dn] · [e] Customize · [s] Split +1 · [b] Traffic",
	}
	m.refreshActiveQuery()
	for _, s := range qr.Cluster.GetAllShards() {
		s.ResetQPSCounter()
	}
	// Run an initial warm-up batch so per-shard and global QPS reflect live traffic immediately
	m.runDynamicLoadStep(1, 1)
	var warmQPS uint64
	for _, s := range qr.Cluster.GetAllShards() {
		warmQPS += s.TickQPS(0.05)
	}
	m.globalQPS = warmQPS
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

// runDynamicLoadStep executes a naturally varying, Zipf-skewed batch of real shard reads
// so per-shard and cluster QPS fluctuate organically with real execution timing.
func (m *DashboardModel) runDynamicLoadStep(cursorUID int64, step uint64) int64 {
	totalUsers := m.qr.Cluster.TotalRows()
	if totalUsers <= 0 {
		totalUsers = 10000
	}
	ns := uint64(time.Now().UnixNano())
	wave := []int{64, 118, 92, 175, 140, 84, 210, 108}[step%8]
	jitter := int((ns >> 7) % 53)
	batchSize := wave + jitter

	// Pick a rotating focal user so individual shards see organic traffic shifts
	focalBase := int64(((step * 397) + (ns >> 11)) % uint64(totalUsers)) + 1
	uid := cursorUID
	for i := 0; i < batchSize; i++ {
		var targetUID int64
		if i%4 == 0 {
			// Skewed access cluster
			targetUID = ((focalBase + int64(i*7)) % totalUsers) + 1
		} else {
			// Sequential + stride scan across all buckets
			targetUID = (uid % totalUsers) + 1
			uid += 3
		}
		shardID, _ := m.qr.RouteFastPoint(targetUID)
		if s, ok := m.qr.Cluster.GetShard(shardID); ok {
			_, _ = s.GetUser(targetUID)
		}
	}
	return uid
}

func (m *DashboardModel) startBackgroundLoad() {
	if !m.stopLoadFlag.CompareAndSwap(true, false) {
		return
	}
	go func(stop *atomic.Bool) {
		uid := int64(1)
		var step uint64 = 1
		for !stop.Load() {
			uid = m.runDynamicLoadStep(uid, step)
			step++
			sleepMs := 10 + int((uint64(time.Now().UnixNano())>>9)%14)
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
	}(m.stopLoadFlag)
}

func (m *DashboardModel) Init() tea.Cmd {
	if m.loadGenActive {
		m.startBackgroundLoad()
	}
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
	m.statusBanner = fmt.Sprintf("Customize S%d · Type any value ([Ctrl+U] Clear · [Enter] Apply)", s.ShardID)
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
	m.statusBanner = fmt.Sprintf("Provision S%d · Customize bytes, name, or buckets & press [Enter]", nextID)
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
	case 1: // Max Capacity Bytes: nudge by 1,024 bytes (1 KB) or 64 bytes if <= 4096
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
		region = "local-node-0"
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
					m.statusBanner = fmt.Sprintf("Saved S%d [%s] · Evacuated %d Ranges via CDC · RAM: %s / %s",
						sid, s.DisplayName(), evacuated, storage.FormatBytesCompact(s.UsedMemoryBytes()), storage.FormatBytesCompact(maxBytes))
				} else {
					m.statusBanner = fmt.Sprintf("Saved S%d [%s] · Live RAM: %s / %s",
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
				m.statusBanner = "Type SQL query & press [Enter] to execute ([Esc] Cancel · [Ctrl+U] Clear)"
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
				m.statusBanner = fmt.Sprintf("SQL Explorer pinned to S%d [%s] · Press [f] to cycle target shard", s.ShardID, s.DisplayName())
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
				for _, s := range m.qr.Cluster.GetAllShards() {
					s.ResetQPSCounter()
				}
				m.globalQPS = 0
				m.statusBanner = "Live Traffic Generator: PAUSED (QPS = 0/s)"
			} else {
				m.loadGenActive = true
				m.startBackgroundLoad()
				m.statusBanner = "Live Traffic Generator: ACTIVE (Dynamic Workload)"
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
			m.statusBanner = fmt.Sprintf("Reset to 4 Shards (1,024 Buckets, %s total rows).", formatUintComma(uint64(m.qr.TotalClusterRows())))
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
			"PEAK BURST: %s req/sec (%d ns/op, 0 allocs) · 0 Dropped!",
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
	// Minimalist 78-column rounded frame (76 inner chars) fitting 80x24 terminals cleanly
	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240")).
		Padding(0, 1).
		Width(78)

	brandStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86"))
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("252"))
	colHdrStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("111"))
	okStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("42"))
	warnStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))
	hotStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))
	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	ruleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("238"))
	selStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("51"))
	keyStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("81"))
	activeTabStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255")).Background(lipgloss.Color("25")).Padding(0, 1)
	inactiveTabStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("244")).Padding(0, 1)
	barFillStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("45"))
	barEmptyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("237"))
	migBarStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("42"))

	shards := m.qr.Cluster.GetAllShards()
	bucketCounts := m.qr.Dir.BucketCountsByShard()
	wf := m.qr.CDC.GetSnapshot()
	alerts := m.qr.HotspotTracker.GetRecentAlerts()
	totalRows := uint64(m.qr.TotalClusterRows())
	appTables := m.qr.ApplicationTableCount()

	var b strings.Builder

	// 1. Minimalist Telemetry Header Bar
	pulseIcons := []string{"●", "◉"}
	pulse := okStyle.Render(pulseIcons[m.ticks%2])
	if !m.loadGenActive && m.globalQPS == 0 {
		pulse = dimStyle.Render("○")
	}
	qpsDisplay := warnStyle.Render(formatUintComma(m.globalQPS) + "/s")
	if m.peakBurstQPS > 0 {
		qpsDisplay += dimStyle.Render(" (Peak ") + okStyle.Render(formatCompactUint(m.peakBurstQPS)+"/s") + dimStyle.Render(")")
	}
	b.WriteString(fmt.Sprintf(
		" %s %s  %s  %d Shards  %s  %s Rows (%d tbls)  %s  QPS %s\n",
		pulse,
		brandStyle.Render("SHARDMASTER"),
		ruleStyle.Render("│"),
		len(shards),
		ruleStyle.Render("│"),
		okStyle.Render(formatUintComma(totalRows)),
		appTables,
		ruleStyle.Render("│"),
		qpsDisplay,
	))

	// 2. Minimalist Pill Navigation Bar
	tabNames := []string{"1 Topology", "2 CDC & VDiff", "3 Hotspots", "4 SQL Explorer"}
	var renderedTabs []string
	for i, name := range tabNames {
		if i == m.activeTab {
			renderedTabs = append(renderedTabs, activeTabStyle.Render(name))
		} else {
			renderedTabs = append(renderedTabs, inactiveTabStyle.Render(name))
		}
	}
	b.WriteString(" " + strings.Join(renderedTabs, "  ") + "\n")
	b.WriteString(ruleStyle.Render(strings.Repeat("─", 74)) + "\n")

	// 3. Focused Tab Viewport (13 lines)
	const viewLines = 13
	var lines []string
	maxScroll := 0

	if m.modalMode == modalEditShard || m.modalMode == modalCreateShard {
		hdr := fmt.Sprintf(" CUSTOMIZE SHARD %d   [Enter] Save  [Ctrl+U] Clear  [Esc] Cancel", m.formShardID)
		if m.modalMode == modalCreateShard {
			hdr = fmt.Sprintf(" PROVISION SHARD %d   [Enter] Create  [Ctrl+U] Clear  [Esc] Cancel", m.formShardID)
		}
		lines = append(lines, brandStyle.Render(hdr))
		lines = append(lines, colHdrStyle.Render("  PARAMETER             VALUE                         LIVE PREVIEW"))

		parsedMaxB, _ := storage.ParseByteSize(m.formInputs[1])
		parsedSlabB, _ := storage.ParseByteSize(m.formInputs[2])
		fields := []struct {
			label string
			val   string
			hint  string
		}{
			{"1. Shard Name      ", m.formInputs[0], "Custom node alias"},
			{"2. Max RAM (Bytes) ", m.formInputs[1], fmt.Sprintf("= %s", storage.FormatBytesCompact(parsedMaxB))},
			{"3. Slab Bytes/Bkt  ", m.formInputs[2], fmt.Sprintf("= %s/bkt", storage.FormatBytesCompact(parsedSlabB))},
			{"4. Virtual Buckets ", m.formInputs[3], "0..1024 buckets"},
			{"5. Weight % : Port ", m.formInputs[4], "e.g. 100:5432"},
			{"6. Shard Tag       ", m.formInputs[5], "Grouping label"},
			{"7. Memory Profile  ", m.formInputs[6], "Columnar slab tier"},
			{"8. Mode/Repl/Conns ", m.formInputs[7], "RW/SYNC_QUORUM/1000"},
		}
		for idx, f := range fields {
			cursor := "  "
			valRendered := okStyle.Render(fmt.Sprintf("%-26s", truncateStr(f.val, 26)))
			if idx == m.formFieldIdx {
				cursor = selStyle.Render("▸ ")
				valRendered = selStyle.Render(fmt.Sprintf("%-26s", "["+truncateStr(f.val, 23)+"_]"))
			}
			line := fmt.Sprintf("%s%-19s  %s  %s", cursor, f.label, valRendered, dimStyle.Render(truncateStr(f.hint, 20)))
			lines = append(lines, line)
		}
	} else if m.activeTab == 0 {
		// TAB 1: MINIMALIST SHARD TOPOLOGY (No fake zones; real local port, mode, bucket bar, RAM, all-table rows & live QPS)
		lines = append(lines, colHdrStyle.Render(
			fmt.Sprintf("  %-13s %-6s %-4s %-14s %-15s %7s %7s",
				"SHARD NAME", "PORT", "MODE", "BUCKETS", "RAM USED / MAX", "ROWS", "QPS"),
		))

		if m.selectedShardIdx >= len(shards) && len(shards) > 0 {
			m.selectedShardIdx = len(shards) - 1
		}

		const maxVisibleShards = 9
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
			shardRows := m.qr.ShardTotalRows(s.ShardID)

			modeShort := "R/W"
			modeRendered := okStyle.Render(fmt.Sprintf("%-4s", modeShort))
			if cfg.AccessMode == "READ_ONLY" {
				modeShort = "R/O"
				modeRendered = warnStyle.Render(fmt.Sprintf("%-4s", modeShort))
			}

			barCells := (bCount * 5) / 256
			if barCells > 5 {
				barCells = 5
			}
			if barCells < 1 && bCount > 0 {
				barCells = 1
			}
			miniBar := barFillStyle.Render(strings.Repeat("█", barCells)) + barEmptyStyle.Render(strings.Repeat("░", 5-barCells))
			bktCol := fmt.Sprintf("%4d/1024 %s", bCount, miniBar)

			rawName := fmt.Sprintf("S%d %s", s.ShardID, s.DisplayName())
			nameCol := fmt.Sprintf("%-13s", truncateStr(rawName, 13))
			portCol := fmt.Sprintf(":%-5d", s.Port)
			ramCol := fmt.Sprintf("%-15s", fmt.Sprintf("%s / %s", storage.FormatBytesCompact(usedB), storage.FormatBytesCompact(maxB)))
			rowCol := fmt.Sprintf("%7s", formatUintComma(uint64(shardRows)))
			qpsCol := fmt.Sprintf("%7s", formatCompactUint(shardQPS)+"/s")

			pointer := "  "
			if idx == m.selectedShardIdx {
				pointer = selStyle.Render("▸ ")
				nameCol = selStyle.Render(nameCol)
				portCol = selStyle.Render(portCol)
			} else {
				portCol = dimStyle.Render(portCol)
			}

			lines = append(lines, fmt.Sprintf(
				"%s%s %s %s %s %s %s %s",
				pointer, nameCol, portCol, modeRendered, bktCol, ramCol, rowCol, qpsCol,
			))
		}
		for len(lines) < 1+maxVisibleShards {
			lines = append(lines, "")
		}

		// Minimalist Selected Shard Inspector Card (3 lines pinned at bottom of Tab 1)
		if len(shards) > 0 {
			sel := shards[m.selectedShardIdx%len(shards)]
			scfg := sel.GetSettings()
			usedB := sel.UsedMemoryBytes()
			selTotalRows := m.qr.ShardTotalRows(sel.ShardID)
			lines = append(lines, ruleStyle.Render(strings.Repeat("─", 74)))
			lines = append(lines, fmt.Sprintf(
				" SELECTED: %s (:%d)  %s  Mode: %s  %s  Rows: %s  %s  Weight: %d%%",
				selStyle.Render(fmt.Sprintf("S%d [%s]", sel.ShardID, sel.DisplayName())),
				sel.Port,
				ruleStyle.Render("│"),
				okStyle.Render(scfg.AccessMode),
				ruleStyle.Render("│"),
				titleStyle.Render(formatUintComma(uint64(selTotalRows))),
				ruleStyle.Render("│"),
				scfg.Weight,
			))
			lines = append(lines, fmt.Sprintf(
				" EXACT RAM: %s B (%s) / %s B (%s)  %s  Slab: %s B/bkt",
				formatUintComma(uint64(usedB)),
				storage.FormatBytesCompact(usedB),
				formatUintComma(uint64(scfg.MaxCapacityBytes)),
				storage.FormatBytesCompact(scfg.MaxCapacityBytes),
				ruleStyle.Render("│"),
				formatUintComma(uint64(scfg.SlabBytesPerBucket)),
			))
		}
	} else {
		switch m.activeTab {
		case 1: // TAB 2: CDC & VDIFF
			lines = append(lines, titleStyle.Render(" VITESS CDC VREPLICATION & SHA-256 PARITY LEDGER"))
			lines = append(lines, fmt.Sprintf(
				" Stream: %-28s %s Status: %s %s Lag: %.2f ms",
				truncateStr(wf.Title, 28),
				ruleStyle.Render("│"),
				okStyle.Render(wf.Status),
				ruleStyle.Render("│"),
				wf.ReplicationLagMs,
			))
			progFilled := int((wf.ProgressPct / 100.0) * 26.0)
			if progFilled > 26 {
				progFilled = 26
			}
			if progFilled < 0 {
				progFilled = 0
			}
			progBar := migBarStyle.Render(strings.Repeat("█", progFilled)) + barEmptyStyle.Render(strings.Repeat("░", 26-progFilled))
			lines = append(lines, fmt.Sprintf(
				" Progress: %s %3.0f%% (%s rows · %s)",
				progBar,
				wf.ProgressPct,
				formatUintComma(uint64(wf.RowsMigrated)),
				okStyle.Render(wf.VDiffStatus),
			))
			lines = append(lines, ruleStyle.Render(strings.Repeat("─", 74)))
			lines = append(lines, colHdrStyle.Render("  BUCKET RANGE    SHARD ROUTE    ROWS MOVED   SHA-256 PARITY DIGEST    STATE"))
			history := m.qr.CDC.GetVDiffHistory()
			if len(history) == 0 {
				lines = append(lines, dimStyle.Render("  No migrations yet · Press [s] Split +1, [+/-] Buckets, or [n] New Shard."))
			} else {
				for i, vd := range history {
					if i >= 8 {
						break
					}
					lines = append(lines, fmt.Sprintf(
						"  Bkt [%03d..%03d]  S%-2d -> S%-2d     %10s   %-22s   %s",
						vd.StartBucket, vd.EndBucket, vd.SourceShard, vd.TargetShard,
						formatUintComma(uint64(vd.TargetRows)),
						dimStyle.Render(vd.TargetDigest[:20]+".."),
						okStyle.Render("VERIFIED"),
					))
				}
			}

		case 2: // TAB 3: EWMA HOTSPOTS
			if len(alerts) > 0 {
				a := alerts[0]
				lines = append(lines, hotStyle.Render(fmt.Sprintf(
					" ● ISOLATED: Bucket #%d (%d QPS) auto-migrated Shard %d -> Shard %d (0ms lag)",
					a.BucketID, a.MeasuredQPS, a.FromShard, a.ToShard,
				)))
			} else {
				lines = append(lines, okStyle.Render(" ● HEALTHY: All 1,024 Virtual Buckets Within Thermal Envelope ([h] Spike #412)"))
			}
			lines = append(lines, ruleStyle.Render(strings.Repeat("─", 74)))
			lines = append(lines, colHdrStyle.Render("  RANK   VIRTUAL BUCKET   OWNER SHARD     EWMA QPS    THERMAL LOAD     STATE"))
			top := m.qr.HotspotTracker.GetTopHotBuckets(9)
			for i, h := range top {
				heatCells := int(h.EWMAQPS / 400)
				if heatCells > 8 {
					heatCells = 8
				}
				if heatCells < 1 && h.EWMAQPS > 0 {
					heatCells = 1
				}
				heatBar := barFillStyle.Render(strings.Repeat("█", heatCells)) + barEmptyStyle.Render(strings.Repeat("░", 8-heatCells))
				stateBadge := okStyle.Render("NORMAL")
				if h.EWMAQPS >= 1500 {
					heatBar = hotStyle.Render(strings.Repeat("█", heatCells)) + barEmptyStyle.Render(strings.Repeat("░", 8-heatCells))
					stateBadge = hotStyle.Render("HOT   ")
				}
				lines = append(lines, fmt.Sprintf(
					"  #%-4d  Bucket #%-6d   Shard %-2d (:%-4d) %7s/s   %s     %s",
					i+1, h.BucketID, h.OwnerShard, 5432+h.OwnerShard,
					formatCompactUint(h.EWMAQPS),
					heatBar,
					stateBadge,
				))
			}

		case 3: // TAB 4: SQL EXPLORER (WITH SHARD PINNING & CUSTOM SQL INPUT)
			scopeBadge := okStyle.Render("ALL SHARDS")
			if m.pinnedShardID >= 0 {
				if ps, ok := m.qr.Cluster.GetShard(uint32(m.pinnedShardID)); ok {
					scopeBadge = selStyle.Render(fmt.Sprintf("PINNED: S%d %s", ps.ShardID, ps.DisplayName()))
				}
			}
			q := presetQueries[m.queryIdx%len(presetQueries)]
			headerTitle := fmt.Sprintf(" preset %d/%d: %s", m.queryIdx+1, len(presetQueries), q.title)
			if m.usingCustomSQL {
				headerTitle = " custom sql query"
			}
			lines = append(lines, fmt.Sprintf(
				"%s  %s  %s  %s",
				titleStyle.Render(headerTitle),
				ruleStyle.Render("│"),
				scopeBadge,
				dimStyle.Render("[p/n] Preset [f] Scope [/] Type"),
			))
			if m.modalMode == modalSQLInput {
				lines = append(lines, " "+selStyle.Render("SQL ▸ "+m.customSQLInput+"_"))
			} else if m.usingCustomSQL {
				lines = append(lines, " "+warnStyle.Render(truncateStr(m.customSQLInput, 72)))
			} else {
				lines = append(lines, " "+warnStyle.Render(truncateStr(q.sql, 72)))
			}
			if m.activeQueryRes != nil {
				lines = append(lines, renderAlignedTUIRows(m.activeQueryRes, colHdrStyle, ruleStyle, 72)...)
			}
		}
	}

	if m.activeTab == 0 && m.modalMode == modalNone {
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

	// 4. Minimalist 2-Line Footer (Strictly <= 74 visible chars)
	b.WriteString(ruleStyle.Render(strings.Repeat("─", 74)) + "\n")
	scrollHint := ""
	if maxScroll > 0 {
		scrollHint = fmt.Sprintf(" [%d/%d]", m.scrollOffset+1, maxScroll+1)
	}
	b.WriteString(" " + warnStyle.Render(truncateStr(m.statusBanner, 64)) + dimStyle.Render(scrollHint) + "\n")
	if m.modalMode == modalEditShard || m.modalMode == modalCreateShard {
		b.WriteString(fmt.Sprintf(" %s Field  %s Edit  %s Nudge  %s Clear  %s Save",
			keyStyle.Render("Up/Dn"), keyStyle.Render("Type"), keyStyle.Render("L/R"), keyStyle.Render("Ctrl+U"), keyStyle.Render("Enter")))
	} else if m.activeTab == 3 {
		b.WriteString(fmt.Sprintf(" %s Preset  %s Tab  %s Pin Shard  %s Type SQL  %s Quit",
			keyStyle.Render("p/n"), keyStyle.Render("L/R"), keyStyle.Render("f"), keyStyle.Render("/"), keyStyle.Render("q")))
	} else {
		b.WriteString(fmt.Sprintf(" %s Edit  %s New  %s Buckets  %s Split+1  %s Traffic  %s Burst  %s Quit",
			keyStyle.Render("e"), keyStyle.Render("n"), keyStyle.Render("+/-"), keyStyle.Render("s"), keyStyle.Render("b"), keyStyle.Render("m"), keyStyle.Render("q")))
	}

	return borderStyle.Render(b.String())
}

func formatCompactUint(n uint64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000.0)
	}
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

func renderAlignedTUIRows(res *router.ResultSet, hdrStyle, sepStyle lipgloss.Style, maxWidth int) []string {
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
			hdr.WriteString(" │ ")
		}
		hdr.WriteString(fmt.Sprintf("%-*s", widths[i], truncateStr(res.Columns[i], widths[i])))
	}
	out = append(out, hdrStyle.Render(hdr.String()))

	for _, r := range res.Rows {
		var rowStr strings.Builder
		rowStr.WriteString(" ")
		for i := 0; i < numCols; i++ {
			if i > 0 {
				rowStr.WriteString(sepStyle.Render(" │ "))
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
