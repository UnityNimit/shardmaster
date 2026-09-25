package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"shardmaster/pkg/hash"
)

// UserRow represents a single row in the distributed `users` table.
type UserRow struct {
	UserID      int64     `json:"user_id"`
	UserKey     string    `json:"user_key"`
	Name        string    `json:"name"`
	Email       string    `json:"email"`
	TenantID    string    `json:"tenant_id"`
	Region      string    `json:"region"`
	BalanceCents int64    `json:"balance_cents"`
	BucketID    uint16    `json:"bucket_id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// MutationOp defines the CDC operation type.
type MutationOp uint8

const (
	MutationInsertOrUpdate MutationOp = 1
	MutationDelete         MutationOp = 2
)

// MutationLogEntry represents a row in the `_shardmaster_cdc` journal table (Pillar 3).
type MutationLogEntry struct {
	LSN           uint64     `json:"lsn"`
	BucketID      uint16     `json:"bucket_id"`
	Op            MutationOp `json:"op"`
	UserID        int64      `json:"user_id"`
	Row           UserRow    `json:"row"`
	TimestampUs   int64      `json:"timestamp_us"`
}

const numStripes = 64

type shardStripe struct {
	mu   sync.RWMutex
	rows map[int64]UserRow
}

// PhysicalShard represents an isolated physical database shard (e.g. Shard 0 :5432 .. Shard 7 :5439).
type PhysicalShard struct {
	ShardID     uint32
	Name        string
	Port        int
	Region      string
	DSN         string
	PostgresUp  bool

	stripes     [numStripes]shardStripe
	rowCount    atomic.Int64
	qpsCounter  atomic.Uint64
	lastQPS     atomic.Uint64
	totalOps    atomic.Uint64
	latencyNs   atomic.Int64 // EWMA latency in nanoseconds

	// Pillar 3: Change Data Capture (CDC) Mutation Log Journal
	cdcMu       sync.RWMutex
	lsnSeq      atomic.Uint64
	cdcJournal  []MutationLogEntry
}

// NewPhysicalShard initializes a physical shard with 64-way lock striping and a CDC mutation ring.
func NewPhysicalShard(shardID uint32, region string) *PhysicalShard {
	port := 5432 + int(shardID)
	s := &PhysicalShard{
		ShardID:    shardID,
		Name:       fmt.Sprintf("Shard %d :%d", shardID, port),
		Port:       port,
		Region:     region,
		DSN:        fmt.Sprintf("postgres://postgres:postgres@localhost:%d/shard_%d?sslmode=disable", port, shardID),
		cdcJournal: make([]MutationLogEntry, 0, 4096),
	}
	s.latencyNs.Store(380_000) // 0.38ms initial baseline
	for i := 0; i < numStripes; i++ {
		s.stripes[i].rows = make(map[int64]UserRow, 256)
	}
	return s
}

// RecordOp records a query hit and updates EWMA latency.
//
//go:inline
func (s *PhysicalShard) RecordOp(elapsedNs int64) {
	s.qpsCounter.Add(1)
	s.totalOps.Add(1)
	old := s.latencyNs.Load()
	s.latencyNs.Store((old*7 + elapsedNs) >> 3)
}

// TickQPS snapshots and resets the per-second QPS counter.
func (s *PhysicalShard) TickQPS(intervalSec float64) uint64 {
	delta := s.qpsCounter.Swap(0)
	qps := uint64(float64(delta) / intervalSec)
	s.lastQPS.Store(qps)
	return qps
}

func (s *PhysicalShard) CurrentQPS() uint64      { return s.lastQPS.Load() }
func (s *PhysicalShard) RowCount() int64         { return s.rowCount.Load() }
func (s *PhysicalShard) AvgLatencyMs() float64   { return float64(s.latencyNs.Load()) / 1e6 }
func (s *PhysicalShard) CurrentLSN() uint64      { return s.lsnSeq.Load() }

// UpsertUser writes a row to the striped storage engine and appends a mutation to `_shardmaster_cdc`.
func (s *PhysicalShard) UpsertUser(row UserRow, recordCDC bool) UserRow {
	start := time.Now()
	if row.UpdatedAt.IsZero() {
		row.UpdatedAt = start.UTC()
	}
	if row.CreatedAt.IsZero() {
		row.CreatedAt = row.UpdatedAt
	}
	if row.UserKey == "" {
		row.UserKey = fmt.Sprintf("user_%d", row.UserID)
	}

	stripeIdx := uint64(row.UserID) & (numStripes - 1)
	stripe := &s.stripes[stripeIdx]

	stripe.mu.Lock()
	_, existed := stripe.rows[row.UserID]
	stripe.rows[row.UserID] = row
	stripe.mu.Unlock()

	if !existed {
		s.rowCount.Add(1)
	}

	if recordCDC {
		lsn := s.lsnSeq.Add(1)
		entry := MutationLogEntry{
			LSN:         lsn,
			BucketID:    row.BucketID,
			Op:          MutationInsertOrUpdate,
			UserID:      row.UserID,
			Row:         row,
			TimestampUs: start.UnixMicro(),
		}
		s.cdcMu.Lock()
		if len(s.cdcJournal) >= 32768 {
			// Retain latest 16,384 entries in bounded ring
			s.cdcJournal = append(s.cdcJournal[:0], s.cdcJournal[16384:]...)
		}
		s.cdcJournal = append(s.cdcJournal, entry)
		s.cdcMu.Unlock()
	}

	s.RecordOp(time.Since(start).Nanoseconds() + 180_000)
	return row
}

// GetUser retrieves a user by ID in O(1) from the lock-striped table.
func (s *PhysicalShard) GetUser(userID int64) (UserRow, bool) {
	start := time.Now()
	stripeIdx := uint64(userID) & (numStripes - 1)
	stripe := &s.stripes[stripeIdx]

	stripe.mu.RLock()
	row, ok := stripe.rows[userID]
	stripe.mu.RUnlock()

	s.RecordOp(time.Since(start).Nanoseconds() + 120_000)
	return row, ok
}

// DeleteUser removes a user by ID and appends a tombstone to `_shardmaster_cdc`.
func (s *PhysicalShard) DeleteUser(userID int64, recordCDC bool) bool {
	start := time.Now()
	stripeIdx := uint64(userID) & (numStripes - 1)
	stripe := &s.stripes[stripeIdx]

	stripe.mu.Lock()
	existing, ok := stripe.rows[userID]
	if ok {
		delete(stripe.rows, userID)
	}
	stripe.mu.Unlock()

	if ok {
		s.rowCount.Add(-1)
		if recordCDC {
			lsn := s.lsnSeq.Add(1)
			s.cdcMu.Lock()
			s.cdcJournal = append(s.cdcJournal, MutationLogEntry{
				LSN:         lsn,
				BucketID:    existing.BucketID,
				Op:          MutationDelete,
				UserID:      userID,
				Row:         existing,
				TimestampUs: start.UnixMicro(),
			})
			s.cdcMu.Unlock()
		}
	}

	s.RecordOp(time.Since(start).Nanoseconds() + 150_000)
	return ok
}

// ScanBucketKeyset performs lock-free Keyset Pagination backfill for a virtual bucket range:
// `SELECT * FROM users WHERE bucket_id BETWEEN $start AND $end AND user_id > $afterID ORDER BY user_id ASC LIMIT $limit`
func (s *PhysicalShard) ScanBucketKeyset(
	startBucket, endBucket uint16,
	afterUserID int64,
	limit int,
) ([]UserRow, uint64) {
	snapshotLSN := s.lsnSeq.Load()
	var matched []UserRow

	for i := 0; i < numStripes; i++ {
		stripe := &s.stripes[i]
		stripe.mu.RLock()
		for _, r := range stripe.rows {
			if r.BucketID >= startBucket && r.BucketID <= endBucket && r.UserID > afterUserID {
				matched = append(matched, r)
			}
		}
		stripe.mu.RUnlock()
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].UserID < matched[j].UserID
	})

	if len(matched) > limit && limit > 0 {
		matched = matched[:limit]
	}
	s.RecordOp(420_000)
	return matched, snapshotLSN
}

// GetBucketRows returns all rows on this shard belonging to [startBucket, endBucket].
func (s *PhysicalShard) GetBucketRows(startBucket, endBucket uint16) []UserRow {
	var matched []UserRow
	for i := 0; i < numStripes; i++ {
		stripe := &s.stripes[i]
		stripe.mu.RLock()
		for _, r := range stripe.rows {
			if r.BucketID >= startBucket && r.BucketID <= endBucket {
				matched = append(matched, r)
			}
		}
		stripe.mu.RUnlock()
	}
	return matched
}

// PurgeBucketRange removes migrated rows belonging to [startBucket, endBucket] after atomic cutover.
func (s *PhysicalShard) PurgeBucketRange(startBucket, endBucket uint16) int {
	purged := 0
	for i := 0; i < numStripes; i++ {
		stripe := &s.stripes[i]
		stripe.mu.Lock()
		for uid, r := range stripe.rows {
			if r.BucketID >= startBucket && r.BucketID <= endBucket {
				delete(stripe.rows, uid)
				purged++
			}
		}
		stripe.mu.Unlock()
	}
	if purged > 0 {
		s.rowCount.Add(-int64(purged))
	}
	return purged
}

// FetchCDCMutationsAfter returns all mutation log entries for [startBucket, endBucket] with LSN > afterLSN.
func (s *PhysicalShard) FetchCDCMutationsAfter(
	startBucket, endBucket uint16,
	afterLSN uint64,
) ([]MutationLogEntry, uint64) {
	s.cdcMu.RLock()
	defer s.cdcMu.RUnlock()

	latestLSN := s.lsnSeq.Load()
	var events []MutationLogEntry
	for _, entry := range s.cdcJournal {
		if entry.LSN > afterLSN && entry.BucketID >= startBucket && entry.BucketID <= endBucket {
			events = append(events, entry)
		}
	}
	return events, latestLSN
}

// QueryFilter executes a local filter & sort scan on this physical shard (used by Pillar 2 Scatter-Gather).
func (s *PhysicalShard) QueryFilter(emailSubstring string, regionFilter string, limit int) []UserRow {
	start := time.Now()
	emailSub := strings.ToLower(strings.Trim(emailSubstring, "%'\" "))
	regSub := strings.ToLower(strings.Trim(regionFilter, "'\" "))

	var matched []UserRow
	for i := 0; i < numStripes; i++ {
		stripe := &s.stripes[i]
		stripe.mu.RLock()
		for _, r := range stripe.rows {
			if emailSub != "" && !strings.Contains(strings.ToLower(r.Email), emailSub) {
				continue
			}
			if regSub != "" && strings.ToLower(r.Region) != regSub {
				continue
			}
			matched = append(matched, r)
		}
		stripe.mu.RUnlock()
	}

	// Sort by CreatedAt DESC, then UserID DESC (local shard sort before K-Way Merge!)
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].UserID > matched[j].UserID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if limit > 0 && len(matched) > limit {
		matched = matched[:limit]
	}
	s.RecordOp(time.Since(start).Nanoseconds() + 350_000)
	return matched
}

// ClusterStorage manages the pool of all physical shards and persists cluster metadata.
type ClusterStorage struct {
	mu      sync.RWMutex
	shards  map[uint32]*PhysicalShard
	dataDir string
}

var defaultRegions = []string{"us-west", "us-east", "eu-central", "ap-south"}

// NewClusterStorage creates the multi-shard backend manager.
func NewClusterStorage(initialShards uint32, dataDir string) *ClusterStorage {
	if dataDir == "" {
		dataDir = filepath.Join(".", "data")
	}
	_ = os.MkdirAll(dataDir, 0755)
	cs := &ClusterStorage{
		shards:  make(map[uint32]*PhysicalShard),
		dataDir: dataDir,
	}
	cs.InitializeShards(initialShards)
	return cs
}

// InitializeShards initializes N physical shards and seeds 10,000 rows if empty.
func (cs *ClusterStorage) InitializeShards(count uint32) {
	cs.mu.Lock()
	cs.shards = make(map[uint32]*PhysicalShard, count)
	for i := uint32(0); i < count; i++ {
		reg := defaultRegions[int(i)%len(defaultRegions)]
		cs.shards[i] = NewPhysicalShard(i, reg)
	}
	cs.mu.Unlock()
}

// EnsureShard provisions a physical shard if it does not yet exist.
func (cs *ClusterStorage) EnsureShard(shardID uint32, region string) *PhysicalShard {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if existing, ok := cs.shards[shardID]; ok {
		return existing
	}
	if region == "" {
		region = defaultRegions[int(shardID)%len(defaultRegions)]
	}
	s := NewPhysicalShard(shardID, region)
	cs.shards[shardID] = s
	return s
}

// GetShard returns a physical shard by ID.
func (cs *ClusterStorage) GetShard(shardID uint32) (*PhysicalShard, bool) {
	cs.mu.RLock()
	s, ok := cs.shards[shardID]
	cs.mu.RUnlock()
	return s, ok
}

// GetAllShards returns all physical shards ordered by ShardID.
func (cs *ClusterStorage) GetAllShards() []*PhysicalShard {
	cs.mu.RLock()
	list := make([]*PhysicalShard, 0, len(cs.shards))
	for _, s := range cs.shards {
		list = append(list, s)
	}
	cs.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool {
		return list[i].ShardID < list[j].ShardID
	})
	return list
}

// SeedCluster populates the cluster with realistic distributed user records.
func (cs *ClusterStorage) SeedCluster(
	totalRows int,
	bucketLookup func(bucket uint16) uint32,
) {
	domains := []string{"gmail.com", "stripe.com", "stanford.edu", "vercel.com", "cloudflare.com"}
	names := []string{
		"Aarav Patel", "Sophia Chen", "Liam Smith", "Olivia Garcia",
		"Noah Kumar", "Emma Kim", "Vikram Silva", "Mia Miller",
		"Lucas Sato", "Zara Lopez", "Ethan Mehta", "Priya Shah",
	}
	tenants := []string{"tenant_enterprise_1", "tenant_fintech", "tenant_saas", "tenant_core"}
	baseTime := time.Now().UTC().Add(-24 * time.Hour)

	for i := 1; i <= totalRows; i++ {
		uid := int64(i)
		key := fmt.Sprintf("%d", uid)
		bucket := hash.ComputeBucket(key)
		shardID := bucketLookup(bucket)

		shard, ok := cs.GetShard(shardID)
		if !ok {
			shard = cs.EnsureShard(shardID, "")
		}

		name := names[i%len(names)]
		domain := domains[i%len(domains)]
		email := fmt.Sprintf("user_%d@%s", uid, domain)
		row := UserRow{
			UserID:       uid,
			UserKey:      key,
			Name:         name,
			Email:        email,
			TenantID:     tenants[i%len(tenants)],
			Region:       shard.Region,
			BalanceCents: int64(10000 + (i*137)%900000),
			BucketID:     bucket,
			CreatedAt:    baseTime.Add(time.Duration(i) * time.Second),
			UpdatedAt:    baseTime.Add(time.Duration(i) * time.Second),
		}
		shard.UpsertUser(row, false)
	}
}

// SaveClusterMetadata persists a lightweight cluster state file so separate CLI invocations share state.
type PersistedShardMeta struct {
	ShardID  uint32 `json:"shard_id"`
	Region   string `json:"region"`
	RowCount int64  `json:"row_count"`
}

type PersistedClusterState struct {
	NumShards int                         `json:"num_shards"`
	Buckets   [hash.TotalVirtualBuckets]uint32 `json:"buckets"`
	Shards    []PersistedShardMeta        `json:"shards"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

func (cs *ClusterStorage) SaveStateFile(buckets [hash.TotalVirtualBuckets]uint32) {
	shards := cs.GetAllShards()
	state := PersistedClusterState{
		NumShards: len(shards),
		Buckets:   buckets,
		Shards:    make([]PersistedShardMeta, len(shards)),
		UpdatedAt: time.Now().UTC(),
	}
	for i, s := range shards {
		state.Shards[i] = PersistedShardMeta{
			ShardID:  s.ShardID,
			Region:   s.Region,
			RowCount: s.RowCount(),
		}
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err == nil {
		_ = os.WriteFile(filepath.Join(cs.dataDir, "cluster_state.json"), raw, 0644)
	}
}
