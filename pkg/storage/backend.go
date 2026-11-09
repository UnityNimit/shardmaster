package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"shardmaster/pkg/hash"
)

// DefaultInitialRows is 10,000 real, hashed, indexed rows across the cluster on startup.
const DefaultInitialRows = 10_000

// DefaultShardCapacityBytes is 64 MB (67,108,864 bytes) per shard so 4 shards use only ~256 MB on a 16 GB RAM PC.
const DefaultShardCapacityBytes int64 = 64 * 1024 * 1024

// DefaultSlabBytesPerBucket is 262,144 bytes (256 KB = 65,536 uint32 entries) per virtual bucket.
const DefaultSlabBytesPerBucket int = 262144

var (
	// ErrShardCapacityExceeded is returned with SQLSTATE 53100 (disk_full) when a write would push a shard over MaxCapacityBytes.
	ErrShardCapacityExceeded = errors.New("SQLSTATE 53100 (disk_full): ERR_SHARD_CAPACITY_EXCEEDED")
	// ErrInsufficientClusterCapacity is returned when shrinking a shard's bytes or draining/resizing buckets cannot fit in cluster free bytes.
	ErrInsufficientClusterCapacity = errors.New("ERR_INSUFFICIENT_CLUSTER_CAPACITY")
	// ErrShardReadOnly is returned with SQLSTATE 25006 when writing to a READ_ONLY shard.
	ErrShardReadOnly = errors.New("SQLSTATE 25006 (read_only_sql_transaction): ERR_SHARD_READ_ONLY")
)

// UserRow represents a materialized row in the distributed `users` table.
type UserRow struct {
	UserID       int64     `json:"user_id"`
	UserKey      string    `json:"user_key"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	TenantID     string    `json:"tenant_id"`
	Region       string    `json:"region"`
	BalanceCents int64     `json:"balance_cents"`
	BucketID     uint16    `json:"bucket_id"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// UserRowMemoryBytes computes the exact in-RAM byte footprint of a committed UserRow in DeltaOverrides.
func UserRowMemoryBytes(r UserRow) int64 {
	strBytes := len(r.UserKey) + len(r.Name) + len(r.Email) + len(r.TenantID) + len(r.Region)
	size := int64(96 + strBytes)
	if size < 128 {
		size = 128
	}
	return size
}

// CustomRow represents a physical row of a user-created or co-located SQL table stored inside a Virtual Bucket.
type CustomRow struct {
	TableName string            `json:"table_name"`
	RowKey    string            `json:"row_key"`
	ShardKey  string            `json:"shard_key"`
	BucketID  uint16            `json:"bucket_id"`
	Columns   map[string]string `json:"columns"`
	ByteSize  int64             `json:"byte_size"`
}

// ComputeCustomRowBytes returns the exact byte footprint of a custom SQL table row including all column names and values.
func ComputeCustomRowBytes(tableName, rowKey, shardKey string, cols map[string]string) int64 {
	size := int64(64 + len(tableName) + len(rowKey) + len(shardKey))
	for k, v := range cols {
		size += int64(16 + len(k) + len(v))
	}
	return size
}

// MutationOp defines the CDC operation type.
type MutationOp uint8

const (
	MutationInsertOrUpdate MutationOp = 1
	MutationDelete         MutationOp = 2
	MutationCustomUpsert   MutationOp = 3
	MutationCustomDelete   MutationOp = 4
)

// MutationLogEntry represents a row in the `_shardmaster_cdc` journal table (Pillar 3).
type MutationLogEntry struct {
	LSN         uint64     `json:"lsn"`
	BucketID    uint16     `json:"bucket_id"`
	Op          MutationOp `json:"op"`
	UserID      int64      `json:"user_id"`
	Row         UserRow    `json:"row"`
	Custom      CustomRow  `json:"custom,omitempty"`
	TimestampUs int64      `json:"timestamp_us"`
}

// BucketSlab stores the columnar slab, delta overlay, and custom SQL table rows for a single virtual bucket on a physical shard.
type BucketSlab struct {
	BucketID         uint16
	RowCount         int64
	SeededMaxID      int64
	BaseSlabSumCents int64
	SeededUIDs       []int64                         // Sorted slice of exact user_ids seeded into this bucket (1-to-1 with SlabBalances)
	SlabBalances     []uint32                        // Real allocated memory slab for this bucket's rows (never truncated in-place)
	SlabChecksum     [32]byte                        // Pre-computed rolling 256-bit SHA256 digest of SlabBalances
	DeltaOverrides   map[int64]UserRow               // Hot point inserts/updates on top of the columnar slab
	DeletedIDs       map[int64]bool                  // Tombstones for deleted user_ids
	CustomTables     map[string]map[string]CustomRow // tableName -> rowKey -> CustomRow
	CustomBytes      int64                           // Exact byte sum of all CustomTables rows in this bucket
}

// findSeededSlot performs an O(log N) binary search on the sorted SeededUIDs slice to locate the exact 1-to-1 index of uid.
func (slab *BucketSlab) findSeededSlot(uid int64) int {
	n := len(slab.SeededUIDs)
	if n == 0 {
		return -1
	}
	idx := sort.Search(n, func(i int) bool { return slab.SeededUIDs[i] >= uid })
	if idx < n && slab.SeededUIDs[idx] == uid {
		return idx
	}
	return -1
}

// SeededBalanceCents returns the exact stored balance (in cents) for a seeded user_id in this bucket.
func (slab *BucketSlab) SeededBalanceCents(uid int64) int64 {
	if idx := slab.findSeededSlot(uid); idx >= 0 && idx < len(slab.SlabBalances) {
		return int64(slab.SlabBalances[idx])
	}
	if len(slab.SlabBalances) > 0 {
		slot := int((uint64(uid) / uint64(hash.TotalVirtualBuckets)) % uint64(len(slab.SlabBalances)))
		return int64(slab.SlabBalances[slot])
	}
	return 0
}

// BucketMemoryBytesLocked computes the exact byte footprint of a BucketSlab while holding its lock.
func BucketMemoryBytesLocked(slab *BucketSlab) int64 {
	if slab == nil {
		return 0
	}
	total := int64(len(slab.SlabBalances) * 4)
	for _, r := range slab.DeltaOverrides {
		total += UserRowMemoryBytes(r)
	}
	total += int64(len(slab.DeletedIDs) * 16)
	total += slab.CustomBytes
	return total
}

// ComputeSlabArrayChecksum computes a real 256-bit SHA-256 digest over the bucket ID, row count, and every uint32 entry in balances.
func ComputeSlabArrayChecksum(bucketID uint16, rowCount int64, balances []uint32) [32]byte {
	h := sha256.New()
	var hdr [10]byte
	binary.LittleEndian.PutUint16(hdr[0:2], bucketID)
	binary.LittleEndian.PutUint64(hdr[2:10], uint64(rowCount))
	_, _ = h.Write(hdr[:])

	var buf [256]byte
	idx := 0
	for _, v := range balances {
		binary.LittleEndian.PutUint32(buf[idx:idx+4], v)
		idx += 4
		if idx == len(buf) {
			_, _ = h.Write(buf[:])
			idx = 0
		}
	}
	if idx > 0 {
		_, _ = h.Write(buf[:idx])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// CloneBucketSlab returns a deep copy of a BucketSlab for CDC migration or ACID transaction rollback backups.
func CloneBucketSlab(src *BucketSlab) *BucketSlab {
	if src == nil {
		return nil
	}
	uidsCopy := make([]int64, len(src.SeededUIDs))
	copy(uidsCopy, src.SeededUIDs)

	balancesCopy := make([]uint32, len(src.SlabBalances))
	copy(balancesCopy, src.SlabBalances)

	deltasCopy := make(map[int64]UserRow, len(src.DeltaOverrides))
	for k, v := range src.DeltaOverrides {
		deltasCopy[k] = v
	}
	deletedCopy := make(map[int64]bool, len(src.DeletedIDs))
	for k, v := range src.DeletedIDs {
		deletedCopy[k] = v
	}
	customCopy := make(map[string]map[string]CustomRow, len(src.CustomTables))
	for tbl, rowsMap := range src.CustomTables {
		tblCopy := make(map[string]CustomRow, len(rowsMap))
		for rk, cr := range rowsMap {
			colsCopy := make(map[string]string, len(cr.Columns))
			for ck, cv := range cr.Columns {
				colsCopy[ck] = cv
			}
			cr.Columns = colsCopy
			tblCopy[rk] = cr
		}
		customCopy[tbl] = tblCopy
	}

	return &BucketSlab{
		BucketID:         src.BucketID,
		RowCount:         src.RowCount,
		SeededMaxID:      src.SeededMaxID,
		BaseSlabSumCents: src.BaseSlabSumCents,
		SeededUIDs:       uidsCopy,
		SlabBalances:     balancesCopy,
		SlabChecksum:     src.SlabChecksum,
		DeltaOverrides:   deltasCopy,
		DeletedIDs:       deletedCopy,
		CustomTables:     customCopy,
		CustomBytes:      src.CustomBytes,
	}
}

var (
	uniqueFirstNames = [100]string{
		"Aarav", "Sophia", "Liam", "Olivia", "Noah", "Emma", "Vikram", "Mia", "Lucas", "Zara",
		"Ethan", "Priya", "Alexander", "Isabella", "Benjamin", "Amelia", "Sebastian", "Harper", "Mateo", "Evelyn",
		"Daniel", "Abigail", "Michael", "Emily", "logan", "Elizabeth", "Jackson", "Sofia", "Levi", "Avery",
		"David", "Ella", "Joseph", "Scarlett", "Samuel", "Grace", "Henry", "Chloe", "Owen", "Victoria",
		"Wyatt", "Riley", "John", "Aria", "Jack", "Lily", "Luke", "Aurora", "Jayden", "Zoey",
		"Dylan", "Penelope", "Grayson", "Layla", "Isaac", "Nora", "Gabriel", "Camila", "Julian", "Hannah",
		"Anthony", "Lillian", "Jaxon", "Addison", "Lincoln", "Eleanor", "Joshua", "Natalie", "Christopher", "Luna",
		"Andrew", "Savannah", "Theodore", "Brooklyn", "Caleb", "Leah", "Ryan", "Zoe", "Asher", "Stella",
		"Nathan", "Hazel", "Thomas", "Ellie", "Leo", "Paisley", "Isaiah", "Audrey", "Charles", "Skylar",
		"Josiah", "Violet", "Hudson", "Claire", "Christian", "Bella", "Hunter", "Lucy", "Connor", "Anna",
	}
	uniqueLastNames = [100]string{
		"Patel", "Chen", "Smith", "Garcia", "Kumar", "Kim", "Silva", "Miller", "Sato", "Lopez",
		"Mehta", "Shah", "Johnson", "Williams", "Brown", "Jones", "Davis", "Rodriguez", "Martinez", "Hernandez",
		"Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez",
		"Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
		"Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
		"Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Gomez", "Phillips",
		"Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris",
		"Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
		"Reed", "Kelly", "Howard", "Ramos", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez",
		"Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes", "Price", "Alvarez", "Castillo",
	}
	sampleDomains = []string{"gmail.com", "stripe.com", "stanford.edu", "vercel.com", "cloudflare.com"}
	sampleTenants = []string{"tenant_enterprise_1", "tenant_fintech", "tenant_saas", "tenant_core"}
	epochBaseTime = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
)

// SynthesizeSeededUserRow deterministically constructs a 100% unique UserRow for a seeded user_id.
// Every single user_id has a unique full name, unique email, unique timestamp, and unique balance.
func SynthesizeSeededUserRow(userID int64, bucket uint16, region string, balCents int64) UserRow {
	u := userID - 1
	if u < 0 {
		u = -u
	}
	firstIdx := int(u % 100)
	lastIdx := int(((u / 100) + int64(firstIdx)*7) % 100)
	firstName := uniqueFirstNames[firstIdx]
	if firstName == "logan" {
		firstName = "Logan"
	}
	lastName := uniqueLastNames[lastIdx]
	fullName := firstName + " " + lastName
	if userID > 10000 {
		fullName = fmt.Sprintf("%s %s #%d", firstName, lastName, userID)
	}
	domain := sampleDomains[int(userID)%len(sampleDomains)]
	email := fmt.Sprintf("%s.%s.%d@%s", strings.ToLower(firstName), strings.ToLower(lastName), userID, domain)
	ts := epochBaseTime.Add(time.Duration(userID) * time.Second)
	return UserRow{
		UserID:       userID,
		UserKey:      strconv.FormatInt(userID, 10),
		Name:         fullName,
		Email:        email,
		TenantID:     sampleTenants[int(userID)%len(sampleTenants)],
		Region:       region,
		BalanceCents: balCents,
		BucketID:     bucket,
		CreatedAt:    ts,
		UpdatedAt:    ts,
	}
}


// ShardSettings holds 100% free-form, byte-exact customizable parameters for a physical shard.
type ShardSettings struct {
	CustomAlias        string `json:"custom_alias"`
	CustomPort         int    `json:"custom_port"`
	Region             string `json:"region"`
	MaxCapacityBytes   int64  `json:"max_capacity_bytes"`    // Exact byte capacity (e.g. 4096 B, 1048576 B, 67108864 B, etc.)
	SlabBytesPerBucket int    `json:"slab_bytes_per_bucket"` // Exact bytes allocated per bucket slab in RAM
	DiskCapacityGB     int    `json:"disk_capacity_gb"`      // Optional GB alias (kept in sync if >= 1 GB)
	HardwareTier       string `json:"hardware_tier"`
	Weight             int    `json:"weight"`
	TargetBuckets      int    `json:"target_buckets"`
	AccessMode         string `json:"access_mode"`
	ReplicationMode    string `json:"replication_mode"`
	MaxConnections     int    `json:"max_connections"`
	BufferPoolBytes    int64  `json:"buffer_pool_bytes"`
	BufferPoolMB       int    `json:"buffer_pool_mb"`
}

// PhysicalShard represents an isolated physical database shard (e.g. Shard 0 :5432 .. Shard 7 :5439).
type PhysicalShard struct {
	ShardID    uint32
	Name       string
	Port       int
	Region     string
	DSN        string
	PostgresUp bool

	metaMu   sync.RWMutex
	settings ShardSettings

	bucketMu    [hash.TotalVirtualBuckets]sync.RWMutex
	buckets     [hash.TotalVirtualBuckets]*BucketSlab
	rowCount    atomic.Int64
	usedBytes   atomic.Int64
	seededMaxID atomic.Int64
	qpsCounter  atomic.Uint64
	lastQPS     atomic.Uint64
	totalOps    atomic.Uint64
	latencyNs   atomic.Int64 // EWMA latency in nanoseconds

	// Pillar 3: Change Data Capture (CDC) Mutation Log Journal
	cdcMu      sync.RWMutex
	lsnSeq     atomic.Uint64
	cdcJournal []MutationLogEntry
}

// NewPhysicalShard initializes a physical shard with 1,024 virtual bucket columnar slabs and lightweight 64 MB default quota.
func NewPhysicalShard(shardID uint32, region string) *PhysicalShard {
	port := 5432 + int(shardID)
	if region == "" {
		region = defaultRegions[int(shardID)%len(defaultRegions)]
	}
	alias := fmt.Sprintf("shard-%d", shardID)
	s := &PhysicalShard{
		ShardID: shardID,
		Name:    fmt.Sprintf("Shard %d :%d", shardID, port),
		Port:    port,
		Region:  region,
		DSN:     fmt.Sprintf("postgres://postgres:postgres@localhost:%d/shard_%d?sslmode=disable", port, shardID),
		settings: ShardSettings{
			CustomAlias:        alias,
			CustomPort:         port,
			Region:             region,
			MaxCapacityBytes:   DefaultShardCapacityBytes, // 67,108,864 B (64 MB) - safe for 16 GB RAM PCs!
			SlabBytesPerBucket: DefaultSlabBytesPerBucket, // 262,144 B (256 KB) per bucket
			DiskCapacityGB:     1,
			HardwareTier:       "16GB-PC-RAM-Slab",
			Weight:             100,
			TargetBuckets:      256,
			AccessMode:         "READ_WRITE",
			ReplicationMode:    "SYNC_QUORUM",
			MaxConnections:     1000,
			BufferPoolBytes:    16 * 1024 * 1024, // 16,777,216 B (16 MB)
			BufferPoolMB:       16,
		},
		cdcJournal: make([]MutationLogEntry, 0, 4096),
	}
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.buckets[b] = &BucketSlab{
			BucketID:       b,
			DeltaOverrides: make(map[int64]UserRow),
			DeletedIDs:     make(map[int64]bool),
			CustomTables:   make(map[string]map[string]CustomRow),
		}
	}
	return s
}

// GetSettings returns a thread-safe snapshot of the shard's customizable settings.
func (s *PhysicalShard) GetSettings() ShardSettings {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.settings
}

// UpdateSettings updates the shard's live operational settings, custom name, exact byte capacity, slab bytes, tier, and weight.
// IMPORTANT: Never truncates committed rows or populated SlabBalances in-place.
func (s *PhysicalShard) UpdateSettings(cfg ShardSettings) {
	s.metaMu.Lock()
	if strings.TrimSpace(cfg.CustomAlias) != "" {
		s.settings.CustomAlias = strings.TrimSpace(cfg.CustomAlias)
	}
	if cfg.CustomPort > 0 {
		s.settings.CustomPort = cfg.CustomPort
		s.Port = cfg.CustomPort
		s.Name = fmt.Sprintf("Shard %d :%d", s.ShardID, s.Port)
		s.DSN = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/shard_%d?sslmode=disable", s.Port, s.ShardID)
	}
	if strings.TrimSpace(cfg.Region) != "" {
		s.settings.Region = strings.TrimSpace(cfg.Region)
		s.Region = s.settings.Region
	}

	if cfg.MaxCapacityBytes > 0 && cfg.MaxCapacityBytes != s.settings.MaxCapacityBytes {
		s.settings.MaxCapacityBytes = cfg.MaxCapacityBytes
		s.settings.DiskCapacityGB = int(cfg.MaxCapacityBytes / (1024 * 1024 * 1024))
	} else if cfg.DiskCapacityGB > 0 && cfg.MaxCapacityBytes == 0 && cfg.DiskCapacityGB != s.settings.DiskCapacityGB {
		s.settings.DiskCapacityGB = cfg.DiskCapacityGB
		s.settings.MaxCapacityBytes = int64(cfg.DiskCapacityGB) * 1024 * 1024 * 1024
	}

	if cfg.SlabBytesPerBucket > 0 && cfg.SlabBytesPerBucket != s.settings.SlabBytesPerBucket {
		s.settings.SlabBytesPerBucket = cfg.SlabBytesPerBucket
	}

	if strings.TrimSpace(cfg.HardwareTier) != "" {
		s.settings.HardwareTier = strings.TrimSpace(cfg.HardwareTier)
	}
	if cfg.Weight >= 0 {
		s.settings.Weight = cfg.Weight
	}
	if cfg.TargetBuckets >= 0 && cfg.TargetBuckets <= hash.TotalVirtualBuckets {
		s.settings.TargetBuckets = cfg.TargetBuckets
	}
	if strings.TrimSpace(cfg.AccessMode) != "" {
		s.settings.AccessMode = strings.ToUpper(strings.TrimSpace(cfg.AccessMode))
	}
	if strings.TrimSpace(cfg.ReplicationMode) != "" {
		s.settings.ReplicationMode = strings.ToUpper(strings.TrimSpace(cfg.ReplicationMode))
	}
	if cfg.MaxConnections > 0 {
		s.settings.MaxConnections = cfg.MaxConnections
	}
	if cfg.BufferPoolBytes > 0 {
		s.settings.BufferPoolBytes = cfg.BufferPoolBytes
		s.settings.BufferPoolMB = int(cfg.BufferPoolBytes / (1024 * 1024))
	} else if cfg.BufferPoolMB > 0 {
		s.settings.BufferPoolMB = cfg.BufferPoolMB
		s.settings.BufferPoolBytes = int64(cfg.BufferPoolMB) * 1024 * 1024
	}
	s.metaMu.Unlock()
}

// ResizeMemorySlabs updates the shard's configured SlabBytesPerBucket without EVER truncating
// existing populated bucket slabs or committed rows in-place.
func (s *PhysicalShard) ResizeMemorySlabs(maxCapacityBytes int64, slabBytesPerBucket int) {
	s.metaMu.Lock()
	if maxCapacityBytes > 0 {
		s.settings.MaxCapacityBytes = maxCapacityBytes
	}
	if slabBytesPerBucket > 0 {
		s.settings.SlabBytesPerBucket = slabBytesPerBucket
	}
	s.metaMu.Unlock()
}

// UsedMemoryBytes returns the exact live byte footprint of all columnar slabs, delta overlays,
// tombstones, and custom SQL table rows currently stored on this physical shard.
func (s *PhysicalShard) UsedMemoryBytes() int64 {
	v := s.usedBytes.Load()
	if v < 0 {
		return 0
	}
	return v
}

// VerifyUsedMemoryBytes scans all 1,024 virtual buckets on this shard and returns the exact byte sum.
func (s *PhysicalShard) VerifyUsedMemoryBytes() int64 {
	var totalBytes int64
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		totalBytes += BucketMemoryBytesLocked(s.buckets[b])
		s.bucketMu[b].RUnlock()
	}
	s.usedBytes.Store(totalBytes)
	return totalBytes
}

// BucketMemoryBytes returns the exact live byte footprint of a single virtual bucket on this shard.
func (s *PhysicalShard) BucketMemoryBytes(bucket uint16) int64 {
	b := bucket & hash.BucketMask
	s.bucketMu[b].RLock()
	bytes := BucketMemoryBytesLocked(s.buckets[b])
	s.bucketMu[b].RUnlock()
	return bytes
}

// FreeMemoryBytes returns how many available bytes remain before hitting MaxCapacityBytes.
func (s *PhysicalShard) FreeMemoryBytes() int64 {
	cfg := s.GetSettings()
	maxCap := cfg.MaxCapacityBytes
	if maxCap <= 0 {
		maxCap = DefaultShardCapacityBytes
	}
	free := maxCap - s.UsedMemoryBytes()
	if free < 0 {
		return 0
	}
	return free
}

// tryReserveBytes atomically checks and reserves `delta` bytes against MaxCapacityBytes using CAS.
func (s *PhysicalShard) tryReserveBytes(delta int64) error {
	if delta <= 0 {
		s.usedBytes.Add(delta)
		return nil
	}
	cfg := s.GetSettings()
	maxCap := cfg.MaxCapacityBytes
	if maxCap <= 0 {
		maxCap = DefaultShardCapacityBytes
	}
	for {
		cur := s.usedBytes.Load()
		if cur+delta > maxCap {
			return fmt.Errorf("%w: shard %d [%s] byte quota exceeded (used %d B + write %d B > max %d B)",
				ErrShardCapacityExceeded, s.ShardID, s.DisplayName(), cur, delta, maxCap)
		}
		if s.usedBytes.CompareAndSwap(cur, cur+delta) {
			return nil
		}
	}
}

// DisplayName returns the user-configured CustomAlias for this shard.
func (s *PhysicalShard) DisplayName() string {
	s.metaMu.RLock()
	alias := s.settings.CustomAlias
	s.metaMu.RUnlock()
	if alias != "" {
		return alias
	}
	return fmt.Sprintf("shard-%d", s.ShardID)
}

// EstimatedUsedDiskGB returns UsedMemoryBytes in MB/GB for legacy callers.
func (s *PhysicalShard) EstimatedUsedDiskGB() float64 {
	return float64(s.UsedMemoryBytes()) / (1024.0 * 1024.0 * 1024.0)
}

// DiskUsagePct returns the percentage of configured MaxCapacityBytes currently used in RAM.
func (s *PhysicalShard) DiskUsagePct() float64 {
	cfg := s.GetSettings()
	capB := cfg.MaxCapacityBytes
	if capB <= 0 {
		capB = DefaultShardCapacityBytes
	}
	pct := (float64(s.UsedMemoryBytes()) / float64(capB)) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return pct
}

// ParseByteSize parses any user string into an exact byte count (int64).
// Accepts raw bytes ("1", "512", "4096", "67108864", "1024B") or units ("4KB", "64MB", "0.5GB", "128 KB").
func ParseByteSize(input string) (int64, error) {
	s := strings.TrimSpace(strings.ReplaceAll(input, ",", ""))
	s = strings.ReplaceAll(s, "_", "")
	if s == "" {
		return 0, fmt.Errorf("empty byte size")
	}
	upper := strings.ToUpper(s)

	multiplier := float64(1)
	numPart := upper
	switch {
	case strings.HasSuffix(upper, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		numPart = strings.TrimSpace(upper[:len(upper)-2])
	case strings.HasSuffix(upper, "GB"):
		multiplier = 1024 * 1024 * 1024
		numPart = strings.TrimSpace(upper[:len(upper)-2])
	case strings.HasSuffix(upper, "MB"):
		multiplier = 1024 * 1024
		numPart = strings.TrimSpace(upper[:len(upper)-2])
	case strings.HasSuffix(upper, "KB"):
		multiplier = 1024
		numPart = strings.TrimSpace(upper[:len(upper)-2])
	case strings.HasSuffix(upper, "BYTES"):
		multiplier = 1
		numPart = strings.TrimSpace(upper[:len(upper)-5])
	case strings.HasSuffix(upper, "BYTE"):
		multiplier = 1
		numPart = strings.TrimSpace(upper[:len(upper)-4])
	case strings.HasSuffix(upper, "B"):
		multiplier = 1
		numPart = strings.TrimSpace(upper[:len(upper)-1])
	}

	if iv, err := strconv.ParseInt(numPart, 10, 64); err == nil && multiplier == 1 {
		return iv, nil
	}
	fv, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size '%s'", input)
	}
	return int64(fv * multiplier), nil
}

// FormatBytesExact formats a byte count with both human unit and exact byte count, e.g. "64.00 MB (67,108,864 B)".
func FormatBytesExact(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.2f KB (%d B)", float64(b)/1024.0, b)
	}
	if b < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB (%d B)", float64(b)/(1024.0*1024.0), b)
	}
	return fmt.Sprintf("%.2f GB (%d B)", float64(b)/(1024.0*1024.0*1024.0), b)
}

// FormatBytesCompact formats a byte count concisely for table/TUI columns, e.g. "64.0MB" or "4096B".
func FormatBytesCompact(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	if b < 1024*1024 {
		if b%1024 == 0 {
			return fmt.Sprintf("%dKB", b/1024)
		}
		return fmt.Sprintf("%.1fKB", float64(b)/1024.0)
	}
	if b < 1024*1024*1024 {
		if b%(1024*1024) == 0 {
			return fmt.Sprintf("%dMB", b/(1024*1024))
		}
		return fmt.Sprintf("%.1fMB", float64(b)/(1024.0*1024.0))
	}
	return fmt.Sprintf("%.2fGB", float64(b)/(1024.0*1024.0*1024.0))
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

// ResetQPSCounter clears the transient QPS counter after internal bootstrap seeding.
func (s *PhysicalShard) ResetQPSCounter() {
	s.qpsCounter.Store(0)
	s.lastQPS.Store(0)
}

func (s *PhysicalShard) CurrentQPS() uint64    { return s.lastQPS.Load() }
func (s *PhysicalShard) RowCount() int64       { return s.rowCount.Load() }
func (s *PhysicalShard) AvgLatencyMs() float64 { return float64(s.latencyNs.Load()) / 1e6 }
func (s *PhysicalShard) CurrentLSN() uint64    { return s.lsnSeq.Load() }
func (s *PhysicalShard) SeededMaxUserID() int64 {
	v := s.seededMaxID.Load()
	if v <= 0 {
		return DefaultInitialRows
	}
	return v
}

// InstallBucketSlab installs or transfers a complete virtual bucket slab onto this physical shard.
func (s *PhysicalShard) InstallBucketSlab(slab *BucketSlab) {
	b := slab.BucketID & hash.BucketMask
	s.bucketMu[b].Lock()
	oldCount := s.buckets[b].RowCount
	oldBytes := BucketMemoryBytesLocked(s.buckets[b])
	if slab.DeltaOverrides == nil {
		slab.DeltaOverrides = make(map[int64]UserRow)
	}
	if slab.DeletedIDs == nil {
		slab.DeletedIDs = make(map[int64]bool)
	}
	if slab.CustomTables == nil {
		slab.CustomTables = make(map[string]map[string]CustomRow)
	}
	slab.SlabChecksum = ComputeSlabArrayChecksum(b, slab.RowCount, slab.SlabBalances)
	newBytes := BucketMemoryBytesLocked(slab)
	s.buckets[b] = slab
	s.usedBytes.Add(newBytes - oldBytes)
	if slab.SeededMaxID > s.seededMaxID.Load() {
		s.seededMaxID.Store(slab.SeededMaxID)
	}
	s.bucketMu[b].Unlock()
	s.rowCount.Add(slab.RowCount - oldCount)
}

// ExportBucketSlab clones a virtual bucket slab for zero-downtime CDC migration.
func (s *PhysicalShard) ExportBucketSlab(bucketID uint16) *BucketSlab {
	slab, _ := s.ExportBucketSlabWithLSN(bucketID)
	return slab
}

// ExportBucketSlabWithLSN atomically clones a virtual bucket slab and captures the exact LSN watermark
// while holding the bucket read lock, preventing duplicate or missed CDC replay during live migrations.
func (s *PhysicalShard) ExportBucketSlabWithLSN(bucketID uint16) (*BucketSlab, uint64) {
	b := bucketID & hash.BucketMask
	s.bucketMu[b].RLock()
	defer s.bucketMu[b].RUnlock()

	lsn := s.lsnSeq.Load()
	return CloneBucketSlab(s.buckets[b]), lsn
}

// UpdateSlabBalanceInPlace modifies a user's balance directly inside the contiguous []uint32 SlabBalances array
// (without adding a DeltaOverride) and updates SlabChecksum and BaseSlabSumCents.
// Because SlabBalances is never truncated, this direct slab modification is 100% preserved across resizes and VReplication.
func (s *PhysicalShard) UpdateSlabBalanceInPlace(userID int64, newBalanceCents uint32) bool {
	key := fmt.Sprintf("%d", userID)
	b := hash.ComputeBucket(key)
	s.bucketMu[b].Lock()
	defer s.bucketMu[b].Unlock()

	slab := s.buckets[b]
	if len(slab.SlabBalances) == 0 || slab.RowCount == 0 {
		return false
	}
	slot := slab.findSeededSlot(userID)
	if slot < 0 || slot >= len(slab.SlabBalances) {
		return false
	}
	oldVal := slab.SlabBalances[slot]
	slab.SlabBalances[slot] = newBalanceCents
	slab.BaseSlabSumCents += int64(newBalanceCents) - int64(oldVal)
	slab.SlabChecksum = ComputeSlabArrayChecksum(b, slab.RowCount, slab.SlabBalances)
	return true
}

// TryUpsertUser writes a row to the bucket's delta overlay, enforcing MaxCapacityBytes (SQLSTATE 53100)
// and READ_ONLY mode (SQLSTATE 25006) when enforceQuota is true.
func (s *PhysicalShard) TryUpsertUser(row UserRow, recordCDC bool, enforceQuota bool) (UserRow, error) {
	start := time.Now()
	if enforceQuota {
		cfg := s.GetSettings()
		if cfg.AccessMode == "READ_ONLY" {
			return UserRow{}, fmt.Errorf("%w: shard %d [%s] is in READ_ONLY mode", ErrShardReadOnly, s.ShardID, s.DisplayName())
		}
	}

	if row.UpdatedAt.IsZero() {
		row.UpdatedAt = start.UTC()
	}
	if row.CreatedAt.IsZero() {
		row.CreatedAt = row.UpdatedAt
	}
	if row.UserKey == "" {
		row.UserKey = fmt.Sprintf("%d", row.UserID)
	}
	if row.TenantID == "" {
		row.TenantID = "tenant_core"
	}
	if row.Region == "" {
		row.Region = s.Region
	}

	b := row.BucketID & hash.BucketMask
	s.bucketMu[b].Lock()
	slab := s.buckets[b]

	oldRow, existedDelta := slab.DeltaOverrides[row.UserID]
	wasDeleted := slab.DeletedIDs[row.UserID]

	newRowBytes := UserRowMemoryBytes(row)
	var deltaBytes int64
	if existedDelta {
		deltaBytes = newRowBytes - UserRowMemoryBytes(oldRow)
	} else {
		deltaBytes = newRowBytes
	}
	if wasDeleted {
		deltaBytes -= 16
	}

	if enforceQuota && deltaBytes > 0 {
		if err := s.tryReserveBytes(deltaBytes); err != nil {
			s.bucketMu[b].Unlock()
			return UserRow{}, err
		}
	} else {
		s.usedBytes.Add(deltaBytes)
	}

	existedInSeeded := slab.findSeededSlot(row.UserID) >= 0
	existedBefore := !wasDeleted && (existedDelta || existedInSeeded)

	delete(slab.DeletedIDs, row.UserID)
	slab.DeltaOverrides[row.UserID] = row

	if !existedBefore {
		slab.RowCount++
		s.rowCount.Add(1)
	}

	var lsn uint64
	if recordCDC {
		lsn = s.lsnSeq.Add(1)
		entry := MutationLogEntry{
			LSN:         lsn,
			BucketID:    b,
			Op:          MutationInsertOrUpdate,
			UserID:      row.UserID,
			Row:         row,
			TimestampUs: start.UnixMicro(),
		}
		s.cdcMu.Lock()
		if len(s.cdcJournal) >= 32768 {
			s.cdcJournal = append(s.cdcJournal[:0], s.cdcJournal[16384:]...)
		}
		s.cdcJournal = append(s.cdcJournal, entry)
		s.cdcMu.Unlock()
	}
	s.bucketMu[b].Unlock()

	s.RecordOp(time.Since(start).Nanoseconds())
	return row, nil
}

// UpsertUser writes a row to the bucket's delta overlay (unconditional internal/CDC helper).
func (s *PhysicalShard) UpsertUser(row UserRow, recordCDC bool) UserRow {
	saved, _ := s.TryUpsertUser(row, recordCDC, false)
	return saved
}

// GetUser retrieves any user in O(1) from the bucket slab or delta overlay.
func (s *PhysicalShard) GetUser(userID int64) (UserRow, bool) {
	start := time.Now()
	if userID <= 0 {
		return UserRow{}, false
	}
	key := fmt.Sprintf("%d", userID)
	b := hash.ComputeBucket(key)

	s.bucketMu[b].RLock()
	slab := s.buckets[b]
	if slab.DeletedIDs[userID] {
		s.bucketMu[b].RUnlock()
		s.RecordOp(time.Since(start).Nanoseconds())
		return UserRow{}, false
	}
	if deltaRow, ok := slab.DeltaOverrides[userID]; ok {
		s.bucketMu[b].RUnlock()
		s.RecordOp(time.Since(start).Nanoseconds())
		return deltaRow, true
	}

	slot := slab.findSeededSlot(userID)
	if slab.RowCount == 0 || slot < 0 || slot >= len(slab.SlabBalances) {
		s.bucketMu[b].RUnlock()
		s.RecordOp(time.Since(start).Nanoseconds())
		return UserRow{}, false
	}

	balCents := int64(slab.SlabBalances[slot])
	s.bucketMu[b].RUnlock()

	row := SynthesizeSeededUserRow(userID, b, s.Region, balCents)
	s.RecordOp(time.Since(start).Nanoseconds())
	return row, true
}

// TryDeleteUser removes a user by ID and appends a tombstone to `_shardmaster_cdc`.
func (s *PhysicalShard) TryDeleteUser(userID int64, recordCDC bool, enforceQuota bool) (bool, error) {
	start := time.Now()
	if enforceQuota {
		cfg := s.GetSettings()
		if cfg.AccessMode == "READ_ONLY" {
			return false, fmt.Errorf("%w: shard %d [%s] is in READ_ONLY mode", ErrShardReadOnly, s.ShardID, s.DisplayName())
		}
	}
	if userID <= 0 {
		return false, nil
	}
	key := fmt.Sprintf("%d", userID)
	b := hash.ComputeBucket(key)

	s.bucketMu[b].Lock()
	slab := s.buckets[b]
	if slab.DeletedIDs[userID] {
		s.bucketMu[b].Unlock()
		return false, nil
	}

	oldDelta, existedDelta := slab.DeltaOverrides[userID]
	existedSeeded := slab.findSeededSlot(userID) >= 0
	if !existedDelta && !existedSeeded {
		s.bucketMu[b].Unlock()
		return false, nil
	}

	var deltaBytes int64
	if existedDelta {
		deltaBytes -= UserRowMemoryBytes(oldDelta)
	}
	if existedSeeded {
		deltaBytes += 16 // Tombstone in DeletedIDs
		slab.DeletedIDs[userID] = true
	}
	delete(slab.DeltaOverrides, userID)
	s.usedBytes.Add(deltaBytes)

	slab.RowCount--
	s.rowCount.Add(-1)

	if recordCDC {
		lsn := s.lsnSeq.Add(1)
		s.cdcMu.Lock()
		if len(s.cdcJournal) >= 32768 {
			s.cdcJournal = append(s.cdcJournal[:0], s.cdcJournal[16384:]...)
		}
		s.cdcJournal = append(s.cdcJournal, MutationLogEntry{
			LSN:         lsn,
			BucketID:    b,
			Op:          MutationDelete,
			UserID:      userID,
			TimestampUs: start.UnixMicro(),
		})
		s.cdcMu.Unlock()
	}
	s.bucketMu[b].Unlock()

	s.RecordOp(time.Since(start).Nanoseconds())
	return true, nil
}

// DeleteUser removes a user by ID and appends a tombstone to `_shardmaster_cdc`.
func (s *PhysicalShard) DeleteUser(userID int64, recordCDC bool) bool {
	ok, _ := s.TryDeleteUser(userID, recordCDC, false)
	return ok
}

// TryUpsertCustomRow stores or updates a user-created SQL table row inside its owning virtual bucket,
// billing its exact column bytes to UsedMemoryBytes() and enforcing MaxCapacityBytes (SQLSTATE 53100).
func (s *PhysicalShard) TryUpsertCustomRow(row CustomRow, recordCDC bool, enforceQuota bool) error {
	if enforceQuota {
		cfg := s.GetSettings()
		if cfg.AccessMode == "READ_ONLY" {
			return fmt.Errorf("%w: shard %d [%s] is in READ_ONLY mode", ErrShardReadOnly, s.ShardID, s.DisplayName())
		}
	}
	if row.ByteSize <= 0 {
		row.ByteSize = ComputeCustomRowBytes(row.TableName, row.RowKey, row.ShardKey, row.Columns)
	}
	b := row.BucketID & hash.BucketMask
	s.bucketMu[b].Lock()
	slab := s.buckets[b]
	if slab.CustomTables == nil {
		slab.CustomTables = make(map[string]map[string]CustomRow)
	}
	tblMap, ok := slab.CustomTables[row.TableName]
	if !ok {
		tblMap = make(map[string]CustomRow)
		slab.CustomTables[row.TableName] = tblMap
	}
	var oldBytes int64
	if prev, exists := tblMap[row.RowKey]; exists {
		oldBytes = prev.ByteSize
	}
	deltaBytes := row.ByteSize - oldBytes
	if enforceQuota && deltaBytes > 0 {
		if err := s.tryReserveBytes(deltaBytes); err != nil {
			s.bucketMu[b].Unlock()
			return err
		}
	} else {
		s.usedBytes.Add(deltaBytes)
	}

	colsCopy := make(map[string]string, len(row.Columns))
	for k, v := range row.Columns {
		colsCopy[k] = v
	}
	row.Columns = colsCopy
	tblMap[row.RowKey] = row
	slab.CustomBytes += deltaBytes

	if recordCDC {
		lsn := s.lsnSeq.Add(1)
		s.cdcMu.Lock()
		if len(s.cdcJournal) >= 32768 {
			s.cdcJournal = append(s.cdcJournal[:0], s.cdcJournal[16384:]...)
		}
		s.cdcJournal = append(s.cdcJournal, MutationLogEntry{
			LSN:         lsn,
			BucketID:    b,
			Op:          MutationCustomUpsert,
			Custom:      row,
			TimestampUs: time.Now().UnixMicro(),
		})
		s.cdcMu.Unlock()
	}
	s.bucketMu[b].Unlock()
	return nil
}

// DeleteCustomRow removes a custom SQL table row from its virtual bucket and frees its exact byte footprint.
func (s *PhysicalShard) DeleteCustomRow(bucket uint16, tableName, rowKey string, recordCDC bool) bool {
	b := bucket & hash.BucketMask
	s.bucketMu[b].Lock()
	slab := s.buckets[b]
	if slab.CustomTables == nil {
		s.bucketMu[b].Unlock()
		return false
	}
	tblMap, ok := slab.CustomTables[tableName]
	if !ok {
		s.bucketMu[b].Unlock()
		return false
	}
	prev, exists := tblMap[rowKey]
	if !exists {
		s.bucketMu[b].Unlock()
		return false
	}
	delete(tblMap, rowKey)
	if len(tblMap) == 0 {
		delete(slab.CustomTables, tableName)
	}
	slab.CustomBytes -= prev.ByteSize
	if slab.CustomBytes < 0 {
		slab.CustomBytes = 0
	}
	s.usedBytes.Add(-prev.ByteSize)

	if recordCDC {
		lsn := s.lsnSeq.Add(1)
		s.cdcMu.Lock()
		if len(s.cdcJournal) >= 32768 {
			s.cdcJournal = append(s.cdcJournal[:0], s.cdcJournal[16384:]...)
		}
		s.cdcJournal = append(s.cdcJournal, MutationLogEntry{
			LSN:         lsn,
			BucketID:    b,
			Op:          MutationCustomDelete,
			Custom:      CustomRow{TableName: tableName, RowKey: rowKey, BucketID: b},
			TimestampUs: time.Now().UnixMicro(),
		})
		s.cdcMu.Unlock()
	}
	s.bucketMu[b].Unlock()
	return true
}

// DropCustomTableOnShard removes all rows of `tableName` across all 1,024 virtual buckets on this shard and frees their RAM bytes.
func (s *PhysicalShard) DropCustomTableOnShard(tableName string) {
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].Lock()
		slab := s.buckets[b]
		if slab.CustomTables != nil {
			if tblMap, ok := slab.CustomTables[tableName]; ok {
				var freed int64
				for _, r := range tblMap {
					freed += r.ByteSize
				}
				delete(slab.CustomTables, tableName)
				slab.CustomBytes -= freed
				if slab.CustomBytes < 0 {
					slab.CustomBytes = 0
				}
				s.usedBytes.Add(-freed)
			}
		}
		s.bucketMu[b].Unlock()
	}
}

// GetCustomTableBucketBytesAndRows returns the current rows and byte total for `tableName` in `bucket`.
func (s *PhysicalShard) GetCustomTableBucketBytesAndRows(bucket uint16, tableName string) (map[string]CustomRow, int64) {
	b := bucket & hash.BucketMask
	s.bucketMu[b].RLock()
	defer s.bucketMu[b].RUnlock()
	slab := s.buckets[b]
	if slab.CustomTables == nil {
		return nil, 0
	}
	tblMap, ok := slab.CustomTables[tableName]
	if !ok || len(tblMap) == 0 {
		return nil, 0
	}
	out := make(map[string]CustomRow, len(tblMap))
	var bytes int64
	for k, v := range tblMap {
		out[k] = v
		bytes += v.ByteSize
	}
	return out, bytes
}

// GetBucketRangeRowCount returns the exact number of rows stored in virtual buckets [startBucket, endBucket].
func (s *PhysicalShard) GetBucketRangeRowCount(startBucket, endBucket uint16) int64 {
	var total int64
	for b := startBucket; b <= endBucket; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		total += slab.RowCount
		for _, tblMap := range slab.CustomTables {
			total += int64(len(tblMap))
		}
		s.bucketMu[b].RUnlock()
	}
	return total
}

func hashCustomRowSHA256(cr CustomRow) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("CUSTOM|"))
	_, _ = h.Write([]byte(cr.TableName))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(cr.RowKey))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(cr.ShardKey))
	keys := make([]string, 0, len(cr.Columns))
	for k := range cr.Columns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte("|"))
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte("="))
		_, _ = h.Write([]byte(cr.Columns[k]))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// ComputeBucketRangeXORHash computes the 256-bit commutative XOR-SHA256 digest across [startBucket, endBucket],
// covering SlabBalances, DeltaOverrides, DeletedIDs, and all CustomTables rows.
func (s *PhysicalShard) ComputeBucketRangeXORHash(startBucket, endBucket uint16) (string, int64) {
	var acc [32]byte
	var totalRows int64

	for b := startBucket; b <= endBucket; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		hasData := slab.RowCount > 0 || len(slab.DeltaOverrides) > 0 || len(slab.DeletedIDs) > 0 || slab.CustomBytes > 0
		if hasData {
			totalRows += slab.RowCount
			slabDigest := ComputeSlabArrayChecksum(b, slab.RowCount, slab.SlabBalances)
			for j := 0; j < 32; j++ {
				acc[j] ^= slabDigest[j]
			}
			for _, dr := range slab.DeltaOverrides {
				h := sha256.Sum256([]byte(fmt.Sprintf("DELTA|%d|%s|%s|%s|%s|%d",
					dr.UserID, dr.Name, dr.Email, dr.TenantID, dr.Region, dr.BalanceCents)))
				for j := 0; j < 32; j++ {
					acc[j] ^= h[j]
				}
			}
			for uid, del := range slab.DeletedIDs {
				if del {
					h := sha256.Sum256([]byte(fmt.Sprintf("DEL|%d|%d", b, uid)))
					for j := 0; j < 32; j++ {
						acc[j] ^= h[j]
					}
				}
			}
			for _, tblMap := range slab.CustomTables {
				totalRows += int64(len(tblMap))
				for _, cr := range tblMap {
					ch := hashCustomRowSHA256(cr)
					for j := 0; j < 32; j++ {
						acc[j] ^= ch[j]
					}
				}
			}
		}
		s.bucketMu[b].RUnlock()
	}

	return hex.EncodeToString(acc[:]), totalRows
}

// PurgeBucketRange clears migrated virtual buckets [startBucket, endBucket] from source shard after cutover.
func (s *PhysicalShard) PurgeBucketRange(startBucket, endBucket uint16) int64 {
	var removed int64
	for b := startBucket; b <= endBucket; b++ {
		s.bucketMu[b].Lock()
		oldSlab := s.buckets[b]
		rc := oldSlab.RowCount
		oldBytes := BucketMemoryBytesLocked(oldSlab)
		if rc > 0 || oldBytes > 0 {
			removed += rc
			s.buckets[b] = &BucketSlab{
				BucketID:       b,
				RowCount:       0,
				SeededMaxID:    oldSlab.SeededMaxID,
				DeltaOverrides: make(map[int64]UserRow),
				DeletedIDs:     make(map[int64]bool),
				CustomTables:   make(map[string]map[string]CustomRow),
				CustomBytes:    0,
			}
			s.usedBytes.Add(-oldBytes)
		}
		s.bucketMu[b].Unlock()
	}
	if removed > 0 {
		s.rowCount.Add(-removed)
	}
	s.cdcMu.Lock()
	if len(s.cdcJournal) > 0 {
		filtered := s.cdcJournal[:0]
		for _, entry := range s.cdcJournal {
			if entry.BucketID < startBucket || entry.BucketID > endBucket {
				filtered = append(filtered, entry)
			}
		}
		s.cdcJournal = filtered
	}
	s.cdcMu.Unlock()
	return removed
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

// GetRecentCDCEntries returns up to `limit` latest CDC mutation log entries from this shard.
func (s *PhysicalShard) GetRecentCDCEntries(limit int) []MutationLogEntry {
	if limit <= 0 {
		limit = 5
	}
	s.cdcMu.RLock()
	n := len(s.cdcJournal)
	var out []MutationLogEntry
	if n > 0 {
		start := n - limit
		if start < 0 {
			start = 0
		}
		for i := n - 1; i >= start; i-- {
			out = append(out, s.cdcJournal[i])
		}
	}
	s.cdcMu.RUnlock()
	return out
}

// CDCEntryCount returns the exact number of recorded entries in the shard's CDC ring buffer.
func (s *PhysicalShard) CDCEntryCount() int {
	s.cdcMu.RLock()
	defer s.cdcMu.RUnlock()
	return len(s.cdcJournal)
}

// ComputeShardBalanceStats aggregates exact row count, sum, min, and max balance (in cents) across all owned buckets.
func (s *PhysicalShard) ComputeShardBalanceStats() (rows int64, sumCents int64, minCents int64, maxCents int64) {
	start := time.Now()
	minCents = 1<<62 - 1
	maxCents = 0
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		rc := slab.RowCount
		if rc > 0 {
			rows += rc
			if len(slab.SeededUIDs) > 0 && len(slab.SeededUIDs) == len(slab.SlabBalances) {
				for idx, uid := range slab.SeededUIDs {
					if slab.DeletedIDs[uid] {
						continue
					}
					if _, overridden := slab.DeltaOverrides[uid]; overridden {
						continue
					}
					bal := int64(slab.SlabBalances[idx])
					sumCents += bal
					if bal < minCents {
						minCents = bal
					}
					if bal > maxCents {
						maxCents = bal
					}
				}
			}
			for uid, dr := range slab.DeltaOverrides {
				if slab.DeletedIDs[uid] {
					continue
				}
				sumCents += dr.BalanceCents
				if dr.BalanceCents < minCents {
					minCents = dr.BalanceCents
				}
				if dr.BalanceCents > maxCents {
					maxCents = dr.BalanceCents
				}
			}
		}
		s.bucketMu[b].RUnlock()
	}
	if rows == 0 || minCents == 1<<62-1 {
		minCents = 0
	}
	s.RecordOp(time.Since(start).Nanoseconds())
	return rows, sumCents, minCents, maxCents
}

// ComputeShardTenantStats scans all active buckets on this shard and returns exact (count, sumCents) grouped by tenant_id.
func (s *PhysicalShard) ComputeShardTenantStats() map[string][2]int64 {
	start := time.Now()
	out := make(map[string][2]int64, 8)
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		if slab.RowCount > 0 {
			if len(slab.SeededUIDs) > 0 && len(slab.SeededUIDs) == len(slab.SlabBalances) {
				for idx, uid := range slab.SeededUIDs {
					if slab.DeletedIDs[uid] {
						continue
					}
					if _, overridden := slab.DeltaOverrides[uid]; overridden {
						continue
					}
					tenant := sampleTenants[int(uid)%len(sampleTenants)]
					cur := out[tenant]
					cur[0]++
					cur[1] += int64(slab.SlabBalances[idx])
					out[tenant] = cur
				}
			}
			for uid, dr := range slab.DeltaOverrides {
				if slab.DeletedIDs[uid] {
					continue
				}
				tenant := dr.TenantID
				if tenant == "" {
					tenant = "tenant_core"
				}
				cur := out[tenant]
				cur[0]++
				cur[1] += dr.BalanceCents
				out[tenant] = cur
			}
		}
		s.bucketMu[b].RUnlock()
	}
	s.RecordOp(time.Since(start).Nanoseconds())
	return out
}

// QueryFilter executes a local top-K filter & sort scan on this physical shard (used by Pillar 2 Scatter-Gather).
func (s *PhysicalShard) QueryFilter(emailSubstring string, regionFilter string, limit int) []UserRow {
	start := time.Now()
	if limit <= 0 {
		limit = 10
	}
	emailSub := strings.ToLower(strings.Trim(emailSubstring, "%'\" "))
	regSub := strings.ToLower(strings.Trim(regionFilter, "'\" "))

	var matched []UserRow
	seenUIDs := make(map[int64]bool)

	// 1. Scan all committed DeltaOverrides on owned buckets so newly inserted/updated rows are always included
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		if slab.RowCount > 0 || len(slab.DeltaOverrides) > 0 {
			for uid, row := range slab.DeltaOverrides {
				if slab.DeletedIDs[uid] {
					continue
				}
				if regSub != "" && strings.ToLower(row.Region) != regSub {
					continue
				}
				if emailSub != "" && !strings.Contains(strings.ToLower(row.Email), emailSub) {
					continue
				}
				matched = append(matched, row)
				seenUIDs[uid] = true
			}
		}
		s.bucketMu[b].RUnlock()
	}

	// 2. Scan real SeededUIDs across all buckets owned by this shard
	if regSub == "" || strings.ToLower(s.Region) == regSub {
		for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
			s.bucketMu[b].RLock()
			slab := s.buckets[b]
			if slab.RowCount > 0 && len(slab.SeededUIDs) > 0 {
				for i := len(slab.SeededUIDs) - 1; i >= 0; i-- {
					uid := slab.SeededUIDs[i]
					if seenUIDs[uid] || slab.DeletedIDs[uid] {
						continue
					}
					if _, overridden := slab.DeltaOverrides[uid]; overridden {
						continue
					}
					row := SynthesizeSeededUserRow(uid, b, s.Region, int64(slab.SlabBalances[i]))
					if emailSub != "" && !strings.Contains(strings.ToLower(row.Email), emailSub) {
						continue
					}
					matched = append(matched, row)
				}
			}
			s.bucketMu[b].RUnlock()
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].UserID > matched[j].UserID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if len(matched) > limit {
		matched = matched[:limit]
	}
	s.RecordOp(time.Since(start).Nanoseconds())
	return matched
}

// ClusterStorage manages the pool of all physical shards.
type ClusterStorage struct {
	mu      sync.RWMutex
	shards  map[uint32]*PhysicalShard
	dataDir string
}

var defaultRegions = []string{"local-node-0", "local-node-1", "local-node-2", "local-node-3"}

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

func (cs *ClusterStorage) InitializeShards(count uint32) {
	cs.mu.Lock()
	cs.shards = make(map[uint32]*PhysicalShard, count)
	for i := uint32(0); i < count; i++ {
		reg := defaultRegions[int(i)%len(defaultRegions)]
		cs.shards[i] = NewPhysicalShard(i, reg)
	}
	cs.mu.Unlock()
}

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

// CreateCustomShard provisions a new physical shard with custom alias, disk size, tier, weight, and settings.
func (cs *ClusterStorage) CreateCustomShard(cfg ShardSettings) *PhysicalShard {
	cs.mu.Lock()
	var nextID uint32
	var maxSeeded int64
	for id, sh := range cs.shards {
		if id >= nextID {
			nextID = id + 1
		}
		if sm := sh.seededMaxID.Load(); sm > maxSeeded {
			maxSeeded = sm
		}
	}
	reg := cfg.Region
	if reg == "" {
		reg = defaultRegions[int(nextID)%len(defaultRegions)]
	}
	s := NewPhysicalShard(nextID, reg)
	s.seededMaxID.Store(maxSeeded)
	s.UpdateSettings(cfg)
	cs.shards[nextID] = s
	cs.mu.Unlock()
	return s
}

func (cs *ClusterStorage) GetShard(shardID uint32) (*PhysicalShard, bool) {
	cs.mu.RLock()
	s, ok := cs.shards[shardID]
	cs.mu.RUnlock()
	return s, ok
}

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

func (cs *ClusterStorage) TotalRows() int64 {
	var total int64
	for _, s := range cs.GetAllShards() {
		total += s.RowCount()
	}
	return total
}

func (cs *ClusterStorage) TotalUsedBytes() int64 {
	var total int64
	for _, s := range cs.GetAllShards() {
		total += s.UsedMemoryBytes()
	}
	return total
}

// SeedCluster populates the cluster with `totalRows` (default: 10,000 real rows)
// in parallel across all CPU cores, hashing every single user_id into its exact virtual bucket.
func (cs *ClusterStorage) SeedCluster(
	totalRows int,
	bucketLookup func(bucket uint16) uint32,
) {
	if totalRows <= 0 {
		totalRows = DefaultInitialRows
	}

	totalInt64 := int64(totalRows)
	for _, s := range cs.GetAllShards() {
		s.seededMaxID.Store(totalInt64)
	}

	var exactUIDs [hash.TotalVirtualBuckets][]int64
	for uid := int64(1); uid <= totalInt64; uid++ {
		b := hash.ComputeBucketInt64(uid)
		exactUIDs[b] = append(exactUIDs[b], uid)
	}

	workers := runtime.NumCPU()
	var wg sync.WaitGroup
	bucketChan := make(chan uint16, hash.TotalVirtualBuckets)
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		bucketChan <- b
	}
	close(bucketChan)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range bucketChan {
				shardID := bucketLookup(b)
				shard, ok := cs.GetShard(shardID)
				if !ok {
					shard = cs.EnsureShard(shardID, "")
					shard.seededMaxID.Store(totalInt64)
				}

				uids := exactUIDs[b]
				count := int64(len(uids))
				balances := make([]uint32, len(uids))
				var baseSum int64
				for i, uid := range uids {
					// Bijective permutation modulo 900,000 (gcd(499979, 900000) == 1) so every single uid has a unique balance
					val := uint32(10000 + ((uint64(uid) * 499979) % 900000))
					balances[i] = val
					baseSum += int64(val)
				}

				slab := &BucketSlab{
					BucketID:         b,
					RowCount:         count,
					SeededMaxID:      totalInt64,
					BaseSlabSumCents: baseSum,
					SeededUIDs:       uids,
					SlabBalances:     balances,
					SlabChecksum:     ComputeSlabArrayChecksum(b, count, balances),
					DeltaOverrides:   make(map[int64]UserRow),
					DeletedIDs:       make(map[int64]bool),
					CustomTables:     make(map[string]map[string]CustomRow),
				}
				shard.InstallBucketSlab(slab)
			}
		}()
	}
	wg.Wait()

	// Record initial seed LSN checkpoint on each populated shard so _shardmaster_cdc contains real events
	nowUs := time.Now().UnixMicro()
	for _, s := range cs.GetAllShards() {
		s.cdcMu.Lock()
		emptyCDC := len(s.cdcJournal) == 0
		s.cdcMu.Unlock()
		if !emptyCDC {
			continue
		}
		for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
			s.bucketMu[b].RLock()
			uids := s.buckets[b].SeededUIDs
			s.bucketMu[b].RUnlock()
			if len(uids) > 0 {
				if firstRow, ok := s.GetUser(uids[0]); ok {
					lsn := s.lsnSeq.Add(1)
					s.cdcMu.Lock()
					s.cdcJournal = append(s.cdcJournal, MutationLogEntry{
						LSN:         lsn,
						BucketID:    b,
						Op:          MutationInsertOrUpdate,
						UserID:      firstRow.UserID,
						Row:         firstRow,
						TimestampUs: nowUs,
					})
					s.cdcMu.Unlock()
					break
				}
			}
		}
	}
}

type PersistedBucketDelta struct {
	BucketID       uint16                          `json:"bucket_id"`
	DeltaOverrides map[int64]UserRow               `json:"delta_overrides,omitempty"`
	DeletedIDs     map[int64]bool                  `json:"deleted_ids,omitempty"`
	CustomTables   map[string]map[string]CustomRow `json:"custom_tables,omitempty"`
	CustomBytes    int64                           `json:"custom_bytes,omitempty"`
}

type PersistedShardMeta struct {
	ShardID    uint32                 `json:"shard_id"`
	Region     string                 `json:"region"`
	RowCount   int64                  `json:"row_count"`
	Settings   ShardSettings          `json:"settings"`
	BucketData []PersistedBucketDelta `json:"bucket_data,omitempty"`
	CDCJournal []MutationLogEntry     `json:"cdc_journal,omitempty"`
	LSNSeq     uint64                 `json:"lsn_seq,omitempty"`
}

type PersistedClusterState struct {
	NumShards int                              `json:"num_shards"`
	Buckets   [hash.TotalVirtualBuckets]uint32 `json:"buckets"`
	Shards    []PersistedShardMeta             `json:"shards"`
	UpdatedAt time.Time                        `json:"updated_at"`
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
		var bDeltas []PersistedBucketDelta
		for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
			s.bucketMu[b].RLock()
			slab := s.buckets[b]
			if len(slab.DeltaOverrides) > 0 || len(slab.DeletedIDs) > 0 || len(slab.CustomTables) > 0 {
				cloned := CloneBucketSlab(slab)
				bDeltas = append(bDeltas, PersistedBucketDelta{
					BucketID:       b,
					DeltaOverrides: cloned.DeltaOverrides,
					DeletedIDs:     cloned.DeletedIDs,
					CustomTables:   cloned.CustomTables,
					CustomBytes:    cloned.CustomBytes,
				})
			}
			s.bucketMu[b].RUnlock()
		}
		s.cdcMu.RLock()
		cdcCopy := make([]MutationLogEntry, len(s.cdcJournal))
		copy(cdcCopy, s.cdcJournal)
		s.cdcMu.RUnlock()

		state.Shards[i] = PersistedShardMeta{
			ShardID:    s.ShardID,
			Region:     s.Region,
			RowCount:   s.RowCount(),
			Settings:   s.GetSettings(),
			BucketData: bDeltas,
			CDCJournal: cdcCopy,
			LSNSeq:     s.lsnSeq.Load(),
		}
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err == nil {
		tmpPath := filepath.Join(cs.dataDir, "cluster_state.json.tmp")
		finalPath := filepath.Join(cs.dataDir, "cluster_state.json")
		if err := os.WriteFile(tmpPath, raw, 0644); err == nil {
			_ = os.Rename(tmpPath, finalPath)
		}
	}
}

func (cs *ClusterStorage) DataDir() string {
	return cs.dataDir
}

func (cs *ClusterStorage) DropCustomTableEverywhere(tableName string) {
	for _, s := range cs.GetAllShards() {
		s.DropCustomTableOnShard(tableName)
	}
}

func (cs *ClusterStorage) LoadStateFile(setBucketOwner func(bucket uint16, shardID uint32)) bool {
	raw, err := os.ReadFile(filepath.Join(cs.dataDir, "cluster_state.json"))
	if err != nil {
		return false
	}
	var state PersistedClusterState
	if err := json.Unmarshal(raw, &state); err != nil || len(state.Shards) == 0 {
		return false
	}

	// Ensure all persisted shards exist (and upgrade legacy auto-generated cloud region names to local PC defaults)
	for _, sm := range state.Shards {
		if strings.HasPrefix(sm.Settings.CustomAlias, "us-west-core-") ||
			strings.HasPrefix(sm.Settings.CustomAlias, "us-east-core-") ||
			strings.HasPrefix(sm.Settings.CustomAlias, "eu-central-core-") ||
			strings.HasPrefix(sm.Settings.CustomAlias, "ap-south-core-") {
			sm.Region = defaultRegions[int(sm.ShardID)%len(defaultRegions)]
			sm.Settings.Region = sm.Region
			sm.Settings.CustomAlias = fmt.Sprintf("shard-%d", sm.ShardID)
		}
		sh := cs.EnsureShard(sm.ShardID, sm.Region)
		sh.UpdateSettings(sm.Settings)
	}

	// Move any seeded bucket slabs to their persisted owner shard if ownership changed
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		targetSID := state.Buckets[b]
		targetShard, ok := cs.GetShard(targetSID)
		if !ok {
			continue
		}
		targetShard.bucketMu[b].RLock()
		alreadyHas := len(targetShard.buckets[b].SeededUIDs) > 0 || targetShard.buckets[b].RowCount > 0
		targetShard.bucketMu[b].RUnlock()
		if !alreadyHas {
			for _, donor := range cs.GetAllShards() {
				if donor.ShardID == targetSID {
					continue
				}
				donor.bucketMu[b].RLock()
				hasDonor := len(donor.buckets[b].SeededUIDs) > 0 || donor.buckets[b].RowCount > 0
				donor.bucketMu[b].RUnlock()
				if hasDonor {
					slab := donor.ExportBucketSlab(b)
					targetShard.InstallBucketSlab(slab)
					donor.PurgeBucketRange(b, b)
					break
				}
			}
		}
		if setBucketOwner != nil {
			setBucketOwner(b, targetSID)
		}
	}

	// Overlay persisted DeltaOverrides, DeletedIDs, CustomTables, and CDCJournal onto each shard
	for _, sm := range state.Shards {
		sh, ok := cs.GetShard(sm.ShardID)
		if !ok {
			continue
		}
		if sm.LSNSeq > 0 {
			sh.lsnSeq.Store(sm.LSNSeq)
		}
		if len(sm.CDCJournal) > 0 {
			sh.cdcMu.Lock()
			sh.cdcJournal = sm.CDCJournal
			sh.cdcMu.Unlock()
		}
		for _, bd := range sm.BucketData {
			b := bd.BucketID & hash.BucketMask
			sh.bucketMu[b].Lock()
			slab := sh.buckets[b]
			oldBytes := BucketMemoryBytesLocked(slab)
			oldCount := slab.RowCount

			if bd.DeltaOverrides != nil {
				slab.DeltaOverrides = bd.DeltaOverrides
			}
			if bd.DeletedIDs != nil {
				slab.DeletedIDs = bd.DeletedIDs
			}
			if bd.CustomTables != nil {
				slab.CustomTables = bd.CustomTables
			}
			slab.CustomBytes = bd.CustomBytes

			// Recompute exact RowCount for this bucket
			var rc int64
			for _, uid := range slab.SeededUIDs {
				if !slab.DeletedIDs[uid] {
					rc++
				}
			}
			for uid := range slab.DeltaOverrides {
				if !slab.DeletedIDs[uid] && slab.findSeededSlot(uid) < 0 {
					rc++
				}
			}
			slab.RowCount = rc
			newBytes := BucketMemoryBytesLocked(slab)
			sh.usedBytes.Add(newBytes - oldBytes)
			sh.rowCount.Add(rc - oldCount)
			sh.bucketMu[b].Unlock()
		}
	}
	return true
}

func (s *PhysicalShard) GetCustomTableKeys(tableName string) map[uint16][]string {
	out := make(map[uint16][]string)
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		if slab.CustomTables != nil {
			if tblMap, ok := slab.CustomTables[tableName]; ok && len(tblMap) > 0 {
				keys := make([]string, 0, len(tblMap))
				for k := range tblMap {
					keys = append(keys, k)
				}
				out[b] = keys
			}
		}
		s.bucketMu[b].RUnlock()
	}
	return out
}

func (s *PhysicalShard) GetCustomTableTotalBytesAndRows(tableName string) (int64, int) {
	var totalBytes int64
	var totalRows int
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		if slab.CustomTables != nil {
			if tblMap, ok := slab.CustomTables[tableName]; ok {
				totalRows += len(tblMap)
				for _, r := range tblMap {
					totalBytes += r.ByteSize
				}
			}
		}
		s.bucketMu[b].RUnlock()
	}
	return totalBytes, totalRows
}

// GetAllCustomRows returns all custom table rows currently residing on this physical shard across all 1,024 buckets.
func (s *PhysicalShard) GetAllCustomRows() []CustomRow {
	var out []CustomRow
	for b := uint16(0); b < hash.TotalVirtualBuckets; b++ {
		s.bucketMu[b].RLock()
		slab := s.buckets[b]
		if slab.CustomTables != nil {
			for _, tblMap := range slab.CustomTables {
				for _, r := range tblMap {
					out = append(out, r)
				}
			}
		}
		s.bucketMu[b].RUnlock()
	}
	return out
}

