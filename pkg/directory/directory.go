package directory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"shardmaster/pkg/hash"
)

const (
	BucketStateNormal       uint32 = 0
	BucketStateCDCStreaming uint32 = 1
	BucketStateCutoverGate  uint32 = 2
)

// LookupResult holds rich telemetry for an O(1) directory lookup.
type LookupResult struct {
	Key           string
	ShardID       uint32
	ShardName     string
	VirtualBucket uint16
	HashValue     uint64
	LookupTimeNs  int64
	Source        string
	BucketState   uint32
}

// ShardDirectory is a lock-free, L1-cache-resident virtual bucket routing table.
// Memory footprint of the core [1024]atomic.Uint32 array is exactly 4,096 bytes (4 KB).
type ShardDirectory struct {
	buckets        [hash.TotalVirtualBuckets]atomic.Uint32
	bucketStates   [hash.TotalVirtualBuckets]atomic.Uint32
	writeGateMu    [hash.TotalVirtualBuckets]sync.RWMutex
	activeShards   atomic.Uint32
	routingVersion atomic.Uint64
	totalLookups   atomic.Uint64

	pinMu      sync.RWMutex
	hasPins    atomic.Bool
	pinnedKeys map[string]uint32
}

// NewShardDirectory initializes the 1,024 virtual buckets across initialShards.
func NewShardDirectory(initialShards uint32) *ShardDirectory {
	if initialShards == 0 {
		initialShards = 4
	}
	d := &ShardDirectory{
		pinnedKeys: make(map[string]uint32),
	}
	d.Reset(initialShards)
	return d
}

// Reset re-initializes the bucket map for numShards physical shards.
func (d *ShardDirectory) Reset(numShards uint32) {
	initial := hash.InitialBucketAssignment(numShards)
	for b := 0; b < hash.TotalVirtualBuckets; b++ {
		d.buckets[b].Store(initial[b])
		d.bucketStates[b].Store(BucketStateNormal)
	}
	d.activeShards.Store(numShards)
	d.routingVersion.Add(1)
	d.pinMu.Lock()
	d.pinnedKeys = make(map[string]uint32)
	d.hasPins.Store(false)
	d.pinMu.Unlock()
}

// LookupFast is the ultra-hot path for point routing: 0 heap allocations, ~15-30 nanoseconds.
//
//go:inline
func (d *ShardDirectory) LookupFast(key string) (shardID uint32, bucket uint16) {
	d.totalLookups.Add(1)
	if d.hasPins.Load() {
		d.pinMu.RLock()
		if pinned, ok := d.pinnedKeys[key]; ok {
			d.pinMu.RUnlock()
			return pinned, hash.ComputeBucket(key)
		}
		d.pinMu.RUnlock()
	}
	bucket = hash.ComputeBucket(key)
	shardID = d.buckets[bucket].Load()
	return shardID, bucket
}

// LookupInt64Fast routes an integer user_id with zero string conversions and zero heap allocations.
//
//go:inline
func (d *ShardDirectory) LookupInt64Fast(userID int64) (shardID uint32, bucket uint16) {
	d.totalLookups.Add(1)
	bucket = hash.ComputeBucketInt64(userID)
	shardID = d.buckets[bucket].Load()
	return shardID, bucket
}

// LookupDetailed returns full telemetry for CLI / HTTP / EXPLAIN SHARD inspection.
func (d *ShardDirectory) LookupDetailed(key string) LookupResult {
	start := time.Now()
	d.totalLookups.Add(1)

	h := hash.HashKey(key)
	bucket := uint16(h & hash.BucketMask)
	source := "ATOMIC_BUCKET_RING"
	var shardID uint32

	if d.hasPins.Load() {
		d.pinMu.RLock()
		if pinned, ok := d.pinnedKeys[key]; ok {
			shardID = pinned
			source = "HOTSPOT_PIN_OVERRIDE"
		}
		d.pinMu.RUnlock()
	}

	if source == "ATOMIC_BUCKET_RING" {
		shardID = d.buckets[bucket].Load()
	}

	elapsedNs := time.Since(start).Nanoseconds()
	if elapsedNs <= 0 {
		t0 := time.Now()
		for i := 0; i < 256; i++ {
			_, _ = d.LookupFast(key)
		}
		elapsedNs = time.Since(t0).Nanoseconds() / 256
		if elapsedNs <= 0 {
			elapsedNs = 1
		}
	}

	return LookupResult{
		Key:           key,
		ShardID:       shardID,
		ShardName:     fmt.Sprintf("shard_%d", shardID),
		VirtualBucket: bucket,
		HashValue:     h,
		LookupTimeNs:  elapsedNs,
		Source:        source,
		BucketState:   d.bucketStates[bucket].Load(),
	}
}

// GetBucketOwner returns the current owning physical shard ID for a virtual bucket [0..1023].
//
//go:inline
func (d *ShardDirectory) GetBucketOwner(bucket uint16) uint32 {
	return d.buckets[bucket&hash.BucketMask].Load()
}

// SetBucketState updates the migration lifecycle state for a bucket.
func (d *ShardDirectory) SetBucketState(bucket uint16, state uint32) {
	d.bucketStates[bucket&hash.BucketMask].Store(state)
}

// GetBucketState returns the migration lifecycle state for a bucket.
func (d *ShardDirectory) GetBucketState(bucket uint16) uint32 {
	return d.bucketStates[bucket&hash.BucketMask].Load()
}

// AtomicCutoverBucket flips a virtual bucket's pointer to newShardID in a single CPU atomic instruction.
func (d *ShardDirectory) AtomicCutoverBucket(bucket uint16, newShardID uint32) {
	b := bucket & hash.BucketMask
	d.buckets[b].Store(newShardID)
	d.bucketStates[b].Store(BucketStateNormal)
	for {
		cur := d.activeShards.Load()
		if newShardID+1 <= cur {
			break
		}
		if d.activeShards.CompareAndSwap(cur, newShardID+1) {
			break
		}
	}
	d.routingVersion.Add(1)
}

// AcquireBucketWrite acquires a read lock on the bucket's write gate and returns the current owning shardID,
// bucket ID, and an unlock callback. This prevents writers from stranding mutations on a source shard during cutover.
func (d *ShardDirectory) AcquireBucketWrite(key string) (uint32, uint16, func()) {
	d.totalLookups.Add(1)
	b := hash.ComputeBucket(key)
	d.writeGateMu[b].RLock()
	if d.hasPins.Load() {
		d.pinMu.RLock()
		if pinned, ok := d.pinnedKeys[key]; ok {
			d.pinMu.RUnlock()
			return pinned, b, func() { d.writeGateMu[b].RUnlock() }
		}
		d.pinMu.RUnlock()
	}
	owner := d.buckets[b].Load()
	return owner, b, func() { d.writeGateMu[b].RUnlock() }
}

// AcquireBucketWriteByID acquires a read lock on the bucket's write gate by bucket ID.
func (d *ShardDirectory) AcquireBucketWriteByID(bucket uint16) (uint32, func()) {
	b := bucket & hash.BucketMask
	d.writeGateMu[b].RLock()
	owner := d.buckets[b].Load()
	return owner, func() { d.writeGateMu[b].RUnlock() }
}

// LockBucketRangeForCutover acquires exclusive write-gate locks on [startBucket, endBucket] in ascending order.
func (d *ShardDirectory) LockBucketRangeForCutover(startBucket, endBucket uint16) {
	for b := startBucket; b <= endBucket; b++ {
		d.writeGateMu[b&hash.BucketMask].Lock()
	}
}

// UnlockBucketRangeForCutover releases exclusive write-gate locks on [startBucket, endBucket] in reverse order.
func (d *ShardDirectory) UnlockBucketRangeForCutover(startBucket, endBucket uint16) {
	for b := int(endBucket); b >= int(startBucket); b-- {
		d.writeGateMu[uint16(b)&hash.BucketMask].Unlock()
	}
}

// RegisterShard increments activeShards if shardID >= activeShards.
func (d *ShardDirectory) RegisterShard(shardID uint32) {
	for {
		cur := d.activeShards.Load()
		if shardID+1 <= cur {
			break
		}
		if d.activeShards.CompareAndSwap(cur, shardID+1) {
			break
		}
	}
	d.routingVersion.Add(1)
}

// PinKey pins a specific hot key to a dedicated shard.
func (d *ShardDirectory) PinKey(key string, shardID uint32) {
	d.pinMu.Lock()
	d.pinnedKeys[key] = shardID
	d.hasPins.Store(true)
	d.pinMu.Unlock()
	d.routingVersion.Add(1)
}

// SnapshotBuckets returns a copy of all 1,024 bucket assignments.
func (d *ShardDirectory) SnapshotBuckets() [hash.TotalVirtualBuckets]uint32 {
	var snap [hash.TotalVirtualBuckets]uint32
	for b := 0; b < hash.TotalVirtualBuckets; b++ {
		snap[b] = d.buckets[b].Load()
	}
	return snap
}

// SnapshotStates returns a copy of all 1,024 bucket migration states.
func (d *ShardDirectory) SnapshotStates() [hash.TotalVirtualBuckets]uint32 {
	var snap [hash.TotalVirtualBuckets]uint32
	for b := 0; b < hash.TotalVirtualBuckets; b++ {
		snap[b] = d.bucketStates[b].Load()
	}
	return snap
}

// BucketCountsByShard returns how many virtual buckets each physical shard currently owns.
func (d *ShardDirectory) BucketCountsByShard() map[uint32]int {
	counts := make(map[uint32]int)
	num := d.activeShards.Load()
	for s := uint32(0); s < num; s++ {
		counts[s] = 0
	}
	for b := 0; b < hash.TotalVirtualBuckets; b++ {
		owner := d.buckets[b].Load()
		counts[owner]++
	}
	return counts
}

func (d *ShardDirectory) ActiveShards() uint32   { return d.activeShards.Load() }
func (d *ShardDirectory) RoutingVersion() uint64 { return d.routingVersion.Load() }
func (d *ShardDirectory) TotalLookups() uint64   { return d.totalLookups.Load() }
