package hash

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
)

const (
	// TotalVirtualBuckets is 1,024 (power of two for single-cycle bitwise mask & 1023)
	TotalVirtualBuckets = 1024
	BucketMask          = TotalVirtualBuckets - 1
)

// BucketMigrationRange represents a contiguous range of virtual buckets moving between physical shards.
type BucketMigrationRange struct {
	StartBucket uint16
	EndBucket   uint16
	FromShard   uint32
	ToShard     uint32
}

// HashKey computes a 64-bit xxHash digest with zero heap allocations.
//
//go:inline
func HashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}

// HashInt64 computes a 64-bit xxHash digest for an integer key on the stack (0 allocs/op).
//
//go:inline
func HashInt64(id int64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(id))
	return xxhash.Sum64(buf[:])
}

// ComputeBucket maps any string shard key to a virtual bucket in [0, 1023] in O(1) time.
//
//go:inline
func ComputeBucket(key string) uint16 {
	return uint16(xxhash.Sum64String(key) & BucketMask)
}

// ComputeBucketInt64 maps a numeric user_id to a virtual bucket in [0, 1023] with 0 heap allocations.
//
//go:inline
func ComputeBucketInt64(id int64) uint16 {
	return uint16(HashInt64(id) & BucketMask)
}

// ComputeModuloShard implements classic modulo hashing: xxhash(key) % numShards.
//
//go:inline
func ComputeModuloShard(key string, numShards uint32) uint32 {
	if numShards == 0 {
		return 0
	}
	return uint32(xxhash.Sum64String(key) % uint64(numShards))
}

// InitialBucketAssignment distributes 1,024 virtual buckets in contiguous ranges across numShards.
// Example for 4 shards:
//   Shard 0: Buckets [0 .. 255]   (256 buckets)
//   Shard 1: Buckets [256 .. 511] (256 buckets)
//   Shard 2: Buckets [512 .. 767] (256 buckets)
//   Shard 3: Buckets [768 .. 1023](256 buckets)
func InitialBucketAssignment(numShards uint32) [TotalVirtualBuckets]uint32 {
	var table [TotalVirtualBuckets]uint32
	if numShards == 0 {
		return table
	}
	perShard := uint32(TotalVirtualBuckets) / numShards
	for b := uint32(0); b < TotalVirtualBuckets; b++ {
		shardID := b / perShard
		if shardID >= numShards {
			shardID = numShards - 1
		}
		table[b] = shardID
	}
	return table
}

// ComputeOptimalSplit calculates the new virtual bucket map when scaling from currentShards to targetShards.
// Only the exact surplus buckets needed to balance the new shard(s) are moved, preserving cache locality
// and minimizing network I/O (consistent virtual-bucket resharding).
func ComputeOptimalSplit(
	current [TotalVirtualBuckets]uint32,
	targetShards uint32,
) ([TotalVirtualBuckets]uint32, []BucketMigrationRange) {
	next := current
	if targetShards == 0 {
		return next, nil
	}

	// Group current buckets by owner shard
	owned := make(map[uint32][]uint16, targetShards)
	var maxExistingShard uint32
	for b := uint16(0); b < TotalVirtualBuckets; b++ {
		s := current[b]
		owned[s] = append(owned[s], b)
		if s > maxExistingShard {
			maxExistingShard = s
		}
	}

	existingCount := maxExistingShard + 1
	if targetShards <= existingCount {
		return next, nil
	}

	targetPerShard := TotalVirtualBuckets / int(targetShards)
	remainder := TotalVirtualBuckets % int(targetShards)

	var ranges []BucketMigrationRange

	// Steal tail buckets from existing shards and assign to new shards [existingCount .. targetShards-1]
	newShardIdx := existingCount
	neededForCurrentNewShard := targetPerShard
	if int(newShardIdx) < remainder {
		neededForCurrentNewShard++
	}

	for s := uint32(0); s < existingCount && newShardIdx < targetShards; s++ {
		buckets := owned[s]
		keepCount := targetPerShard
		if int(s) < remainder {
			keepCount++
		}
		if len(buckets) <= keepCount {
			continue
		}

		// Donate buckets from the tail of shard s (e.g., Buckets [204..255] when scaling 4 -> 5)
		donateSlice := buckets[keepCount:]
		idx := 0
		for idx < len(donateSlice) && newShardIdx < targetShards {
			take := neededForCurrentNewShard
			remainingDonate := len(donateSlice) - idx
			if take > remainingDonate {
				take = remainingDonate
			}

			chunk := donateSlice[idx : idx+take]
			for _, b := range chunk {
				next[b] = newShardIdx
			}

			if len(chunk) > 0 {
				runStart := chunk[0]
				runPrev := chunk[0]
				for k := 1; k < len(chunk); k++ {
					if chunk[k] != runPrev+1 {
						ranges = append(ranges, BucketMigrationRange{
							StartBucket: runStart,
							EndBucket:   runPrev,
							FromShard:   s,
							ToShard:     newShardIdx,
						})
						runStart = chunk[k]
					}
					runPrev = chunk[k]
				}
				ranges = append(ranges, BucketMigrationRange{
					StartBucket: runStart,
					EndBucket:   runPrev,
					FromShard:   s,
					ToShard:     newShardIdx,
				})
			}

			idx += take
			neededForCurrentNewShard -= take
			if neededForCurrentNewShard == 0 {
				newShardIdx++
				if newShardIdx < targetShards {
					neededForCurrentNewShard = targetPerShard
					if int(newShardIdx) < remainder {
						neededForCurrentNewShard++
					}
				}
			}
		}
	}

	return next, ranges
}
