package cdc

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"

	"shardmaster/pkg/storage"
)

// VDiffReport contains the cryptographic verification proof between Source and Target shards.
type VDiffReport struct {
	StartBucket  uint16    `json:"start_bucket"`
	EndBucket    uint16    `json:"end_bucket"`
	SourceShard  uint32    `json:"source_shard"`
	TargetShard  uint32    `json:"target_shard"`
	SourceRows   int64     `json:"source_rows"`
	TargetRows   int64     `json:"target_rows"`
	SourceDigest string    `json:"source_digest"`
	TargetDigest string    `json:"target_digest"`
	Matched      bool      `json:"matched"`
	DurationUs   int64     `json:"duration_us"`
	VerifiedAt   time.Time `json:"verified_at"`
}

// ComputeRowSHA256 computes a canonical 256-bit SHA-256 digest of every column of a UserRow.
func ComputeRowSHA256(r storage.UserRow) [32]byte {
	h := sha256.New()
	var numBuf [16]byte
	binary.LittleEndian.PutUint64(numBuf[0:8], uint64(r.UserID))
	binary.LittleEndian.PutUint64(numBuf[8:16], uint64(r.BalanceCents))
	_, _ = h.Write(numBuf[:])
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(r.UserKey))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(r.Name))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(r.Email))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(r.TenantID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(r.Region))
	_, _ = h.Write([]byte("|"))

	binary.LittleEndian.PutUint16(numBuf[0:2], r.BucketID)
	binary.LittleEndian.PutUint64(numBuf[2:10], uint64(r.UpdatedAt.UnixMicro()))
	_, _ = h.Write(numBuf[:10])

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

// ComputeRollingXORHash computes the commutative 256-bit XOR of SHA-256 row digests.
func ComputeRollingXORHash(rows []storage.UserRow) (string, int) {
	var acc [32]byte
	for _, r := range rows {
		rowDigest := ComputeRowSHA256(r)
		for j := 0; j < 32; j++ {
			acc[j] ^= rowDigest[j]
		}
	}
	return hex.EncodeToString(acc[:]), len(rows)
}

// VerifyBucketRangeVDiff runs Vitess-style VDiff cryptographic parity verification
// between sourceShard and targetShard across virtual buckets [startBucket, endBucket].
func VerifyBucketRangeVDiff(
	sourceShard *storage.PhysicalShard,
	targetShard *storage.PhysicalShard,
	startBucket, endBucket uint16,
) VDiffReport {
	start := time.Now()

	srcHex, srcCount := sourceShard.ComputeBucketRangeXORHash(startBucket, endBucket)
	dstHex, dstCount := targetShard.ComputeBucketRangeXORHash(startBucket, endBucket)

	elapsedUs := time.Since(start).Microseconds()

	return VDiffReport{
		StartBucket:  startBucket,
		EndBucket:    endBucket,
		SourceShard:  sourceShard.ShardID,
		TargetShard:  targetShard.ShardID,
		SourceRows:   srcCount,
		TargetRows:   dstCount,
		SourceDigest: srcHex,
		TargetDigest: dstHex,
		Matched:      srcCount == dstCount && srcHex == dstHex,
		DurationUs:   elapsedUs,
		VerifiedAt:   time.Now().UTC(),
	}
}
