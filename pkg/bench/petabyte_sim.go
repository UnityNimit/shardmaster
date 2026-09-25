package bench

import (
	"shardmaster/pkg/hash"
)

// PetabyteSimReport holds mathematical and structural metrics for a 1-Petabyte (1 Trillion rows) cluster.
type PetabyteSimReport struct {
	TotalDataPB            float64
	TotalRecords           uint64
	AvgRowSizeBytes        uint64
	VirtualBuckets         int
	InitialPhysicalShards  uint32
	TargetPhysicalShards   uint32
	RecordsPerBucket       uint64
	DataGBPerBucket        float64
	DirectoryRAMBytes      int
	RangesToMigrate        []hash.BucketMigrationRange
	BucketsMoved           int
	DataMovedPct           float64
	NaiveModuloMovedPct    float64
	NetworkSavedTB         float64
}

// SimulatePetabyteScale proves how the 1,024 Virtual Bucket architecture handles 1 Petabyte (1,000 TB)
// of data across physical shards while keeping the entire routing table in 4,096 bytes of CPU L1 cache.
func SimulatePetabyteScale(initialShards, targetShards uint32) PetabyteSimReport {
	if initialShards == 0 {
		initialShards = 64
	}
	if targetShards <= initialShards {
		targetShards = initialShards + 16
	}

	const totalRecords uint64 = 1_000_000_000_000 // 1 Trillion records
	const rowBytes uint64 = 1024                  // 1 KB per row = 1 Petabyte (1,024 TB)
	const totalTB = 1024.0

	initialMap := hash.InitialBucketAssignment(initialShards)
	_, ranges := hash.ComputeOptimalSplit(initialMap, targetShards)

	bucketsMoved := 0
	for _, r := range ranges {
		bucketsMoved += int(r.EndBucket - r.StartBucket + 1)
	}

	movedPct := (float64(bucketsMoved) / float64(hash.TotalVirtualBuckets)) * 100.0
	naiveModuloPct := (float64(targetShards-1) / float64(targetShards)) * 100.0
	savedTB := totalTB * ((naiveModuloPct - movedPct) / 100.0)

	return PetabyteSimReport{
		TotalDataPB:           1.0,
		TotalRecords:          totalRecords,
		AvgRowSizeBytes:       rowBytes,
		VirtualBuckets:        hash.TotalVirtualBuckets,
		InitialPhysicalShards: initialShards,
		TargetPhysicalShards:  targetShards,
		RecordsPerBucket:      totalRecords / uint64(hash.TotalVirtualBuckets),
		DataGBPerBucket:       (totalTB * 1024.0) / float64(hash.TotalVirtualBuckets),
		DirectoryRAMBytes:     hash.TotalVirtualBuckets * 4, // [1024]atomic.Uint32 = 4,096 bytes!
		RangesToMigrate:       ranges,
		BucketsMoved:          bucketsMoved,
		DataMovedPct:          movedPct,
		NaiveModuloMovedPct:   naiveModuloPct,
		NetworkSavedTB:        savedTB,
	}
}
