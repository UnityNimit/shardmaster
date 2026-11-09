package router

import (
	"container/heap"
	"sync"

	"shardmaster/pkg/storage"
)

// TaggedRow pairs a UserRow with the physical shard ID that produced it.
type TaggedRow struct {
	ShardID uint32
	Row     storage.UserRow
}

// kWayNode represents the head item of one shard's streaming channel in the Priority Queue.
type kWayNode struct {
	shardID   uint32
	row       storage.UserRow
	streamIdx int
}

// kWayHeap implements container/heap.Interface ordered by CreatedAt DESC (then UserID DESC).
type kWayHeap []kWayNode

func (h kWayHeap) Len() int { return len(h) }

func (h kWayHeap) Less(i, j int) bool {
	if h[i].row.CreatedAt.Equal(h[j].row.CreatedAt) {
		return h[i].row.UserID > h[j].row.UserID
	}
	return h[i].row.CreatedAt.After(h[j].row.CreatedAt)
}

func (h kWayHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *kWayHeap) Push(x any) {
	*h = append(*h, x.(kWayNode))
}

func (h *kWayHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// ExecuteScatterGatherKWayMerge implements Pillar 2:
//  1. Fans out parallel queries across all N physical shards simultaneously using worker Goroutines.
//  2. Streams rows back from each shard over bounded Go channels.
//  3. Uses a Priority Queue (container/heap) to execute a streaming K-Way Merge Sort in O(K * batch) RAM.
//  4. Returns the global Top `limit` rows seamlessly.
func ExecuteScatterGatherKWayMerge(
	shards []*storage.PhysicalShard,
	emailPattern string,
	regionFilter string,
	limit int,
) []TaggedRow {
	if limit <= 0 {
		limit = 10
	}
	k := len(shards)
	if k == 0 {
		return nil
	}

	streams := make([]chan storage.UserRow, k)
	var wg sync.WaitGroup

	// Step 1: Fan out parallel Goroutines to all N physical shards
	for idx, shard := range shards {
		ch := make(chan storage.UserRow, 16) // Strictly bounded O(batch) channel buffer
		streams[idx] = ch
		wg.Add(1)

		go func(s *storage.PhysicalShard, out chan<- storage.UserRow) {
			defer wg.Done()
			defer close(out)
			localTop := s.QueryFilter(emailPattern, regionFilter, limit)
			for _, r := range localTop {
				out <- r
			}
		}(shard, ch)
	}

	// Step 2: Seed the K-Way Merge Priority Queue with the first element from each non-empty shard stream
	pq := make(kWayHeap, 0, k)
	for i := 0; i < k; i++ {
		if firstRow, ok := <-streams[i]; ok {
			pq = append(pq, kWayNode{
				shardID:   shards[i].ShardID,
				row:       firstRow,
				streamIdx: i,
			})
		}
	}
	heap.Init(&pq)

	// Step 3: Pop global maximum and advance only that shard's channel stream
	results := make([]TaggedRow, 0, limit)
	for pq.Len() > 0 && len(results) < limit {
		best := heap.Pop(&pq).(kWayNode)
		results = append(results, TaggedRow{
			ShardID: best.shardID,
			Row:     best.row,
		})

		if nextRow, ok := <-streams[best.streamIdx]; ok {
			heap.Push(&pq, kWayNode{
				shardID:   best.shardID,
				row:       nextRow,
				streamIdx: best.streamIdx,
			})
		}
	}

	// Drain any remaining items so worker Goroutines exit cleanly
	for i := 0; i < k; i++ {
		go func(ch <-chan storage.UserRow) {
			for range ch {
			}
		}(streams[i])
	}
	wg.Wait()

	return results
}
