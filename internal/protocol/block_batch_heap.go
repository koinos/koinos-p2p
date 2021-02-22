package protocol

import types "github.com/koinos/koinos-types-golang"

// BlockBatch is a batch of blocks
type BlockBatch struct {
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
	VectorBlockItems *types.VariableBlob
}

// BlockBatchHeap is a min-heap of block batch responses.
type BlockBatchHeap []BlockBatch

func (h BlockBatchHeap) Len() int           { return len(h) }
func (h BlockBatchHeap) Less(i, j int) bool { return h[i].StartBlockHeight < h[j].StartBlockHeight }
func (h BlockBatchHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push heap.Push
func (h *BlockBatchHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(BlockBatch))
}

// Pop head.Pop
func (h *BlockBatchHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
