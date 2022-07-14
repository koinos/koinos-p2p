package options

const (
	maxPendingBlocksDefault = 2500
)

// BlockApplicatorOptions are options for BlockApplicator
type BlockApplicatorOptions struct {
	MaxPendingBlocks uint64
}

// NewBlockApplicatorOptions returns default initialized BlockApplicatorOptions
func NewBlockApplicatorOptions() *BlockApplicatorOptions {
	return &BlockApplicatorOptions{
		MaxPendingBlocks: maxPendingBlocksDefault,
	}
}
