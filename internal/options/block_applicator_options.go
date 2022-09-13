package options

import "time"

const (
	maxPendingBlocksDefault = 2500
	maxHeightDeltaDefault   = 60
	delayThresholdDefault   = time.Second * 4
	delayTimeoutDefault     = time.Second * 60
)

// BlockApplicatorOptions are options for BlockApplicator
type BlockApplicatorOptions struct {
	MaxPendingBlocks uint64
	MaxHeightDelta   uint64
	DelayThreshold   time.Duration
	DelayTimeout     time.Duration
}

// NewBlockApplicatorOptions returns default initialized BlockApplicatorOptions
func NewBlockApplicatorOptions() *BlockApplicatorOptions {
	return &BlockApplicatorOptions{
		MaxPendingBlocks: maxPendingBlocksDefault,
		MaxHeightDelta:   maxHeightDeltaDefault,
		DelayThreshold:   delayThresholdDefault,
		DelayTimeout:     delayTimeoutDefault,
	}
}
