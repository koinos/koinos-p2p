package options

import (
	"runtime"
	"time"
)

func min(x, y int) int {
	if x < y {
		return x
	}

	return y
}

const (
	maxPendingBlocksDefault = 2500
	maxHeightDeltaDefault   = 60
	delayThresholdDefault   = time.Second * 4
	delayTimeoutDefault     = time.Second * 60
	applicationJobsDefault  = 8
)

// ApplicatorOptions are options for Applicator
type ApplicatorOptions struct {
	MaxPendingBlocks uint64
	MaxHeightDelta   uint64
	DelayThreshold   time.Duration
	DelayTimeout     time.Duration
	ApplicationJobs  int
}

// NewApplicatorOptions returns default initialized ApplicatorOptions
func NewApplicatorOptions() *ApplicatorOptions {
	return &ApplicatorOptions{
		MaxPendingBlocks: maxPendingBlocksDefault,
		MaxHeightDelta:   maxHeightDeltaDefault,
		DelayThreshold:   delayThresholdDefault,
		DelayTimeout:     delayTimeoutDefault,
		ApplicationJobs:  min(applicationJobsDefault, runtime.NumCPU()),
	}
}
