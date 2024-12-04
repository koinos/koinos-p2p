package options

import "time"

const (
	blockTimeoutDefault       = 3 * time.Second
	transactionTimeoutDefault = 1 * time.Second
)

// GossipOptions are options for Gossip
type GossipOptions struct {
	BlockTimeout       time.Duration
	TransactionTimeout time.Duration
}

// New GossipOptions returns default initialized GossipOptions
func NewGossipOptions() *GossipOptions {
	return &GossipOptions{
		BlockTimeout:       blockTimeoutDefault,
		TransactionTimeout: transactionTimeoutDefault,
	}
}
