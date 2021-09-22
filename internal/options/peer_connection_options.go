package options

import (
	"time"

	"github.com/multiformats/go-multihash"
)

// Checkpoint required block to sync to peer
type Checkpoint struct {
	BlockHeight uint64
	BlockID     multihash.Multihash
}

const (
	localRPCTimeoutDefault       = time.Millisecond * 100
	remoteRPCTimeoutDefault      = time.Second
	blockRequestBatchSizeDefault = 1000
	blockRequestTimeoutDefault   = time.Second * 5
	handshakeRetryTimeDefault    = time.Second * 3
	syncedBlockDeltaDefault      = 5
	syncedPingTimeDefault        = time.Second * 10
)

// PeerConnectionOptions are options for PeerConnection
type PeerConnectionOptions struct {
	Checkpoints           []Checkpoint
	LocalRPCTimeout       time.Duration
	RemoteRPCTimeout      time.Duration
	BlockRequestBatchSize uint64
	BlockRequestTimeout   time.Duration
	HandshakeRetryTime    time.Duration
	SyncedBlockDelta      uint64
	SyncedPingTime        time.Duration
}

// NewPeerConnectionOptions returns default initialized PeerConnectionOptions
func NewPeerConnectionOptions() *PeerConnectionOptions {
	return &PeerConnectionOptions{
		Checkpoints:           make([]Checkpoint, 0),
		LocalRPCTimeout:       localRPCTimeoutDefault,
		RemoteRPCTimeout:      remoteRPCTimeoutDefault,
		BlockRequestBatchSize: blockRequestBatchSizeDefault,
		BlockRequestTimeout:   blockRequestTimeoutDefault,
		HandshakeRetryTime:    handshakeRetryTimeDefault,
		SyncedBlockDelta:      syncedBlockDeltaDefault,
		SyncedPingTime:        syncedPingTimeDefault,
	}
}
