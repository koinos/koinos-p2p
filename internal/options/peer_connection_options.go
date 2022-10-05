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
	localRPCTimeoutDefault       = time.Second * 6
	applicatorTimeoutDefault     = localRPCTimeoutDefault * 2
	remoteRPCTimeoutDefault      = time.Second * 6
	blockRequestBatchSizeDefault = 1000
	blockRequestTimeoutDefault   = time.Second * time.Duration(blockRequestBatchSizeDefault * 0.3) // Roughly calculated considering 30Mbps minimum upload speed and 1MB blocks
	handshakeRetryTimeDefault    = time.Second * 6
	syncedBlockDeltaDefault      = 5
	syncedPingTimeDefault        = time.Second * 10
)

// PeerConnectionOptions are options for PeerConnection
type PeerConnectionOptions struct {
	Checkpoints           []Checkpoint
	LocalRPCTimeout       time.Duration
	ApplicatorTimeout     time.Duration
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
		ApplicatorTimeout:     applicatorTimeoutDefault,
		RemoteRPCTimeout:      remoteRPCTimeoutDefault,
		BlockRequestBatchSize: blockRequestBatchSizeDefault,
		BlockRequestTimeout:   blockRequestTimeoutDefault,
		HandshakeRetryTime:    handshakeRetryTimeDefault,
		SyncedBlockDelta:      syncedBlockDeltaDefault,
		SyncedPingTime:        syncedPingTimeDefault,
	}
}
