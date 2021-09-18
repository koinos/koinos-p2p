package options

import "github.com/multiformats/go-multihash"

// Checkpoint required block to sync to peer
type Checkpoint struct {
	BlockHeight uint64
	BlockID     multihash.Multihash
}

// PeerConnectionOptions are options for PeerConnection
type PeerConnectionOptions struct {
	Checkpoints []Checkpoint
}

// NewPeerConnectionOptions returns default initialized PeerConnectionOptions
func NewPeerConnectionOptions() *PeerConnectionOptions {
	return &PeerConnectionOptions{}
}
