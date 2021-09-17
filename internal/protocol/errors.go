package protocol

import (
	"errors"

	"github.com/libp2p/go-libp2p-core/peer"
)

// PeerError represents an error originating from a peer
type PeerError struct {
	id  peer.ID
	err error
}

var (
	// ErrGossip wraps all errors that take place during gossip
	ErrGossip = errors.New("gossip error")

	// ErrDeserialization represents any sort of error deserializing a type using Koinos types
	ErrDeserialization = errors.New("error during deserialization")

	// ErrBlockIrreversibility is when a block is earlier than irreversibility
	ErrBlockIrreversibility = errors.New("block is earlier than irreversibility block")

	// ErrBlockApplication represents any error applying the block in chain
	ErrBlockApplication = errors.New("block application failed")

	// ErrChainIDMismatch represents the peer has a different chain id
	ErrChainIDMismatch = errors.New("chain id does not match peer's")

	// ErrChainNotConnected represents that progress can not be made from peer
	ErrChainNotConnected = errors.New("last irreversible block does not connect to peer chain")

	ErrLocalRPCTimeout = errors.New("local RPC request timed out")

	ErrPeerRPCTimeout = errors.New("peer RPC request timed out")
)
