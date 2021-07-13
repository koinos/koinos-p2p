package protocol

import (
	"errors"
)

var (
	// ErrGossip wraps all errors that take place during gossip
	ErrGossip = errors.New("gossip error")

	// ErrDeserialization represents any sort of error deserializing a type using Koinos types
	ErrDeserialization = errors.New("error during deserialization")

	// ErrBlockIrreversibility is when a block is earlier than irreversibility
	ErrBlockIrreversibility = errors.New("block is earlier than irreversibility block")

	// ErrBlockApplication represents any error applying the block in chain
	ErrBlockApplication = errors.New("block application failed")
)
