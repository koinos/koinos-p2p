package p2perrors

import (
	"errors"
)

var (
	// ErrDeserialization represents any sort of error deserializing a type using Koinos types
	ErrDeserialization = errors.New("error during deserialization")

	// ErrSerialization represents any sort of error deserializing a type using Koinos types
	ErrSerialization = errors.New("error during serialization")

	// ErrBlockIrreversibility is when a block is earlier than irreversibility
	ErrBlockIrreversibility = errors.New("block is earlier than irreversibility block")

	// ErrBlockApplication represents any error applying the block in chain
	ErrBlockApplication = errors.New("block application failed")

	// ErrTransactionApplication represents any error applying a transaction to the mem pool
	ErrTransactionApplication = errors.New("transaction application failed")

	// ErrChainIDMismatch represents the peer has a different chain id
	ErrChainIDMismatch = errors.New("chain id does not match peer's")

	// ErrChainNotConnected represents that progress can not be made from peer
	ErrChainNotConnected = errors.New("last irreversible block does not connect to peer chain")

	// ErrCheckpointMismatch represents peer does not have required checkpoint block
	ErrCheckpointMismatch = errors.New("peer does not have checkpoint block")

	// ErrLocalRPC represents an error occurred during a local rpc
	ErrLocalRPC = errors.New("local RPC error")

	// ErrPeerRPC represents an error occurred during a peer rpc
	ErrPeerRPC = errors.New("peer RPC error")

	// ErrLocalRPCTimeout represents a local rpc timed out
	ErrLocalRPCTimeout = errors.New("local RPC request timed out")

	// ErrPeerRPCTimeout represents a peer rpc timed out
	ErrPeerRPCTimeout = errors.New("peer RPC request timed out")

	// ErrProcessRequestTimeout represents an in process asynchronous request time out
	ErrProcessRequestTimeout = errors.New("in process request timed out")

	// ErrUnknownPreviousBlock represents when a block's previous block cannot be found
	ErrUnknownPreviousBlock = errors.New("previous block does not exist")

	// ErrBlockApplicationTimeout represents when a block application timed out
	ErrBlockApplicationTimeout = errors.New("block application timed out")

	// ErrMaxPendingBlocks represents when too many blocks are pending application
	ErrMaxPendingBlocks = errors.New("max blocks are pending application")

	// ErrForkBomb represents when too many forks from a producer is detected
	ErrForkBomb = errors.New("unacceptable number of forks on the same parent for a single producer")

	// ErrMaxHeight represents when a block application is requested that has too high of a height
	ErrMaxHeight = errors.New("block height exceeds max height")

	// ErrDuplicateTransaction represents when a transaction is already in the transaction cache
	ErrDuplicateTransaction = errors.New("transaction already exists in transaction cache")
)
