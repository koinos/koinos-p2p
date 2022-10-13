package options

import (
	"time"
)

const (
	errorScoreDecayHalflifeDefault      = time.Minute * 10
	errorScoreThresholdDefault          = 100000
	errorScoreReconnectThresholdDefault = errorScoreThresholdDefault / 2

	deserializationErrorScoreDefault         = 5000
	serializationErrorScoreDefault           = 0
	blockIrreversibilityErrorScoreDefault    = 100
	blockApplicationErrorScoreDefault        = 5000
	unknownPreviousBlockErrorScoreDefault    = 5000
	blockApplicationTimeoutErrorScoreDefault = 5000
	maxPendingBlocksErrorScoreDefault        = 1000
	transactionApplicationErrorScoreDefault  = 1000
	chainIDMismatchErrorScoreDefault         = errorScoreThresholdDefault * 2
	chainNotConnectedErrorScoreDefault       = errorScoreThresholdDefault * 2
	checkpointMismatchErrorScoreDefault      = errorScoreThresholdDefault * 2
	localRPCErrorScoreDefault                = 0
	peerRPCErrorScoreDefault                 = 2500
	localRPCTimeoutErrorScoreDefault         = 0
	peerRPCTimeoutErrorScoreDefault          = 10000
	processRequestTimeoutErrorScoreDefault   = 0
	forkBombErrorScoreDefault                = errorScoreThresholdDefault * 2
	maxHeightErrorScoreDefault               = blockApplicationErrorScoreDefault
	unknownErrorScoreDefault                 = blockApplicationErrorScoreDefault
)

// PeerErrorHandlerOptions are options for PeerErrorHandler
type PeerErrorHandlerOptions struct {
	ErrorScoreDecayHalflife      time.Duration
	ErrorScoreThreshold          uint64
	ErrorScoreReconnectThreshold uint64

	DeserializationErrorScore         uint64
	SerializationErrorScore           uint64
	BlockIrreversibilityErrorScore    uint64
	BlockApplicationErrorScore        uint64
	UnknownPreviousBlockErrorScore    uint64
	BlockApplicationTimeoutErrorScore uint64
	MaxPendingBlocksErrorScore        uint64
	TransactionApplicationErrorScore  uint64
	ChainIDMismatchErrorScore         uint64
	ChainNotConnectedErrorScore       uint64
	CheckpointMismatchErrorScore      uint64
	LocalRPCErrorScore                uint64
	PeerRPCErrorScore                 uint64
	LocalRPCTimeoutErrorScore         uint64
	PeerRPCTimeoutErrorScore          uint64
	ProcessRequestTimeoutErrorScore   uint64
	ForkBombErrorScore                uint64
	MaxHeightErrorScore               uint64
	UnknownErrorScore                 uint64
}

// NewPeerErrorHandlerOptions returns default initialized PeerErrorHandlerOptions
func NewPeerErrorHandlerOptions() *PeerErrorHandlerOptions {
	return &PeerErrorHandlerOptions{
		ErrorScoreDecayHalflife:           errorScoreDecayHalflifeDefault,
		ErrorScoreThreshold:               errorScoreThresholdDefault,
		ErrorScoreReconnectThreshold:      errorScoreReconnectThresholdDefault,
		DeserializationErrorScore:         deserializationErrorScoreDefault,
		SerializationErrorScore:           serializationErrorScoreDefault,
		BlockIrreversibilityErrorScore:    blockIrreversibilityErrorScoreDefault,
		BlockApplicationErrorScore:        blockApplicationErrorScoreDefault,
		UnknownPreviousBlockErrorScore:    unknownPreviousBlockErrorScoreDefault,
		BlockApplicationTimeoutErrorScore: blockApplicationTimeoutErrorScoreDefault,
		MaxPendingBlocksErrorScore:        maxPendingBlocksErrorScoreDefault,
		TransactionApplicationErrorScore:  transactionApplicationErrorScoreDefault,
		ChainIDMismatchErrorScore:         chainIDMismatchErrorScoreDefault,
		ChainNotConnectedErrorScore:       chainNotConnectedErrorScoreDefault,
		CheckpointMismatchErrorScore:      checkpointMismatchErrorScoreDefault,
		LocalRPCErrorScore:                localRPCErrorScoreDefault,
		PeerRPCErrorScore:                 peerRPCErrorScoreDefault,
		LocalRPCTimeoutErrorScore:         localRPCTimeoutErrorScoreDefault,
		PeerRPCTimeoutErrorScore:          peerRPCTimeoutErrorScoreDefault,
		ProcessRequestTimeoutErrorScore:   processRequestTimeoutErrorScoreDefault,
		ForkBombErrorScore:                forkBombErrorScoreDefault,
		MaxHeightErrorScore:               maxHeightErrorScoreDefault,
		UnknownErrorScore:                 unknownErrorScoreDefault,
	}
}
