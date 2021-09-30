package options

import (
	"math"
	"time"
)

const (
	errorScoreDecayHalflifeDefault = time.Minute * 10
	errorScoreThresholdDefault     = 100000

	deserializationErrorScoreDefault        = 5000
	serializationErrorScoreDefault          = 0
	blockIrreversibilityErrorScoreDefault   = 100
	blockApplicationErrorScoreDefault       = 5000
	transactionApplicationErrorScoreDefault = 1000
	chainIDMismatchErrorScoreDefault        = uint64(math.MaxUint32)
	chainNotConnectedErrorScoreDefault      = uint64(math.MaxUint32)
	checkpointMismatchErrorScoreDefault     = uint64(math.MaxUint32)
	localRPCErrorScoreDefault               = 0
	peerRPCErrorScoreDefault                = 1000
	localRPCTimeoutErrorScoreDefault        = 0
	peerRPCTimeoutErrorScoreDefault         = 1000
	processRequestTimeoutErrorScoreDefault  = 0
	unknownErrorScoreDefault                = blockApplicationErrorScoreDefault
)

// PeerErrorHandlerOptions are options for PeerErrorHandler
type PeerErrorHandlerOptions struct {
	ErrorScoreDecayHalflife time.Duration
	ErrorScoreThreshold     uint64

	DeserializationErrorScore        uint64
	SerializationErrorScore          uint64
	BlockIrreversibilityErrorScore   uint64
	BlockApplicationErrorScore       uint64
	TransactionApplicationErrorScore uint64
	ChainIDMismatchErrorScore        uint64
	ChainNotConnectedErrorScore      uint64
	CheckpointMismatchErrorScore     uint64
	LocalRPCErrorScore               uint64
	PeerRPCErrorScore                uint64
	LocalRPCTimeoutErrorScore        uint64
	PeerRPCTimeoutErrorScore         uint64
	ProcessRequestTimeoutErrorScore  uint64
	UnknownErrorScore                uint64
}

// NewPeerErrorHandlerOptions returns default initialized PeerErrorHandlerOptions
func NewPeerErrorHandlerOptions() *PeerErrorHandlerOptions {
	return &PeerErrorHandlerOptions{
		ErrorScoreDecayHalflife:          errorScoreDecayHalflifeDefault,
		ErrorScoreThreshold:              errorScoreThresholdDefault,
		DeserializationErrorScore:        deserializationErrorScoreDefault,
		SerializationErrorScore:          serializationErrorScoreDefault,
		BlockIrreversibilityErrorScore:   blockIrreversibilityErrorScoreDefault,
		BlockApplicationErrorScore:       blockApplicationErrorScoreDefault,
		TransactionApplicationErrorScore: transactionApplicationErrorScoreDefault,
		ChainIDMismatchErrorScore:        chainIDMismatchErrorScoreDefault,
		ChainNotConnectedErrorScore:      chainNotConnectedErrorScoreDefault,
		CheckpointMismatchErrorScore:     checkpointMismatchErrorScoreDefault,
		LocalRPCErrorScore:               localRPCErrorScoreDefault,
		PeerRPCErrorScore:                peerRPCErrorScoreDefault,
		LocalRPCTimeoutErrorScore:        localRPCTimeoutErrorScoreDefault,
		PeerRPCTimeoutErrorScore:         peerRPCTimeoutErrorScoreDefault,
		ProcessRequestTimeoutErrorScore:  processRequestTimeoutErrorScoreDefault,
		UnknownErrorScore:                unknownErrorScoreDefault,
	}
}
