package options

import "time"

const (
	errorScoreDecayHalflifeDefault = time.Minute * 10
	errorScoreThresholdDefault     = 100000

	gossipErrorScoreDefault               = 5000
	deserializationErrorScoreDefault      = 5000
	blockIrreversibilityErrorScoreDefault = 100
	blockApplicationErrorScoreDefault     = 5000
	chainIDMismatchErrorScoreDefault      = uint64(^uint32(0))
	chainNotConnectedErrorScoreDefault    = uint64(^uint32(0))
	localRPCTimeoutErrorScoreDefault      = 0
	peerRPCTimeoutErrorScoreDefault       = 1000
	unknownErrorScoreDefault              = blockApplicationErrorScoreDefault
)

// PeerErrorHandlerOptions are options for PeerErrorHandler
type PeerErrorHandlerOptions struct {
	ErrorScoreDecayHalflife time.Duration
	ErrorScoreThreshold     uint64

	GossipErrorScore               uint64
	DeserializationErrorScore      uint64
	BlockIrreversibilityErrorScore uint64
	BlockApplicationErrorScore     uint64
	ChainIDMismatchErrorScore      uint64
	ChainNotConnectedErrorScore    uint64
	LocalRPCTimeoutErrorScore      uint64
	PeerRPCTimeoutErrorScore       uint64
	UnknownErrorScore              uint64
}

// NewPeerErrorHandlerOptions returns default initialized PeerErrorHandlerOptions
func NewPeerErrorHandlerOptions() *PeerErrorHandlerOptions {
	return &PeerErrorHandlerOptions{
		ErrorScoreDecayHalflife:        errorScoreDecayHalflifeDefault,
		ErrorScoreThreshold:            errorScoreThresholdDefault,
		GossipErrorScore:               gossipErrorScoreDefault,
		DeserializationErrorScore:      deserializationErrorScoreDefault,
		BlockIrreversibilityErrorScore: blockIrreversibilityErrorScoreDefault,
		BlockApplicationErrorScore:     blockApplicationErrorScoreDefault,
		ChainIDMismatchErrorScore:      chainIDMismatchErrorScoreDefault,
		ChainNotConnectedErrorScore:    chainNotConnectedErrorScoreDefault,
		LocalRPCTimeoutErrorScore:      localRPCTimeoutErrorScoreDefault,
		PeerRPCTimeoutErrorScore:       peerRPCTimeoutErrorScoreDefault,
		UnknownErrorScore:              unknownErrorScoreDefault,
	}
}
