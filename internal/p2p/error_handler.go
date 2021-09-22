package p2p

import (
	"context"
	"errors"
	"math"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p-core/peer"
)

// PeerError represents an error originating from a peer
type PeerError struct {
	id  peer.ID
	err error
}

type errorScoreRecord struct {
	lastUpdate time.Time
	score      uint64
}

type canConnectRequest struct {
	id         peer.ID
	resultChan chan bool
}

// PeerErrorHandler handles PeerErrors and tracks errors over time
// to determine if a peer should be disconnected from
type PeerErrorHandler struct {
	errorScores        map[peer.ID]*errorScoreRecord
	disconnectPeerChan chan<- peer.ID
	peerErrorChan      <-chan PeerError
	canConnectChan     chan canConnectRequest

	opts options.PeerErrorHandlerOptions
}

// CanConnect to peer if the peer's error score is below the error score threshold
func (p *PeerErrorHandler) CanConnect(ctx context.Context, id peer.ID) bool {
	resultChan := make(chan bool, 1)
	p.canConnectChan <- canConnectRequest{
		id:         id,
		resultChan: resultChan,
	}

	select {
	case res := <-resultChan:
		return res
	case <-ctx.Done():
		return false
	}
}

func (p *PeerErrorHandler) handleCanConnect(id peer.ID) bool {
	if record, ok := p.errorScores[id]; ok {
		p.decayErrorScore(record)
		return record.score < p.opts.ErrorScoreThreshold
	}

	return true
}

func (p *PeerErrorHandler) handleError(ctx context.Context, peerErr PeerError) {
	log.Infof("Encountered peer error: %s, %s", peerErr.id, peerErr.err.Error())

	if record, ok := p.errorScores[peerErr.id]; ok {
		p.decayErrorScore(record)
		record.score += p.getScoreForError(peerErr.err)
	} else {
		p.errorScores[peerErr.id] = &errorScoreRecord{
			lastUpdate: time.Now(),
			score:      p.getScoreForError(peerErr.err),
		}
	}

	if p.errorScores[peerErr.id].score >= p.opts.ErrorScoreThreshold {
		go func() {
			select {
			case p.disconnectPeerChan <- peerErr.id:
			case <-ctx.Done():
			}
		}()
	}
}

func (p *PeerErrorHandler) getScoreForError(err error) uint64 {
	// These should be ordered from most common error to least
	switch {

	// Errors that are commonly expected during normal use or potential attack vectors
	case errors.Is(err, p2perrors.ErrTransactionApplication):
		return p.opts.TransactionApplicationErrorScore
	case errors.Is(err, p2perrors.ErrBlockApplication):
		return p.opts.BlockApplicationErrorScore
	case errors.Is(err, p2perrors.ErrDeserialization):
		return p.opts.DeserializationErrorScore
	case errors.Is(err, p2perrors.ErrBlockIrreversibility):
		return p.opts.BlockIrreversibilityErrorScore
	case errors.Is(err, p2perrors.ErrPeerRPC):
		return p.opts.PeerRPCErrorScore
	case errors.Is(err, p2perrors.ErrPeerRPCTimeout):
		return p.opts.PeerRPCTimeoutErrorScore

	// These errors are expected, but result in instant disconnection
	case errors.Is(err, p2perrors.ErrChainIDMismatch):
		return p.opts.ChainIDMismatchErrorScore
	case errors.Is(err, p2perrors.ErrChainNotConnected):
		return p.opts.ChainNotConnectedErrorScore

	// Errors that should only originate from the local process or local node
	case errors.Is(err, p2perrors.ErrLocalRPC):
		return p.opts.LocalRPCErrorScore
	case errors.Is(err, p2perrors.ErrLocalRPCTimeout):
		return p.opts.LocalRPCTimeoutErrorScore
	case errors.Is(err, p2perrors.ErrSerialization):
		return p.opts.SerializationErrorScore

	default:
		return p.opts.UnknownErrorScore
	}
}

func (p *PeerErrorHandler) decayErrorScore(record *errorScoreRecord) {
	decayConstant := math.Log(2) / float64(p.opts.ErrorScoreDecayHalflife)
	now := time.Now()
	record.score = uint64(float64(record.score) * math.Exp(-1*decayConstant*float64(now.Sub(record.lastUpdate))))
	record.lastUpdate = now
}

// Start processing peer errors
func (p *PeerErrorHandler) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case perr := <-p.peerErrorChan:
				p.handleError(ctx, perr)
			case req := <-p.canConnectChan:
				req.resultChan <- p.handleCanConnect(req.id)

			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewPeerErrorHandler creates a new PeerErrorHandler
func NewPeerErrorHandler(disconnectPeerChan chan<- peer.ID, peerErrorChan <-chan PeerError, opts options.PeerErrorHandlerOptions) *PeerErrorHandler {
	return &PeerErrorHandler{
		errorScores:        make(map[peer.ID]*errorScoreRecord),
		disconnectPeerChan: disconnectPeerChan,
		peerErrorChan:      peerErrorChan,
		canConnectChan:     make(chan canConnectRequest),
		opts:               opts,
	}
}
