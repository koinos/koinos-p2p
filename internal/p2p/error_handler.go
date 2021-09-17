package p2p

import (
	"context"
	"errors"
	"math"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p-core/network"
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

type PeerErrorHandler struct {
	errorScores    map[peer.ID]*errorScoreRecord
	peerNetwork    network.Network
	peerErrorChan  <-chan PeerError
	canConnectChan chan canConnectRequest

	opts options.PeerErrorHandlerOptions
}

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
		return record.score <= p.opts.ErrorScoreThreshold
	}

	return true
}

func (p *PeerErrorHandler) handleError(peerErr PeerError) {
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
		p.peerNetwork.ClosePeer(peerErr.id)
	}
}

func (p *PeerErrorHandler) getScoreForError(err error) uint64 {
	switch {
	case errors.Is(err, p2perrors.ErrGossip):
		return p.opts.GossipErrorScore
	case errors.Is(err, p2perrors.ErrDeserialization):
		return p.opts.DeserializationErrorScore
	case errors.Is(err, p2perrors.ErrBlockIrreversibility):
		return p.opts.BlockIrreversibilityErrorScore
	case errors.Is(err, p2perrors.ErrBlockApplication):
		return p.opts.BlockApplicationErrorScore
	case errors.Is(err, p2perrors.ErrChainIDMismatch):
		return p.opts.ChainIDMismatchErrorScore
	case errors.Is(err, p2perrors.ErrChainNotConnected):
		return p.opts.ChainNotConnectedErrorScore
	case errors.Is(err, p2perrors.ErrLocalRPCTimeout):
		return p.opts.LocalRPCTimeoutErrorScore
	case errors.Is(err, p2perrors.ErrPeerRPCTimeout):
		return p.opts.PeerRPCTimeoutErrorScore
	default:
		return p.opts.UnknownErrorScore
	}
}

func (p *PeerErrorHandler) decayErrorScore(record *errorScoreRecord) {
	decay_constant := float64(p.opts.ErrorScoreDecayHalflife) / math.Log(2)
	now := time.Now()
	record.score = uint64(float64(record.score) * math.Exp(-1*decay_constant*float64(now.Sub(record.lastUpdate))))
	record.lastUpdate = now
}

func (p *PeerErrorHandler) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case perr := <-p.peerErrorChan:
				p.handleError(perr)
			case req := <-p.canConnectChan:
				req.resultChan <- p.handleCanConnect(req.id)

			case <-ctx.Done():
				return
			}
		}
	}()
}

func NewPeerErrorHandler(peerNetwork network.Network, peerErrorChan chan PeerError, opts options.PeerErrorHandlerOptions) *PeerErrorHandler {
	return &PeerErrorHandler{
		errorScores:    make(map[peer.ID]*errorScoreRecord),
		peerNetwork:    peerNetwork,
		peerErrorChan:  peerErrorChan,
		canConnectChan: make(chan canConnectRequest),
		opts:           opts,
	}
}
