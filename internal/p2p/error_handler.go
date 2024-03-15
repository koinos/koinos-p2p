package p2p

import (
	"context"
	"errors"
	"math"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// PeerAddressProvider return's the peers remote address given a peer ID
type PeerAddressProvider interface {
	GetPeerAddress(ctx context.Context, id peer.ID) ma.Multiaddr
}

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
	addr       ma.Multiaddr
	resultChan chan<- bool
}

type getPeerErrorScoreRequest struct {
	addr       ma.Multiaddr
	resultChan chan<- uint64
}

// PeerErrorHandler handles PeerErrors and tracks errors over time
// to determine if a peer should be disconnected from
type PeerErrorHandler struct {
	errorScores           map[string]*errorScoreRecord
	disconnectPeerChan    chan<- peer.ID
	peerErrorChan         <-chan PeerError
	canConnectChan        chan canConnectRequest
	getPeerErrorScoreChan chan getPeerErrorScoreRequest
	addrProvider          PeerAddressProvider

	opts options.PeerErrorHandlerOptions
}

// CanConnect to peer if the peer's error score is below the error score threshold
func (p *PeerErrorHandler) CanConnect(ctx context.Context, id peer.ID) bool {
	if addr := p.addrProvider.GetPeerAddress(ctx, id); addr != nil {
		return p.CanConnectAddr(ctx, addr)
	}

	return true
}

// CanConnectAddr to peer if the peer's error score is below the error score threshold
func (p *PeerErrorHandler) CanConnectAddr(ctx context.Context, addr ma.Multiaddr) bool {
	resultChan := make(chan bool, 1)
	p.canConnectChan <- canConnectRequest{
		addr:       addr,
		resultChan: resultChan,
	}

	select {
	case res := <-resultChan:
		return res
	case <-ctx.Done():
		return false
	}
}

// GetPeerErrorScore returns the current error score for a given peer ID
func (p *PeerErrorHandler) GetPeerErrorScore(ctx context.Context, id peer.ID) uint64 {
	resultChan := make(chan uint64, 1)
	if addr := p.addrProvider.GetPeerAddress(ctx, id); addr != nil {
		p.getPeerErrorScoreChan <- getPeerErrorScoreRequest{addr, resultChan}

		select {
		case res := <-resultChan:
			return res
		case <-ctx.Done():
		}
	}

	return 0
}

func (p *PeerErrorHandler) GetOptions() options.PeerErrorHandlerOptions {
	return p.opts
}

func (p *PeerErrorHandler) handleCanConnect(addr ma.Multiaddr) bool {
	if record, ok := p.errorScores[ma.Split(addr)[0].String()]; ok {
		p.decayErrorScore(record)
		return record.score < p.opts.ErrorScoreReconnectThreshold
	}

	return true
}

func (p *PeerErrorHandler) handleGetPeerErrorScore(addr ma.Multiaddr) uint64 {
	if record, ok := p.errorScores[ma.Split(addr)[0].String()]; ok {
		p.decayErrorScore(record)
		return record.score
	}

	return 0
}

func (p *PeerErrorHandler) handleError(ctx context.Context, peerErr PeerError) {
	if addr := p.addrProvider.GetPeerAddress(ctx, peerErr.id); addr != nil {
		ipAddr := ma.Split(addr)[0].String()
		if record, ok := p.errorScores[ipAddr]; ok {
			p.decayErrorScore(record)
			record.score += p.getScoreForError(peerErr.err)
		} else {
			p.errorScores[ipAddr] = &errorScoreRecord{
				lastUpdate: time.Now(),
				score:      p.getScoreForError(peerErr.err),
			}
		}

		log.Infof("Encountered peer error: %s, %s. Current error score: %v", peerErr.id, peerErr.err.Error(), p.errorScores[ipAddr].score)

		if p.errorScores[ipAddr].score >= p.opts.ErrorScoreThreshold {
			go func() {
				select {
				case p.disconnectPeerChan <- peerErr.id:
				case <-ctx.Done():
				}
			}()
		}
	}
}

func (p *PeerErrorHandler) getScoreForError(err error) uint64 {
	// These should be ordered from most common error to least
	switch {

	// Errors that are commonly expected during normal use or potential attack vectors
	case errors.Is(err, p2perrors.ErrTransactionApplication):
		return p.opts.TransactionApplicationErrorScore
	case errors.Is(err, p2perrors.ErrInvalidNonce):
		return p.opts.InvalidNonceErrorScore
	case errors.Is(err, p2perrors.ErrBlockApplication):
		return p.opts.BlockApplicationErrorScore
	case errors.Is(err, p2perrors.ErrUnknownPreviousBlock):
		return p.opts.UnknownPreviousBlockErrorScore
	case errors.Is(err, p2perrors.ErrBlockApplicationTimeout):
		return p.opts.BlockApplicationTimeoutErrorScore
	case errors.Is(err, p2perrors.ErrMaxPendingBlocks):
		return p.opts.MaxPendingBlocksErrorScore
	case errors.Is(err, p2perrors.ErrMaxHeight):
		return p.opts.MaxHeightErrorScore
	case errors.Is(err, p2perrors.ErrDeserialization):
		return p.opts.DeserializationErrorScore
	case errors.Is(err, p2perrors.ErrBlockIrreversibility):
		return p.opts.BlockIrreversibilityErrorScore
	case errors.Is(err, p2perrors.ErrPeerRPC):
		return p.opts.PeerRPCErrorScore
	case errors.Is(err, p2perrors.ErrPeerRPCTimeout):
		return p.opts.PeerRPCTimeoutErrorScore
	case errors.Is(err, p2perrors.ErrForkBomb):
		return p.opts.ForkBombErrorScore

	// These errors are expected, but result in instant disconnection
	case errors.Is(err, p2perrors.ErrChainIDMismatch):
		return p.opts.ChainIDMismatchErrorScore
	case errors.Is(err, p2perrors.ErrChainNotConnected):
		return p.opts.ChainNotConnectedErrorScore
	case errors.Is(err, p2perrors.ErrCheckpointMismatch):
		return p.opts.CheckpointMismatchErrorScore

	// Errors that should only originate from the local process or local node
	case errors.Is(err, p2perrors.ErrLocalRPC):
		return p.opts.LocalRPCErrorScore
	case errors.Is(err, p2perrors.ErrLocalRPCTimeout):
		return p.opts.LocalRPCTimeoutErrorScore
	case errors.Is(err, p2perrors.ErrSerialization):
		return p.opts.SerializationErrorScore
	case errors.Is(err, p2perrors.ErrProcessRequestTimeout):
		return p.opts.ProcessRequestTimeoutErrorScore

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

// InterceptPeerDial implements the libp2p ConnectionGater interface
func (p *PeerErrorHandler) InterceptPeerDial(pid peer.ID) bool {
	return p.CanConnect(context.Background(), pid)
}

// InterceptAddrDial implements the libp2p ConnectionGater interface
func (p *PeerErrorHandler) InterceptAddrDial(_ peer.ID, addr ma.Multiaddr) bool {
	return p.CanConnectAddr(context.Background(), addr)
}

// InterceptAccept implements the libp2p ConnectionGater interface
func (p *PeerErrorHandler) InterceptAccept(conn network.ConnMultiaddrs) bool {
	return p.CanConnectAddr(context.Background(), conn.RemoteMultiaddr())
}

// InterceptSecured implements the libp2p ConnectionGater interface
func (p *PeerErrorHandler) InterceptSecured(_ network.Direction, _ peer.ID, conn network.ConnMultiaddrs) bool {
	return p.CanConnectAddr(context.Background(), conn.RemoteMultiaddr())
}

// InterceptUpgraded implements the libp2p ConnectionGater interface
func (p *PeerErrorHandler) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}

// Start processing peer errors
func (p *PeerErrorHandler) Start(ctx context.Context) {
	if p.addrProvider == nil {
		return
	}

	go func() {
		for {
			select {
			case perr := <-p.peerErrorChan:
				p.handleError(ctx, perr)
			case req := <-p.canConnectChan:
				req.resultChan <- p.handleCanConnect(req.addr)
			case req := <-p.getPeerErrorScoreChan:
				req.resultChan <- p.handleGetPeerErrorScore(req.addr)

			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewPeerErrorHandler creates a new PeerErrorHandler
func NewPeerErrorHandler(
	disconnectPeerChan chan<- peer.ID,
	peerErrorChan <-chan PeerError,
	opts options.PeerErrorHandlerOptions) *PeerErrorHandler {

	return &PeerErrorHandler{
		errorScores:           make(map[string]*errorScoreRecord),
		disconnectPeerChan:    disconnectPeerChan,
		peerErrorChan:         peerErrorChan,
		canConnectChan:        make(chan canConnectRequest, 10),
		getPeerErrorScoreChan: make(chan getPeerErrorScoreRequest, 10),
		opts:                  opts,
	}
}

// SetPeerStore of the PeerErrorHandler. This must be called before starting
// the error score and is a separate function because PeerErrorHandler can
// be passed in to a libp2p Host during construction as a ConnectionGater.
// But the Host to be created is the PeerStore the PeerErrorHandler requires.
func (p *PeerErrorHandler) SetPeerAddressProvider(addrProvider PeerAddressProvider) {
	p.addrProvider = addrProvider
}
