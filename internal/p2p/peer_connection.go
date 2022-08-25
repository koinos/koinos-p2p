package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type signalRequestBlocks struct{}

// PeerConnection handles the sync portion of a connection to a peer
type PeerConnection struct {
	id         peer.ID
	isSynced   bool
	gossipVote bool
	opts       *options.PeerConnectionOptions

	requestBlockChan chan signalRequestBlocks

	libProvider     LastIrreversibleBlockProvider
	localRPC        rpc.LocalRPC
	peerRPC         rpc.RemoteRPC
	blockApplicator *BlockApplicator
	peerErrorChan   chan<- PeerError
	gossipVoteChan  chan<- GossipVote
}

func (p *PeerConnection) requestBlocks() {
	p.requestBlockChan <- signalRequestBlocks{}
}

func (p *PeerConnection) handshake(ctx context.Context) error {
	// Get my chain id
	rpcContext, cancelLocalGetChainID := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
	defer cancelLocalGetChainID()
	myChainID, err := p.localRPC.GetChainID(rpcContext)
	if err != nil {
		return err
	}

	// Get peer's chain id
	rpcContext, cancelPeerGetChainID := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelPeerGetChainID()
	peerChainID, err := p.peerRPC.GetChainID(rpcContext)
	if err != nil {
		return err
	}

	if !bytes.Equal(myChainID.ChainId, peerChainID) {
		return p2perrors.ErrChainIDMismatch
	}

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelGetPeerHead()
	peerHeadID, _, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	for _, checkpoint := range p.opts.Checkpoints {
		rpcContext, cancel := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
		defer cancel()
		peerBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, checkpoint.BlockHeight)
		if err != nil {
			return err
		}

		if !bytes.Equal(peerBlock, checkpoint.BlockID) {
			return p2perrors.ErrCheckpointMismatch
		}
	}

	return nil
}

func (p *PeerConnection) handleRequestBlocks(ctx context.Context) error {
	// Get my last irreversible block
	lib := p.libProvider.GetLastIrreversibleBlock()

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelGetPeerHead()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= lib.Height {
		p.isSynced = true
		return nil
	}

	// If we already know about the peer's head block, don't request anything
	rpcContext, cancelCheckPeerHeadBlock := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
	defer cancelCheckPeerHeadBlock()
	localBlocks, err := p.localRPC.GetBlocksByID(rpcContext, []multihash.Multihash{peerHeadID})
	if err != nil {
		return err
	}

	if len(localBlocks.BlockItems) != 1 {
		return fmt.Errorf("%w: unexpected number of block items returned", p2perrors.ErrLocalRPC)
	}

	// We already know of the peer's head block, don't request anything and consider ourselves sycned
	if localBlocks.BlockItems[0].BlockHeight != 0 {
		p.isSynced = true
		return nil
	}

	// If LIB is 0, we are still at genesis and could connect to any chain
	if lib.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelGetAncestorBlock := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
		defer cancelGetAncestorBlock()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, lib.Height)
		if err != nil {
			return err
		}

		if !bytes.Equal([]byte(ancestorBlock), lib.Id) {
			return p2perrors.ErrChainNotConnected
		}
	}

	blocksToRequest := peerHeadHeight - lib.Height
	if blocksToRequest > p.opts.BlockRequestBatchSize {
		blocksToRequest = p.opts.BlockRequestBatchSize
	}

	// Request blocks
	if blocksToRequest == p.opts.BlockRequestBatchSize {
		log.Infof("Requesting blocks %v-%v from peer %s", lib.Height+1, lib.Height+1+blocksToRequest, p.id)
	}

	rpcContext, cancelGetBlocks := context.WithTimeout(ctx, p.opts.BlockRequestTimeout)
	defer cancelGetBlocks()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, lib.Height+1, uint32(blocksToRequest))
	if err != nil {
		return err
	}

	// Apply blocks to local node
	for i := range blocks {
		applicatorContext, cancelApplyBlock := context.WithTimeout(ctx, p.opts.BlockApplicatorTimeout)
		defer cancelApplyBlock()

		err = p.blockApplicator.ApplyBlock(applicatorContext, &blocks[i])

		if err != nil {
			// If it was a local RPC timeout, do not wrap it
			if errors.Is(err, p2perrors.ErrLocalRPCTimeout) {
				return err
			}

			// If we are applying a now irreversible block, it is probably we synced further with another peer,
			// just keep applying blocks until we are caught up or we encounter a different error.
			if errors.Is(err, p2perrors.ErrBlockIrreversibility) {
				continue
			}
		}
	}

	return nil
}

func (p *PeerConnection) reportGossipVote(ctx context.Context) {
	p.gossipVote = p.isSynced
	go func() {
		select {
		case p.gossipVoteChan <- GossipVote{p.id, p.gossipVote}:
		case <-ctx.Done():
		}
	}()
}

func (p *PeerConnection) connectionLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.requestBlockChan:
			err := p.handleRequestBlocks(ctx)
			if err != nil {
				go time.AfterFunc(time.Second, p.requestBlocks)
				go func() {
					select {
					case p.peerErrorChan <- PeerError{id: p.id, err: err}:
					case <-ctx.Done():
					}
				}()
			} else {
				if p.gossipVote != p.isSynced {
					p.reportGossipVote(ctx)
				}
				if p.isSynced {
					go time.AfterFunc(p.opts.SyncedPingTime, p.requestBlocks)
				} else {
					go p.requestBlocks()
				}
			}
		}
	}
}

// Start syncing to the peer
func (p *PeerConnection) Start(ctx context.Context) {
	go func() {
		for {
			// Does the handshake in a loop until we are successful
			// or the connection is closed, sleeping between attempts
			err := p.handshake(ctx)
			if err != nil {
				go func() {
					select {
					case p.peerErrorChan <- PeerError{id: p.id, err: err}:
					case <-ctx.Done():
					}
				}()
			} else {
				p.reportGossipVote(ctx)
				go p.connectionLoop(ctx)
				go p.requestBlocks()
				return
			}
			select {
			case <-time.After(p.opts.HandshakeRetryTime):
			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewPeerConnection creates a PeerConnection
func NewPeerConnection(
	id peer.ID,
	libProvider LastIrreversibleBlockProvider,
	localRPC rpc.LocalRPC,
	peerRPC rpc.RemoteRPC,
	peerErrorChan chan<- PeerError,
	gossipVoteChan chan<- GossipVote,
	opts *options.PeerConnectionOptions,
	blockApplicator *BlockApplicator) *PeerConnection {
	return &PeerConnection{
		id:         id,
		isSynced:   false,
		gossipVote: false,
		opts:       opts,
		requestBlockChan: make(chan signalRequestBlocks),
		libProvider:      libProvider,
		localRPC:         localRPC,
		peerRPC:          peerRPC,
		blockApplicator:  blockApplicator,
		peerErrorChan:    peerErrorChan,
		gossipVoteChan:   gossipVoteChan,
	}
}
