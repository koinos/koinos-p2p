package p2p

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/libp2p/go-libp2p-core/peer"
)

type signalRequestBlocks struct{}

// PeerConnection handles the sync portion of a connection to a peer
type PeerConnection struct {
	id         peer.ID
	isSynced   bool
	gossipVote bool
	opts       *options.PeerConnectionOptions

	requestBlockChan chan signalRequestBlocks

	localRPC       rpc.LocalRPC
	peerRPC        rpc.RemoteRPC
	peerErrorChan  chan<- PeerError
	gossipVoteChan chan<- GossipVote
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

	if bytes.Compare(myChainID.ChainId, peerChainID) != 0 {
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

		if bytes.Compare(peerBlock, checkpoint.BlockID) != 0 {
			return p2perrors.ErrCheckpointMismatch
		}
	}

	return nil
}

func (p *PeerConnection) handleRequestBlocks(ctx context.Context) error {
	// Get my head info
	rpcContext, cancelGetForkHeads := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
	defer cancelGetForkHeads()
	forkHeads, err := p.localRPC.GetForkHeads(rpcContext)
	if err != nil {
		return err
	}

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelGetPeerHead()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= forkHeads.LastIrreversibleBlock.Height {
		p.isSynced = true
		return nil
	}

	// If LIB is 0, we are still at genesis and could connec to any chain
	if forkHeads.LastIrreversibleBlock.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelGetAncestorBlock := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
		defer cancelGetAncestorBlock()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height)
		if err != nil {
			return err
		}

		if bytes.Compare([]byte(ancestorBlock), []byte(forkHeads.LastIrreversibleBlock.Id)) != 0 {
			return p2perrors.ErrChainNotConnected
		}
	}

	blocksToRequest := peerHeadHeight - forkHeads.LastIrreversibleBlock.Height
	if blocksToRequest > p.opts.BlockRequestBatchSize {
		blocksToRequest = p.opts.BlockRequestBatchSize
	}

	// Request blocks
	rpcContext, cancelGetBlocks := context.WithTimeout(ctx, p.opts.BlockRequestTimeout)
	defer cancelGetBlocks()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height+1, uint32(blocksToRequest))
	if err != nil {
		return err
	}

	// Apply blocks to local node
	for _, block := range blocks {
		rpcContext, cancelApplyBlock := context.WithTimeout(ctx, time.Second)
		defer cancelApplyBlock()
		_, err = p.localRPC.ApplyBlock(rpcContext, &block)
		if err != nil {
			return fmt.Errorf("%w: %s", p2perrors.ErrBlockApplication, err.Error())
		}
	}

	// We will consider ourselves as syncing if we have more than 5 blocks to sync
	p.isSynced = peerHeadHeight-blocks[len(blocks)-1].Header.Height < p.opts.SyncedBlockDelta

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
		case <-p.requestBlockChan:
			err := p.handleRequestBlocks(ctx)
			if err != nil {
				time.AfterFunc(time.Second, p.requestBlocks)
				select {
				case p.peerErrorChan <- PeerError{id: p.id, err: err}:
				case <-ctx.Done():
				}
			} else {
				if p.gossipVote != p.isSynced {
					p.reportGossipVote(ctx)
				}
				if p.isSynced {
					time.AfterFunc(p.opts.SyncedPingTime, p.requestBlocks)
				} else {
					go p.requestBlocks()
				}
			}

		case <-ctx.Done():
			return
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
					p.peerErrorChan <- PeerError{id: p.id, err: err}
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
func NewPeerConnection(id peer.ID, localRPC rpc.LocalRPC, peerRPC rpc.RemoteRPC, peerErrorChan chan<- PeerError, gossipVoteChan chan<- GossipVote, opts *options.PeerConnectionOptions) *PeerConnection {
	return &PeerConnection{
		id:               id,
		isSynced:         false,
		gossipVote:       false,
		opts:             opts,
		requestBlockChan: make(chan signalRequestBlocks),
		localRPC:         localRPC,
		peerRPC:          peerRPC,
		peerErrorChan:    peerErrorChan,
		gossipVoteChan:   gossipVoteChan,
	}
}
