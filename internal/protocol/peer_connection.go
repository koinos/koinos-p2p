package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/libp2p/go-libp2p-core/peer"
)

type signalRequestBlocks struct{}

// PeerConnection handles the sync portion of a connection to a peer
type PeerConnection struct {
	id        peer.ID
	isSyncing bool

	requestBlockChan chan signalRequestBlocks

	localRPC      rpc.LocalRPC
	peerRPC       rpc.RemoteRPC
	peerErrorChan chan<- PeerError
}

func (p *PeerConnection) requestBlocks() {
	p.requestBlockChan <- signalRequestBlocks{}
}

func (p *PeerConnection) handshake(ctx context.Context) error {
	// Get my chain id
	rpcContext, cancelLocalGetChainID := context.WithTimeout(ctx, time.Second*3)
	defer cancelLocalGetChainID()
	myChainID, err := p.localRPC.GetChainID(rpcContext)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrLocalRPCTimeout
		}
		return err
	}

	// Get peer's chain id
	rpcContext, cancelPeerGetChainID := context.WithTimeout(ctx, time.Second*3)
	defer cancelPeerGetChainID()
	peerChainID, err := p.peerRPC.GetChainID(rpcContext)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrPeerRPCTimeout
		}
		return err
	}

	if bytes.Compare(myChainID.ChainId, *peerChainID) != 0 {
		return ErrChainIDMismatch
	}

	// TODO: Check checkpoints (#165)

	return nil
}

func (p *PeerConnection) handleRequestBlocks(ctx context.Context) error {
	// Get my head info
	rpcContext, cancelGetForkHeads := context.WithTimeout(ctx, time.Second*3)
	defer cancelGetForkHeads()
	forkHeads, err := p.localRPC.GetForkHeads(rpcContext)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrLocalRPCTimeout
		}
		return err
	}

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, time.Second*3)
	defer cancelGetPeerHead()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrPeerRPCTimeout
		}
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= forkHeads.LastIrreversibleBlock.Height {
		p.isSyncing = false
		return nil
	}

	// If LIB is 0, we are still at genesis and could connec to any chain
	if forkHeads.LastIrreversibleBlock.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelGetAncestorBlock := context.WithTimeout(ctx, time.Second*3)
		defer cancelGetAncestorBlock()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return ErrPeerRPCTimeout
			}
			return err
		}

		if bytes.Compare([]byte(*ancestorBlock), []byte(forkHeads.LastIrreversibleBlock.Id)) != 0 {
			return ErrChainNotConnected
		}
	}

	blocksToRequest := peerHeadHeight - forkHeads.LastIrreversibleBlock.Height
	if blocksToRequest > 500 {
		blocksToRequest = 500
	}

	// Request blocks
	rpcContext, cancelGetBlocks := context.WithTimeout(ctx, time.Second*5)
	defer cancelGetBlocks()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height+1, uint32(blocksToRequest))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrPeerRPCTimeout
		}
		return err
	}

	// Apply blocks to local node
	for _, block := range blocks {
		rpcContext, cancelApplyBlock := context.WithTimeout(ctx, time.Second)
		defer cancelApplyBlock()
		_, err = p.localRPC.ApplyBlock(rpcContext, &block)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return ErrLocalRPCTimeout
			}
			return fmt.Errorf("%w: %s", ErrBlockApplication, err.Error())
		}
	}

	// We will consider ourselves as syncing if we have more than 5 blocks to sync
	p.isSyncing = peerHeadHeight-blocks[len(blocks)-1].Header.Height >= 5

	return nil
}

func (p *PeerConnection) connectionLoop(ctx context.Context) {
	for {
		select {
		case <-p.requestBlockChan:
			err := p.handleRequestBlocks(ctx)
			if err != nil {
				time.AfterFunc(time.Second, p.requestBlocks)
				p.peerErrorChan <- PeerError{id: p.id, err: err}
			} else if p.isSyncing {
				go p.requestBlocks()
				// TODO: disable gossip (#164)
			} else {
				time.AfterFunc(time.Second*10, p.requestBlocks)
				// TODO: enable gossip (#164)
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
				go p.connectionLoop(ctx)
				go p.requestBlocks()
				return
			}
			select {
			case <-time.After(time.Second * 3):
			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewPeerConnection creates a PeerConnection
func NewPeerConnection(id peer.ID, localRPC rpc.LocalRPC, peerRPC rpc.RemoteRPC, peerErrorChan chan<- PeerError) *PeerConnection {
	return &PeerConnection{
		id:               id,
		requestBlockChan: make(chan signalRequestBlocks),
		localRPC:         localRPC,
		peerRPC:          peerRPC,
		peerErrorChan:    peerErrorChan,
	}
}
