package protocol

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/libp2p/go-libp2p-core/peer"
)

type signalRequestBlocks struct{}

// PeerConnection handles the sync portion of a connection to a peer
type PeerConnection struct {
	id peer.ID

	requestBlockChan chan signalRequestBlocks

	localRPC      rpc.LocalRPC
	peerRPC       rpc.RemoteRPC
	peerErrorChan chan<- PeerError
}

func (p *PeerConnection) requestBlocks() {
	p.requestBlockChan <- signalRequestBlocks{}
}

func (p *PeerConnection) handshake(ctx context.Context) error {
	rpcContext, cancelLocalGetChainID := context.WithTimeout(ctx, time.Second*3)
	defer cancelLocalGetChainID()
	myChainID, err := p.localRPC.GetChainID(rpcContext)
	if err != nil {
		return err
	}

	rpcContext, cancelPeerGetChainID := context.WithTimeout(ctx, time.Second*3)
	defer cancelPeerGetChainID()
	peerChainID, err := p.peerRPC.GetChainID(rpcContext)
	if err != nil {
		return err
	}

	if bytes.Compare(myChainID.ChainId, *peerChainID) != 0 {
		return errors.New("my irreversible block is not an ancestor of peer's head block")
	}

	return nil
}

func (p *PeerConnection) handleRequestBlocks(ctx context.Context) error {
	// Get my head info
	rpcContext, cancelZ := context.WithTimeout(ctx, time.Second*3)
	defer cancelZ()
	forkHeads, err := p.localRPC.GetForkHeads(rpcContext)
	if err != nil {
		return err
	}

	// Get peer's head block
	rpcContext, cancelA := context.WithTimeout(ctx, time.Second*3)
	defer cancelA()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= forkHeads.LastIrreversibleBlock.Height {
		time.AfterFunc(time.Second*3, p.requestBlocks)
		return nil
	}

	// If LIB is 0, we are still at genesis and could connec to any chain
	if forkHeads.LastIrreversibleBlock.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelB := context.WithTimeout(ctx, time.Second*3)
		defer cancelB()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height)
		if err != nil {
			return err
		}

		if bytes.Compare([]byte(*ancestorBlock), []byte(forkHeads.LastIrreversibleBlock.Id)) != 0 {
			return errors.New("my irreversible block is not an ancestor of peer's head block")
		}
	}

	blocksToRequest := peerHeadHeight - forkHeads.LastIrreversibleBlock.Height
	if blocksToRequest > 500 {
		blocksToRequest = 500
	}

	// Request blocks
	rpcContext, cancelC := context.WithTimeout(ctx, time.Second*5)
	defer cancelC()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, forkHeads.LastIrreversibleBlock.Height+1, uint32(blocksToRequest))
	if err != nil {
		return err
	}

	// Apply blocks to local node
	for _, block := range blocks {
		rpcContext, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err = p.localRPC.ApplyBlock(rpcContext, &block)
		if err != nil {
			return err
		}
	}

	if peerHeadHeight-blocks[len(blocks)-1].Header.Height < 5 {
		// If we think we are caught up, slow down how often we poll
		// TODO: Enable gossip
		time.AfterFunc(time.Second*10, p.requestBlocks)
	} else {
		go p.requestBlocks()
	}

	return nil
}

// Start syncing to the peer
func (p *PeerConnection) Start(ctx context.Context) {
	err := p.handshake(ctx)
	if err != nil {
		go func() {
			p.peerErrorChan <- PeerError{id: p.id, err: err}
		}()
	} else {
		go func() {
			for {
				select {
				case <-p.requestBlockChan:
					err := p.handleRequestBlocks(ctx)
					if err != nil {
						p.peerErrorChan <- PeerError{id: p.id, err: err}
						time.AfterFunc(time.Second, p.requestBlocks)
					}

				case <-ctx.Done():
					return
				}
			}
		}()

		go p.requestBlocks()
	}
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
