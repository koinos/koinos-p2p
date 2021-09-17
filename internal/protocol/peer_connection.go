package protocol

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/libp2p/go-libp2p-core/peer"
)

type signalRequestBlocks struct{}

type PeerConnection struct {
	id                    peer.ID
	lastIrreversibleBlock *koinos.BlockTopology

	requestBlockChan chan signalRequestBlocks
	libChan          chan *koinos.BlockTopology

	localRPC      rpc.LocalRPC
	peerRPC       rpc.RemoteRPC
	peerErrorChan chan<- PeerError
}

func (p *PeerConnection) UpdateLastIrreversibleBlock(lib *koinos.BlockTopology) {
	p.libChan <- lib
}

func (p *PeerConnection) requestBlocks() {
	p.requestBlockChan <- signalRequestBlocks{}
}

func (p *PeerConnection) handleRequestBlocks(ctx context.Context) error {
	// Get my head info
	rpcContext, cancelZ := context.WithTimeout(ctx, time.Second*3)
	defer cancelZ()
	forkHeads, err := p.localRPC.GetForkHeads(rpcContext)
	if err != nil {
		return err
	}
	p.lastIrreversibleBlock = (*koinos.BlockTopology)(forkHeads.LastIrreversibleBlock)

	// Get peer's head block
	rpcContext, cancelA := context.WithTimeout(ctx, time.Second*3)
	defer cancelA()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= p.lastIrreversibleBlock.Height {
		time.AfterFunc(time.Second*3, p.requestBlocks)
		return nil
	}

	// If LIB is 0, we are still at genesis and could connec to any chain
	if p.lastIrreversibleBlock.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelB := context.WithTimeout(ctx, time.Second*3)
		defer cancelB()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, p.lastIrreversibleBlock.Height)
		if err != nil {
			return err
		}

		if bytes.Compare([]byte(*ancestorBlock), []byte(p.lastIrreversibleBlock.Id)) != 0 {
			return errors.New("my irreversible block is not an ancestor of peer's head block")
		}
	}

	blocksToRequest := peerHeadHeight - p.lastIrreversibleBlock.Height
	if blocksToRequest > 100 {
		blocksToRequest = 100
	}

	// Request blocks
	rpcContext, cancelC := context.WithTimeout(ctx, time.Second*5)
	defer cancelC()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, p.lastIrreversibleBlock.Height+1, uint32(blocksToRequest))
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

func (p *PeerConnection) handleUpdateLastIrreversibleBlock(ctx context.Context, lib *koinos.BlockTopology) {
	p.lastIrreversibleBlock = lib
}

func (p *PeerConnection) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-p.requestBlockChan:
				err := p.handleRequestBlocks(ctx)
				if err != nil {
					p.peerErrorChan <- PeerError{id: p.id, err: err}
					time.AfterFunc(time.Second, p.requestBlocks)
				}
			case lib := <-p.libChan:
				p.handleUpdateLastIrreversibleBlock(ctx, lib)

			case <-ctx.Done():
				return
			}
		}
	}()

	p.requestBlocks()
}

func NewPeerConnection(id peer.ID, lib *koinos.BlockTopology, localRPC rpc.LocalRPC, peerRPC rpc.RemoteRPC, peerErrorChan chan<- PeerError) *PeerConnection {
	return &PeerConnection{
		id:                    id,
		lastIrreversibleBlock: lib,
		requestBlockChan:      make(chan signalRequestBlocks),
		libChan:               make(chan *koinos.BlockTopology),
		localRPC:              localRPC,
		peerRPC:               peerRPC,
		peerErrorChan:         peerErrorChan,
	}
}
