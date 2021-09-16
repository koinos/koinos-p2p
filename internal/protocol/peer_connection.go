package protocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"time"

	log "github.com/koinos/koinos-log-golang"
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
	// Get peer's head block
	log.Info("Getting peer head block")
	rpcContext, cancelA := context.WithTimeout(ctx, time.Second*3)
	defer cancelA()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}
	log.Infof("head block: %s, height: %s", hex.EncodeToString(*peerHeadID), peerHeadHeight)
	log.Infof("my irreversible block: %s", p.lastIrreversibleBlock.String())

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= p.lastIrreversibleBlock.Height {
		time.AfterFunc(time.Second*3, p.requestBlocks)
		return nil
	}

	// If LIB is 0, we are still at genesis and could connec to any chain
	if p.lastIrreversibleBlock.Height > 0 {
		// Check if my LIB connect's to peer's head block
		log.Info("Checking if my LIB connects to peer's head block")
		rpcContext, cancelB := context.WithTimeout(ctx, time.Second*3)
		defer cancelB()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, p.lastIrreversibleBlock.Height)
		if err != nil {
			log.Info(err.Error())
			return err
		}

		log.Infof("Got back %s", ancestorBlock.String())

		if bytes.Compare([]byte(*ancestorBlock), []byte(p.lastIrreversibleBlock.Id)) != 0 {
			return errors.New("my irreversible block is not an ancestor of peer's head block")
		}
	}

	// Request blocks
	rpcContext, cancelC := context.WithTimeout(ctx, time.Second*5)
	defer cancelC()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, p.lastIrreversibleBlock.Height, 1000)
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

	p.requestBlocks()
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
