package protocol

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"

	rpc "github.com/koinos/koinos-p2p/internal/rpc"
	util "github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
)

const (
	pollMyTopologySeconds = uint64(2)
	// TODO:  This should be configurable, and probably ~100 for mainnet
	heightInterestReach = uint64(5)
	rescanIntervalMs    = uint64(200)
)

// BdmiProvider is the implementation of Block Download Manager Interface.
//
// BdmiProvider is responsible for the following:
//
// - Create and fill channels as specified in BlockDownloadManagerInterface
// - Start a polling loop (TODO: replace polling with event-driven) for updates to send via myBlockTopologyChan / myLastIrrChan
// - Create, start, and (TODO: cancel) PeerHandler for each peer
// - Directly connect each PeerHandler to the peerHasBlockChan loop
// - Create, start, and (TODO: cancel) handleNewPeersLoop to create a PeerHandler for each new peer
// - Create, start, and (TODO: cancel) sendHeightRangeLoop to multiplex my topology updates to peer handlers as height range updates
// - Create, start, and (TODO: cancel) dispatchDownloadLoop to distribute download requests submitted by RequestDownload() to the correct peer handler
// - Create, start, and (TODO: cancel) applyBlockLoop to service ApplyBlock() calls by submitting the blocks to koinosd
// - Create, start, and (TODO: cancel) a loop to regularly submit rescan requests to rescanChan
//
type BdmiProvider struct {
	peerHandlers map[peer.ID]*PeerHandler

	rpc rpc.RPC

	heightRange HeightRange

	newPeerChan     chan peer.ID
	peerErrChan     chan PeerError
	heightRangeChan chan HeightRange

	// Below channels are drained by DownloadManager

	myBlockTopologyChan  chan types.BlockTopology
	myLastIrrChan        chan types.BlockTopology
	peerHasBlockChan     chan PeerHasBlock
	downloadResponseChan chan BlockDownloadResponse
	applyBlockResultChan chan BlockDownloadApplyResult
	rescanChan           chan bool
}

var _ BlockDownloadManagerInterface = (*BdmiProvider)(nil)

func NewBdmiProvider(rpc rpc.RPC) *BdmiProvider {
	return &BdmiProvider{
		peerHandlers: make(map[peer.ID]*PeerHandler),
		rpc:          rpc,
		heightRange:  HeightRange{1, 1},

		newPeerChan:     make(chan peer.ID),
		peerErrChan:     make(chan PeerError),
		heightRangeChan: make(chan HeightRange),

		myBlockTopologyChan:  make(chan types.BlockTopology),
		myLastIrrChan:        make(chan types.BlockTopology),
		peerHasBlockChan:     make(chan PeerHasBlock),
		downloadResponseChan: make(chan BlockDownloadResponse),
		applyBlockResultChan: make(chan BlockDownloadApplyResult),
		rescanChan:           make(chan bool),
	}
}

func (p *BdmiProvider) MyBlockTopologyChan() <-chan types.BlockTopology {
	return p.myBlockTopologyChan
}

func (p *BdmiProvider) MyLastIrrChan() <-chan types.BlockTopology {
	return p.myLastIrrChan
}

func (p *BdmiProvider) PeerHasBlockChan() <-chan PeerHasBlock {
	return p.peerHasBlockChan
}

func (p *BdmiProvider) DownloadResponseChan() <-chan BlockDownloadResponse {
	return p.downloadResponseChan
}

func (p *BdmiProvider) ApplyBlockResultChan() <-chan BlockDownloadApplyResult {
	return p.applyBlockResultChan
}

func (p *BdmiProvider) RescanChan() <-chan bool {
	return p.rescanChan
}

func (p *BdmiProvider) RequestDownload(ctx context.Context, req BlockDownloadRequest) {

	log.Printf("Downloading block %v from peer %v\n", req.Topology, req.PeerID)

	resp := BlockDownloadResponse{
		Topology: req.Topology,
		PeerID:   req.PeerID,
		Err:      nil,
	}

	peerHandler, hasHandler := p.peerHandlers[req.PeerID]
	if !hasHandler {
		resp.Err = fmt.Errorf("Tried to download block %v from peer %v, but handler was not registered\n", req.Topology.ID, req.PeerID)
		log.Printf("%v\n", resp.Err.Error())
	}

	go func() {
		// If there was an error, send it
		if resp.Err != nil {
			select {
			case p.downloadResponseChan <- resp:
			case <-ctx.Done():
			}
			return
		}

		// Send to downloadRequestChan
		select {
		case peerHandler.downloadRequestChan <- req:
		case <-ctx.Done():
			return
		}

		// Sequence of events that now happens elsewhere in the code:
		//
		// PeerHandler will drain downloadRequestChan
		// PeerHandler will fill downloadResponseChan
		// BlockDownloadManager will drain downloadResponseChan
		// BlockDownloadManager will call BdmiProvider.ApplyBlock()
	}()
}

func (p *BdmiProvider) ApplyBlock(ctx context.Context, resp BlockDownloadResponse) {

	// Even if the peer's disappeared, we still attempt to apply the block.
	go func() {
		applyResult := BlockDownloadApplyResult{
			Topology: resp.Topology,
			PeerID:   resp.PeerID,
			Ok:       false,
			Err:      nil,
		}

		topo := util.BlockTopologyFromCmp(resp.Topology)

		// TODO:  We should not unbox here, however for some reason the API requires Block not OpaqueBlock
		resp.Block.Unbox()
		block, err := resp.Block.GetNative()
		if err != nil {
			applyResult.Err = err
		} else {
			applyResult.Ok, applyResult.Err = p.rpc.ApplyBlock(block, &topo)
		}

		select {
		case p.applyBlockResultChan <- applyResult:
		case <-ctx.Done():
			return
		}

		// Sequence of events that now happens elsewhere in the code:
		//
		// BlockDownloadManager will drain applyBlockResultChan
		// BlockDownloadManager will call another peer to download if apply failed

		log.Printf("Successfully applied block %v from peer %v\n", applyResult.Topology, applyResult.PeerID)

	}()
}

func (p *BdmiProvider) handleNewPeer(ctx context.Context, newPeer peer.ID) {
	// TODO handle case where peer already exists
	p.peerHandlers[newPeer] = &PeerHandler{
		peerID:               newPeer,
		heightRange:          p.heightRange,
		errChan:              p.peerErrChan,
		heightRangeChan:      make(chan HeightRange),
		peerHasBlockChan:     p.peerHasBlockChan,
		downloadRequestChan:  make(chan BlockDownloadRequest),
		downloadResponseChan: p.downloadResponseChan,
	}
}

func (p *BdmiProvider) handleHeightRange(ctx context.Context, heightRange HeightRange) {
	p.heightRange = heightRange
	for _, peerHandler := range p.peerHandlers {
		go func(ph *PeerHandler) {
			select {
			case ph.heightRangeChan <- heightRange:
			case <-ctx.Done():
			}
		}(peerHandler)
	}
}

type MyTopologyLoopState struct {
	heightRange HeightRange
}

func (p *BdmiProvider) pollMyTopologyLoop(ctx context.Context) {
	state := MyTopologyLoopState{heightRange: HeightRange{1, 1}}
	for {
		err := p.pollMyTopologyCycle(ctx, &state)

		if err != nil {
			log.Printf("Error polling my topology: %v\n", err)
		}

		select {
		case <-time.After(time.Duration(pollMyTopologySeconds) * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (p *BdmiProvider) pollMyTopologyCycle(ctx context.Context, state *MyTopologyLoopState) error {
	//
	// TODO:  Currently this code has the client poll for blocks in the height range.
	//        This is inefficient, we should instead have the server pro-actively send
	//        blocks within the requested height range.  This way both client and server
	//        are properly event-driven rather than polling.
	//
	//        We will need some means to feed height range, this may require modification to
	//        libp2p-gorpc to support passing the peer ID into the caller.
	//

	forkHeads, blockTopology, err := p.rpc.GetTopologyAtHeight(state.heightRange.Height, state.heightRange.NumBlocks)

	if err != nil {
		return err
	}

	if len(forkHeads.ForkHeads) == 0 {
		return errors.New("Zero ForkHeads were returned")
	}

	longestForkHeight := forkHeads.ForkHeads[0].Height
	for i := 1; i < len(forkHeads.ForkHeads); i++ {
		if forkHeads.ForkHeads[i].Height > longestForkHeight {
			log.Printf("Best fork head was not returned first\n")
			longestForkHeight = forkHeads.ForkHeads[i].Height
		}
	}

	libHeight := uint64(forkHeads.LastIrreversibleBlock.Height)

	newHeightRange := HeightRange{
		Height:    types.BlockHeightType(libHeight),
		NumBlocks: types.UInt32((uint64(longestForkHeight) + heightInterestReach) - libHeight),
	}

	// Any changes to heightRange get sent to the main loop for broadcast to PeerHandlers
	if newHeightRange != state.heightRange {
		log.Printf("My topology height range changed from %v to %v\n", state.heightRange, newHeightRange)

		state.heightRange = newHeightRange

		select {
		case p.heightRangeChan <- newHeightRange:
		case <-ctx.Done():
			return nil
		}
	}

	select {
	case p.myLastIrrChan <- forkHeads.LastIrreversibleBlock:
	case <-ctx.Done():
		return nil
	}

	for _, topo := range blockTopology {
		select {
		case p.myBlockTopologyChan <- topo:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// TODO:  Create loop to service downloadResponseChan and applyBlockResultChan
// TODO:  Create loop to write downloadFailedChan

func (p *BdmiProvider) providerLoop(ctx context.Context) {
	for {
		select {
		case newPeer := <-p.newPeerChan:
			p.handleNewPeer(ctx, newPeer)
		case heightRange := <-p.heightRangeChan:
			p.handleHeightRange(ctx, heightRange)
		case <-ctx.Done():
			return
		}
	}
}

type RescanLoopState struct {
	lastForceRescanTime time.Time
}

func (p *BdmiProvider) triggerRescanLoop(ctx context.Context) {

	// Set the start time to be far enough in the past to trigger rescan immediately
	state := RescanLoopState{
		lastForceRescanTime: time.Now().Add(-1000 * time.Duration(rescanIntervalMs) * time.Millisecond),
	}
	for {
		select {
		case <-time.After(time.Duration(rescanIntervalMs) * time.Millisecond):
			p.triggerRescanCycle(ctx, &state)
		case <-ctx.Done():
			return
		}
	}
}

func (p *BdmiProvider) triggerRescanCycle(ctx context.Context, state *RescanLoopState) {
	forceRescan := false
	now := time.Now()
	if now.Sub(state.lastForceRescanTime) >= time.Duration(rescanIntervalMs)*time.Millisecond {
		state.lastForceRescanTime = now
		forceRescan = true
	}
	select {
	case p.rescanChan <- forceRescan:
	case <-ctx.Done():
		return
	}
}

func (p *BdmiProvider) Start(ctx context.Context) {
	go p.pollMyTopologyLoop(ctx)
	go p.providerLoop(ctx)
	go p.triggerRescanLoop(ctx)
}
