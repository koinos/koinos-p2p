package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	rpc "github.com/koinos/koinos-p2p/internal/rpc"
	util "github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
)

const (
	pollMyTopologySeconds     = uint64(2)
	heightRangeTimeoutSeconds = uint64(10)
	// TODO:  This should be configurable, and probably ~100 for mainnet
	heightInterestReach = uint64(5)
	rescanIntervalMs    = uint64(1000) // TODO: Lower to 200ms once we're done debugging the p2p code
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

	client *gorpc.Client
	rpc    rpc.RPC

	heightRange HeightRange

	enableDebugMessages bool

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

// NewBdmiProvider creates a new instance of BdmiProvider
func NewBdmiProvider(client *gorpc.Client, rpc rpc.RPC) *BdmiProvider {
	return &BdmiProvider{
		peerHandlers: make(map[peer.ID]*PeerHandler),
		client:       client,
		rpc:          rpc,
		heightRange:  HeightRange{0, 0},

		enableDebugMessages: false,

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

// MyBlockTopologyChan is a getter for myBlockTopologyChan
func (p *BdmiProvider) MyBlockTopologyChan() <-chan types.BlockTopology {
	return p.myBlockTopologyChan
}

// MyLastIrrChan is a getter for myLastIrrChan
func (p *BdmiProvider) MyLastIrrChan() <-chan types.BlockTopology {
	return p.myLastIrrChan
}

// PeerHasBlockChan is a getter for peerHasBlockChan
func (p *BdmiProvider) PeerHasBlockChan() <-chan PeerHasBlock {
	return p.peerHasBlockChan
}

// DownloadResponseChan is a getter for downloadResponseChan
func (p *BdmiProvider) DownloadResponseChan() <-chan BlockDownloadResponse {
	return p.downloadResponseChan
}

// ApplyBlockResultChan is a getter for applyBlockResultChan
func (p *BdmiProvider) ApplyBlockResultChan() <-chan BlockDownloadApplyResult {
	return p.applyBlockResultChan
}

// RescanChan is a getter for rescanChan
func (p *BdmiProvider) RescanChan() <-chan bool {
	return p.rescanChan
}

// RequestDownload initiates a downlaod request
func (p *BdmiProvider) RequestDownload(ctx context.Context, req BlockDownloadRequest) {

	if p.enableDebugMessages {
		log.Printf("Downloading block %v from peer %v\n", req.Topology, req.PeerID)
	}

	resp := BlockDownloadResponse{
		Topology: req.Topology,
		PeerID:   req.PeerID,
		Err:      nil,
	}

	peerHandler, hasHandler := p.peerHandlers[req.PeerID]
	if !hasHandler {
		resp.Err = fmt.Errorf("Tried to download block %v from peer %v, but handler was not registered", req.Topology.ID, req.PeerID)
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

// ApplyBlock attempts to apply a block from a peer
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
			log.Printf("Tried to apply block of height %d, got error %s\n", applyResult.Topology.Height, err.Error())
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

		topoStr, err := json.Marshal(util.BlockTopologyFromCmp(applyResult.Topology))
		if err != nil {
			log.Printf("Successfully applied block %v from peer %v\n", topoStr, applyResult.PeerID)
		}
	}()
}

func (p *BdmiProvider) handleNewPeer(ctx context.Context, newPeer peer.ID) {
	// TODO handle case where peer already exists
	h := &PeerHandler{
		peerID:                  newPeer,
		heightRange:             p.heightRange,
		client:                  p.client,
		enableDebugMessages:     p.enableDebugMessages,
		errChan:                 p.peerErrChan,
		heightRangeChan:         make(chan HeightRange),
		internalHeightRangeChan: make(chan HeightRange),
		peerHasBlockChan:        p.peerHasBlockChan,
		downloadRequestChan:     make(chan BlockDownloadRequest),
		downloadResponseChan:    p.downloadResponseChan,
	}
	p.peerHandlers[newPeer] = h
	go h.peerHandlerLoop(ctx)
	go h.heightRangeUpdateLoop(ctx)
}

func (p *BdmiProvider) handleHeightRange(ctx context.Context, heightRange HeightRange) {
	p.heightRange = heightRange
	for _, peerHandler := range p.peerHandlers {
		go func(ph *PeerHandler) {
			select {
			case <-time.After(time.Duration(heightRangeTimeoutSeconds) * time.Second):
				log.Printf("PeerHandler for peer %s did not timely service height range update %v\n",
					ph.peerID, heightRange)
			case ph.heightRangeChan <- heightRange:
			case <-ctx.Done():
			}
		}(peerHandler)
	}
}

// MyTopologyLoopState represents that state of a topology loop
type MyTopologyLoopState struct {
	heightRange HeightRange
}

func (p *BdmiProvider) pollMyTopologyLoop(ctx context.Context) {
	state := MyTopologyLoopState{heightRange: HeightRange{0, 0}}
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

// getHeightInterestRange computes the heights of interest for a given GetForkHeadsResponse
func getHeightInterestRange(forkHeads *types.GetForkHeadsResponse) HeightRange {
	if len(forkHeads.ForkHeads) == 0 {
		// Zero ForkHeads means we're spinning up a brand-new node that doesn't have any blocks yet.
		// In this case we simply ask for the first few blocks.
		return HeightRange{1, types.UInt32(heightInterestReach)}
	}

	longestForkHeight := forkHeads.ForkHeads[0].Height
	for i := 1; i < len(forkHeads.ForkHeads); i++ {
		if forkHeads.ForkHeads[i].Height > longestForkHeight {
			log.Printf("Best fork head was not returned first\n")
			longestForkHeight = forkHeads.ForkHeads[i].Height
		}
	}

	libHeight := uint64(forkHeads.LastIrreversibleBlock.Height)

	if uint64(longestForkHeight) < libHeight {
		log.Printf("Longest fork height was smaller than LIB height!?\n")
		return HeightRange{types.BlockHeightType(libHeight), types.UInt32(heightInterestReach)}
	}

	newHeightRange := HeightRange{
		Height:    types.BlockHeightType(libHeight),
		NumBlocks: types.UInt32((uint64(longestForkHeight) + heightInterestReach) - libHeight),
	}

	// Poll range should never include block 0, even if block 0 is irreversible
	if newHeightRange.Height < 1 {
		newHeightRange.Height = 1
	}

	if newHeightRange.NumBlocks < types.UInt32(heightInterestReach) {
		newHeightRange.NumBlocks = types.UInt32(heightInterestReach)
	}
	return newHeightRange
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

	newHeightRange := getHeightInterestRange(forkHeads)

	// Any changes to heightRange get sent to the main loop for broadcast to PeerHandlers
	if newHeightRange != state.heightRange {
		if p.enableDebugMessages {
			log.Printf("My topology height range changed from %v to %v\n", state.heightRange, newHeightRange)
		}

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

// RescanLoopState represents the state of a rescan loop
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

// Start starts the Bdmi provider
func (p *BdmiProvider) Start(ctx context.Context) {
	go p.pollMyTopologyLoop(ctx)
	go p.providerLoop(ctx)
	go p.triggerRescanLoop(ctx)
}
