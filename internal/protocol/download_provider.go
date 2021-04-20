package protocol

import (
	"context"
	"fmt"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
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
// - Create, start, and (TODO: cancel) sendNodeUpdateLoop to multiplex my topology updates to peer handlers as height range updates
// - Create, start, and (TODO: cancel) dispatchDownloadLoop to distribute download requests submitted by RequestDownload() to the correct peer handler
// - Create, start, and (TODO: cancel) applyBlockLoop to service ApplyBlock() calls by submitting the blocks to koinosd
// - Create, start, and (TODO: cancel) a loop to regularly submit rescan requests to rescanChan
//
type BdmiProvider struct {
	peerHandlers map[peer.ID]*PeerHandler

	client *gorpc.Client
	rpc    rpc.RPC

	forkHeads   *types.ForkHeads
	lastNodeUpdate NodeUpdate

	Options            options.BdmiProviderOptions
	PeerHandlerOptions options.PeerHandlerOptions

	newPeerChan    chan peer.ID
	peerErrChan    chan PeerError
	nodeUpdateChan chan NodeUpdate

	// Below channels are drained by DownloadManager

	myBlockTopologyChan    chan types.BlockTopology
	myLastIrrChan          chan types.BlockTopology
	peerHasBlockChan       chan PeerHasBlock
	peerIsContemporaryChan chan PeerIsContemporary
	downloadResponseChan   chan BlockDownloadResponse
	applyBlockResultChan   chan BlockDownloadApplyResult
	rescanChan             chan bool
}

var _ BlockDownloadManagerInterface = (*BdmiProvider)(nil)

// NewBdmiProvider creates a new instance of BdmiProvider
func NewBdmiProvider(client *gorpc.Client, rpc rpc.RPC, opts options.BdmiProviderOptions, phopts options.PeerHandlerOptions) *BdmiProvider {
	return &BdmiProvider{
		peerHandlers: make(map[peer.ID]*PeerHandler),
		client:       client,
		rpc:          rpc,

		forkHeads:   types.NewForkHeads(),
		lastNodeUpdate: NodeUpdate{0, 0, 0},

		Options:            opts,
		PeerHandlerOptions: phopts,

		newPeerChan:     make(chan peer.ID, 1),
		peerErrChan:     make(chan PeerError, 1),
		nodeUpdateChan:  make(chan NodeUpdate, 1),

		myBlockTopologyChan:    make(chan types.BlockTopology),
		myLastIrrChan:          make(chan types.BlockTopology),
		peerHasBlockChan:       make(chan PeerHasBlock, opts.PeerHasBlockQueueSize),
		peerIsContemporaryChan: make(chan PeerIsContemporary, 1),
		downloadResponseChan:   make(chan BlockDownloadResponse, 1),
		applyBlockResultChan:   make(chan BlockDownloadApplyResult, 1),
		rescanChan:             make(chan bool, 1),
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

// PeerIsContemporaryChan is a getter for peerIsContemporaryChan
func (p *BdmiProvider) PeerIsContemporaryChan() <-chan PeerIsContemporary {
	return p.peerIsContemporaryChan
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

	log.Debugf("Downloading block %s from peer %s", util.BlockTopologyCmpString(&req.Topology), req.PeerID)

	resp := BlockDownloadResponse{
		Topology: req.Topology,
		PeerID:   req.PeerID,
		Err:      nil,
	}

	peerHandler, hasHandler := p.peerHandlers[req.PeerID]
	if !hasHandler {
		resp.Err = fmt.Errorf("Tried to download block %s from peer %s, but handler was not registered", util.BlockTopologyCmpString(&req.Topology), req.PeerID)
		log.Error(resp.Err.Error())
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

		// TODO:  We should not unbox here, however for some reason the API requires Block not OpaqueBlock
		resp.Block.Unbox()
		block, err := resp.Block.GetNative()
		if err != nil {
			applyResult.Err = err
			log.Warnf("Downloaded block not applied - %s from peer %s - Error %s",
				util.BlockTopologyCmpString(&applyResult.Topology), applyResult.PeerID, err.Error())
		} else {
			applyResult.Ok, applyResult.Err = p.rpc.ApplyBlock(ctx, block)
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

		log.Infof("Downloaded block applied - %s from peer %s",
			util.BlockTopologyCmpString(&applyResult.Topology), applyResult.PeerID)
	}()
}

func (p *BdmiProvider) initialize(ctx context.Context) {
	heads, err := p.rpc.GetForkHeads(ctx)
	if err != nil {
		log.Warnf("Could not get initial fork heads: %v", err)
		return
	}

	p.forkHeads.ForkHeads = heads.ForkHeads
	p.forkHeads.LastIrreversibleBlock = heads.LastIrreversibleBlock

	p.heightRange = getHeightInterestRange(p.forkHeads, p.Options.HeightInterestReach)

	select {
	case p.heightRangeChan <- p.heightRange:
	case <-ctx.Done():
		return
	}

	select {
	case p.myLastIrrChan <- p.forkHeads.LastIrreversibleBlock:
	case <-ctx.Done():
		return
	}

	for _, head := range p.forkHeads.ForkHeads {
		response, err := p.rpc.GetBlocksByHeight(ctx, &head.ID, p.heightRange.Height, types.UInt32(p.heightRange.NumBlocks))
		if err != nil {
			log.Warnf("Could not get initial blocks: %v", err)
		} else {
			for _, opaqueBlock := range response.BlockItems {
				opaqueBlock.Block.Unbox()
				block, err := opaqueBlock.Block.GetNative()
				if err != nil {
					log.Warnf("Could not unbox initial block: %v", err)
					return
				}

				select {
				case p.myBlockTopologyChan <- types.BlockTopology{
					ID:       block.ID,
					Height:   block.Header.Height,
					Previous: block.Header.Previous,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// EnableGossip enables or disables gossip mode
func (p *BdmiProvider) EnableGossip(ctx context.Context, enableGossip bool) {
	// TODO
}

func (p *BdmiProvider) handleNewPeer(ctx context.Context, newPeer peer.ID) {
	// TODO handle case where peer already exists
	h := &PeerHandler{
		peerID:                 newPeer,
		lastNodeUpdate:         p.lastNodeUpdate,
		client:                 p.client,
		Options:                p.PeerHandlerOptions,
		errChan:                p.peerErrChan,
		nodeUpdateChan:         make(chan NodeUpdate, 1),
		internalNodeUpdateChan: make(chan NodeUpdate, 1),
		peerHasBlockChan:       p.peerHasBlockChan,
		downloadRequestChan:    make(chan BlockDownloadRequest, 1),
		downloadResponseChan:   p.downloadResponseChan,
	}
	p.peerHandlers[newPeer] = h
	go h.peerHandlerLoop(ctx)
	go h.nodeUpdateLoop(ctx)
}

func (p *BdmiProvider) handleNodeUpdate(ctx context.Context, nodeUpdate NodeUpdate) {
	p.lastNodeUpdate = nodeUpdate
	for _, peerHandler := range p.peerHandlers {
		go func(ph *PeerHandler, nodeUpdate NodeUpdate) {
			select {
			case <-time.After(time.Duration(p.Options.HeightRangeTimeoutMs) * time.Millisecond):
				log.Warnf("PeerHandler for peer %s did not timely service NodeUpdate %v",
					ph.peerID, nodeUpdate)
			case ph.nodeUpdateChan <- nodeUpdate:
			case <-ctx.Done():
			}
		}(peerHandler, nodeUpdate)
	}
}

// MyTopologyLoopState represents that state of a topology loop
type MyTopologyLoopState struct {
	lastNodeUpdate NodeUpdate
}

// getNodeUpdate computes the NodeUpdate based on GetForkHeadsResponse
func getNodeUpdate(forkHeads *types.ForkHeads, heightInterestReach uint64) NodeUpdate {
	if len(forkHeads.ForkHeads) == 0 {
		// Zero ForkHeads means we're spinning up a brand-new node that doesn't have any blocks yet.
		// In this case we simply ask for the first few blocks.
		return NodeUpdate{0, 1, types.UInt32(heightInterestReach)}
	}

	longestForkHeight := forkHeads.ForkHeads[0].Height
	for i := 1; i < len(forkHeads.ForkHeads); i++ {
		if forkHeads.ForkHeads[i].Height > longestForkHeight {
			log.Warnf("Best fork head was not returned first")
			longestForkHeight = forkHeads.ForkHeads[i].Height
		}
	}

	libHeight := uint64(forkHeads.LastIrreversibleBlock.Height)

	if uint64(longestForkHeight) < libHeight {
		log.Error("Longest fork height was smaller than LIB height!?")
		return NodeUpdate{types.BlockHeightType(libHeight), types.BlockHeightType(libHeight), types.UInt32(heightInterestReach)}
	}

	newNodeUpdate := NodeUpdate{
		NodeHeight:          longestForkHeight,
		InterestStartHeight: types.BlockHeightType(libHeight),
		InterestNumBlocks:   types.UInt32((uint64(longestForkHeight) + heightInterestReach) - libHeight),
	}

	// Poll range should never include block 0, even if block 0 is irreversible
	if newNodeUpdate.InterestStartHeight < 1 {
		newNodeUpdate.InterestStartHeight = 1
	}

	if newNodeUpdate.InterestNumBlocks < types.UInt32(heightInterestReach) {
		newNodeUpdate.InterestNumBlocks = types.UInt32(heightInterestReach)
	}
	return newNodeUpdate
}

// HandleForkHeads handles fork broadcast
func (p *BdmiProvider) HandleForkHeads(ctx context.Context, newHeads *types.ForkHeads) {
	p.forkHeads = newHeads

	newNodeUpdate := getNodeUpdate(newHeads, p.Options.HeightInterestReach)

	// Any changes to nodeUpdate get sent to the main loop for broadcast to PeerHandlers
	if newNodeUpdate != p.lastNodeUpdate {
		log.Debugf("lastNodeUpdate changed from %v to %v", p.lastNodeUpdate, newNodeUpdate)

		p.lastNodeUpdate = newNodeUpdate

		select {
		case p.nodeUpdateChan <- newNodeUpdate:
		case <-ctx.Done():
			return
		}
	}

	select {
	case p.myLastIrrChan <- p.forkHeads.LastIrreversibleBlock:
	case <-ctx.Done():
		return
	}
}

// HandleBlockBroadcast handles block broadcast
func (p *BdmiProvider) HandleBlockBroadcast(ctx context.Context, blockBroadcast *types.BlockAccepted) {
	select {
	case p.myBlockTopologyChan <- types.BlockTopology{
		ID:       blockBroadcast.Block.ID,
		Height:   blockBroadcast.Block.Header.Height,
		Previous: blockBroadcast.Block.Header.Previous,
	}:
	case <-ctx.Done():
		return
	}
}

func (p *BdmiProvider) providerLoop(ctx context.Context) {
	for {
		select {
		case newPeer := <-p.newPeerChan:
			p.handleNewPeer(ctx, newPeer)
		case nodeUpdate := <-p.nodeUpdateChan:
			p.handleNodeUpdate(ctx, nodeUpdate)

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
		lastForceRescanTime: time.Now().Add(-1000 * time.Duration(p.Options.RescanIntervalMs) * time.Millisecond),
	}
	for {
		select {
		case <-time.After(time.Duration(p.Options.RescanIntervalMs) * time.Millisecond):
			p.triggerRescanCycle(ctx, &state)
		case <-ctx.Done():
			return
		}
	}
}

func (p *BdmiProvider) triggerRescanCycle(ctx context.Context, state *RescanLoopState) {
	forceRescan := false
	now := time.Now()
	if now.Sub(state.lastForceRescanTime) >= time.Duration(p.Options.RescanIntervalMs)*time.Millisecond {
		state.lastForceRescanTime = now
		forceRescan = true
	}
	select {
	case p.rescanChan <- forceRescan:
	case <-ctx.Done():
		return
	}
}

// GetForkHeads returns current Fork Heads
func (p *BdmiProvider) GetForkHeads() types.ForkHeads {
	return *p.forkHeads
}

// Start starts the Bdmi provider
func (p *BdmiProvider) Start(ctx context.Context) {
	go p.initialize(ctx)
	go p.providerLoop(ctx)
	go p.triggerRescanLoop(ctx)
}
