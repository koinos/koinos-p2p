package protocol

import (
	"context"
	"log"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"

	types "github.com/koinos/koinos-types-golang"
)

const (
	pollMyTopologySeconds = uint64(2)
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

	heightRange HeightRange

	heightRangeChan      chan HeightRange
	newPeerChan          chan peer.ID
	peerErrChan          chan PeerError
	myBlockTopologyChan  chan types.BlockTopology
	myLastIrrChan        chan types.BlockTopology
	peerHasBlockChan     chan PeerHasBlock
	downloadResponseChan chan BlockDownloadResponse
	applyBlockResultChan chan BlockDownloadApplyResult
	rescanChan           chan bool
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
		go func() {
			select {
			case peerHandler.heightRangeChan <- heightRange:
			case <-ctx.Done():
			}
		}()
	}
}

// TODO:  Create loop to write heightRange, myBlockTopologyChan, myLastIrrChan
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

func (p *BdmiProvider) pollMyTopologyLoop(ctx context.Context) {
	for {
		err := p.pollMyTopologyCycle(ctx)

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

func (p *BdmiProvider) pollMyTopologyCycle(ctx context.Context) error {
	// TODO: Implement this
	return nil
}

func (p *BdmiProvider) dispatchDownloadLoop(ctx context.Context) {
	// TODO: Implement this
}

func (p *BdmiProvider) applyBlockLoop(ctx context.Context) {
	// TODO: Implement this
}

func (p *BdmiProvider) triggerRescanLoop(ctx context.Context) {
	// TODO: Implement this
}

func (p *BdmiProvider) triggerRescanCycle(ctx context.Context) {
	// TODO: Implement this
}
