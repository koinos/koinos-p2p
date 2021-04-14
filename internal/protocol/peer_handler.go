package protocol

import (
	"context"
	"errors"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
)

// HeightRange is a message that specifies a peer should send topology updates for the given height range.
// TODO: Refactor to have height, blocks
type HeightRange struct {
	Height    types.BlockHeightType
	NumBlocks types.UInt32
}

// PeerHandler is created by BdmiProvider to handle communications with a single peer.
type PeerHandler struct {
	// ID of the current peer
	peerID peer.ID

	// Current height range
	heightRange HeightRange

	// RPC client
	client *gorpc.Client

	// Options
	Options options.PeerHandlerOptions

	// Channel for sending if peer has an error.
	// All PeerHandlers send their errors to a common channel.
	errChan chan<- PeerError

	// Channel for receiving height range updates.
	// Each PeerHandler has its own heightRangeChan.
	// It is filled by BdmiProvider and drained by PeerHandler.
	heightRangeChan chan HeightRange

	// Channel for sending height updates from heightRangeUpdateLoop to peerHandlerLoop.
	internalHeightRangeChan chan HeightRange

	// Channel for sending your topology updates.
	// All PeerHandlers send PeerHasBlock messages to a common channel.
	peerHasBlockChan chan<- PeerHasBlock

	// Channel for requesting downloads.
	// Each PeerHandler has its own downloadRequestChan.
	// It is filled by BdmiProvider and drained by PeerHandler.
	downloadRequestChan chan BlockDownloadRequest

	// Channel for download responses.
	// All PeerHandlers send BlockDownloadResponse messages to a common channel.
	downloadResponseChan chan<- BlockDownloadResponse
}

func (h *PeerHandler) requestDownload(ctx context.Context, req BlockDownloadRequest) {
	go func() {
		log.Debugf("Request block %s from peer %s", util.BlockTopologyCmpString(&req.Topology), h.peerID)
		rpcReq := GetBlocksByIDRequest{BlockID: []types.Multihash{util.MultihashFromCmp(req.Topology.ID)}}
		rpcResp := GetBlocksByIDResponse{}
		rpcResp.BlockItems = [][]byte{}

		subctx, cancel := context.WithTimeout(ctx, time.Duration(h.Options.DownloadTimeoutMs)*time.Millisecond)
		defer cancel()
		err := h.client.CallContext(subctx, h.peerID, "SyncService", "GetBlocksByID", rpcReq, &rpcResp)
		resp := NewBlockDownloadResponse()
		resp.Topology = req.Topology
		resp.PeerID = h.peerID
		if err != nil {
			log.Warnf("Error getting block %s from peer %s: error was %s", util.BlockTopologyCmpString(&req.Topology), h.peerID, err.Error())
			resp.Err = err
		} else if len(rpcResp.BlockItems) < 1 {
			log.Warnf("  - Got 0 block")
			resp.Err = errors.New("Got 0 blocks from peer")
		} else {
			vbBlock := types.VariableBlob(rpcResp.BlockItems[0])
			resp.Block = *types.NewOpaqueBlockFromBlob(&vbBlock)
		}
		select {
		case h.downloadResponseChan <- *resp:
		case <-ctx.Done():
		}
	}()
}

func (h *PeerHandler) heightRangeUpdateLoop(ctx context.Context) {
	var value HeightRange
	hasValue := false

	for {
		if hasValue {
			select {
			case value = <-h.heightRangeChan:
			case h.internalHeightRangeChan <- value:
				hasValue = false
			case <-ctx.Done():
				return
			}
		} else {
			select {
			case value = <-h.heightRangeChan:
				hasValue = true
			case <-ctx.Done():
				return
			}
		}
	}
}

func (h *PeerHandler) peerHandlerLoop(ctx context.Context) {
	// Helper function to call peerHandlerCycle() and send any error to errChan
	log.Debugf("Start peer handler loop for peer %s", h.peerID)
	defer log.Debugf("Exit peer handler loop for peer %s", h.peerID)

	doPeerCycle := func() {
		err := h.peerHandlerCycle(ctx)
		if err != nil {
			select {
			case h.errChan <- PeerError{h.peerID, err}:
			case <-ctx.Done():
			}
			return
		}
	}

	nextPollTime := time.After(time.Duration(h.Options.HeightRangePollTimeMs) * time.Millisecond)
	for {
		select {
		case <-nextPollTime:
			doPeerCycle()
			nextPollTime = time.After(time.Duration(h.Options.HeightRangePollTimeMs) * time.Millisecond)
		case h.heightRange = <-h.internalHeightRangeChan:
		case req := <-h.downloadRequestChan:
			h.requestDownload(ctx, req)
		case <-ctx.Done():
			return
		}
	}
}

func (h *PeerHandler) peerHandlerCycle(ctx context.Context) error {
	//
	// TODO:  Currently this code has the client poll for blocks in the height range.
	//        This is inefficient, we should instead have the server pro-actively send
	//        blocks within the requested height range.  This way both client and server
	//        are properly event-driven rather than polling.
	//
	//        We will need some means to feed height range, this may require modification to
	//        libp2p-gorpc to support passing the peer ID into the caller.
	//

	log.Debugf("%s: Polling HeightRange{%d,%d}", h.peerID, h.heightRange.Height, h.heightRange.NumBlocks)

	req := GetTopologyAtHeightRequest{
		BlockHeight: h.heightRange.Height,
		NumBlocks:   h.heightRange.NumBlocks,
	}
	resp := NewGetTopologyAtHeightResponse()
	subctx, cancel := context.WithTimeout(ctx, time.Duration(h.Options.RPCTimeoutMs)*time.Millisecond)
	defer cancel()
	err := h.client.CallContext(subctx, h.peerID, "SyncService", "GetTopologyAtHeight", req, &resp)
	if err != nil {
		log.Warnf("%s: error calling GetTopologyAtHeight, error was %s", h.peerID, err.Error())
		return err
	}

	for _, b := range resp.BlockTopology {
		hasBlockMsg := PeerHasBlock{h.peerID, util.BlockTopologyToCmp(b)}
		log.Debugf("%s: Sending PeerHasBlock message for block %s", h.peerID, util.BlockTopologyCmpString(&hasBlockMsg.Block))

		select {
		case h.peerHasBlockChan <- hasBlockMsg:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
