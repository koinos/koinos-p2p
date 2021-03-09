package protocol

import (
	"context"
	"encoding/json"
	"log"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	"github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
)

// HeightRange is a message that specifies a peer should send topology updates for the given height range.
// TODO: Refactor to have height, blocks
type HeightRange struct {
	Height    types.BlockHeightType
	NumBlocks types.UInt32
}

type PeerError struct {
	PeerID peer.ID
	Error  error
}

// PeerHandler is created by BdmiProvider to handle communications with a single peer.
type PeerHandler struct {
	// ID of the current peer
	peerID peer.ID

	// Current height range
	heightRange HeightRange

	// RPC client
	client *gorpc.Client

	// Channel for sending if peer has an error.
	// All PeerHandlers send their errors to a common channel.
	errChan chan<- PeerError

	// Channel for receiving height range updates.
	// Each PeerHandler has its own heightRangeChan.
	// It is filled by BdmiProvider and drained by PeerHandler.
	heightRangeChan chan HeightRange

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

const (
	heightRangePollTime    = 2
	downloadTimeoutSeconds = 50
)

func (h *PeerHandler) requestDownload(ctx context.Context, req BlockDownloadRequest) {
	go func() {
		log.Printf("Getting block %d from peer %v using SyncService GetBlocksByID RPC\n", req.Topology.Height, req.PeerID)
		rpcReq := GetBlocksByIDRequest{BlockID: []types.Multihash{util.MultihashFromCmp(req.Topology.ID)}}
		rpcResp := GetBlocksByIDResponse{}
		subctx, cancel := context.WithTimeout(ctx, time.Duration(downloadTimeoutSeconds)*time.Second)
		defer cancel()
		err := h.client.CallContext(subctx, h.peerID, "SyncService", "GetBlocksByID", rpcReq, &rpcResp)
		resp := BlockDownloadResponse{
			Topology: req.Topology,
			PeerID:   h.peerID,
		}
		if err != nil {
			log.Printf("Error getting block %v from peer %v: error was %v", req.Topology.ID, h.peerID, err)
			resp.Err = err
		} else {
			resp.Block = rpcResp.BlockItems[0].Block
		}
		select {
		case h.downloadResponseChan <- resp:
		case <-ctx.Done():
		}
	}()
}

func (h *PeerHandler) peerHandlerLoop(ctx context.Context) {
	// Helper function to call peerHandlerCycle() and send any error to errChan
	log.Printf("Start peer handler loop for peer %v\n", h.peerID)
	defer log.Printf("Exit peer handler loop for peer %v\n", h.peerID)

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

	nextPollTime := time.After(time.Duration(heightRangePollTime) * time.Second)
	for {
		select {
		case <-nextPollTime:
			doPeerCycle()
			nextPollTime = time.After(time.Duration(heightRangePollTime) * time.Second)
		case h.heightRange = <-h.heightRangeChan:
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

	log.Printf("%v: Polling HeightRange{%d,%d}\n", h.peerID, h.heightRange.Height, h.heightRange.NumBlocks)

	req := GetTopologyAtHeightRequest{
		BlockHeight: h.heightRange.Height,
		NumBlocks:   h.heightRange.NumBlocks,
	}
	resp := GetTopologyAtHeightResponse{}
	subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	err := h.client.CallContext(subctx, h.peerID, "SyncService", "GetTopologyAtHeight", req, &resp)
	if err != nil {
		log.Printf("%v: error calling GetTopologyAtHeight, error was %v\n", h.peerID, err)
		return err
	}

	for _, b := range resp.BlockTopology {
		hasBlockMsg := PeerHasBlock{h.peerID, util.BlockTopologyToCmp(b)}
		topoStr, err := json.Marshal(b)
		if err == nil {
			log.Printf("%v: Sending PeerHasBlock message %s\n", h.peerID, topoStr)
		}

		select {
		case h.peerHasBlockChan <- hasBlockMsg:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
