package protocol

import (
	"context"
	"log"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	types "github.com/koinos/koinos-types-golang"
)

// PeerHasBlock is a message that specifies a peer has a block with the given topology.
//
type PeerHasBlock struct {
	PeerID peer.ID
	Block  types.BlockTopology
}

// HeightRange is a message that specifies a peer should send topology updates for the given height range.
type HeightRange struct {
	MinHeight types.BlockHeightType
	MaxHeight types.BlockHeightType
}

type PeerError struct {
	Peer  peer.ID
	Error error
}

// PeerHandler is a struct that implements the BlockDownloadManagerInterface
// and supplies the necessary channels using an actual peer.
type PeerHandler struct {
	// ID of the current peer
	peerID peer.ID

	// RPC client
	client *gorpc.Client

	// My current fork state
	heightRange HeightRange

	// Channel for sending if peer has an error
	errChan chan PeerError

	// Channel for sending topology updates
	hasBlockChan chan PeerHasBlock

	// Channel for receiving height range updates
	heightRangeChan chan HeightRange

	myBlockTopologyChan  chan types.BlockTopology
	myLastIrrChan        chan types.BlockTopology
	peerHasBlockChan     chan PeerHasBlock
	downloadResponseChan chan BlockDownloadResponse
	applyBlockResultChan chan BlockDownloadApplyResult
	rescanChan           chan bool
}

const (
	heightRangePollTime    = 2
	downloadTimeoutSeconds = 50
)

func (h *PeerHandler) MyBlockTopologyChan() <-chan types.BlockTopology {
	return h.myBlockTopologyChan
}

func (h *PeerHandler) MyLastIrrChan() <-chan types.BlockTopology {
	return h.myLastIrrChan
}

func (h *PeerHandler) PeerHasBlockChan() <-chan PeerHasBlock {
	return h.peerHasBlockChan
}

func (h *PeerHandler) DownloadResponseChan() <-chan BlockDownloadResponse {
	return h.downloadResponseChan
}

func (h *PeerHandler) ApplyBlockResultChan() <-chan BlockDownloadApplyResult {
	return h.applyBlockResultChan
}

func (h *PeerHandler) RescanChan() <-chan bool {
	return h.rescanChan
}

func (h *PeerHandler) RequestDownload(ctx context.Context, req BlockDownloadRequest) {
	go func() {
		rpcReq := GetBlocksByIDRequest{BlockID: []types.Multihash{req.Topology.ID}}
		rpcResp := GetBlocksByIDResponse{}
		subctx, cancel := context.WithTimeout(ctx, time.Duration(downloadTimeoutSeconds)*time.Second)
		defer cancel()
		err := h.client.CallContext(subctx, h.peerID, "SyncService", "GetBlocksByID", rpcReq, &rpcResp)
		resp := BlockDownloadResponse{
			SerTopology: req.SerTopology,
			Topology:    req.Topology,
			PeerID:      req.PeerID,
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
	for {
		nextPollTime := time.After(time.Duration(heightRangePollTime) * time.Second)
		for {
			select {
			case <-nextPollTime:
				break
			case h.heightRange <- heightRangeChan:
			case <-ctx.Done():
				return
			}
		}
		err := h.peerHandlerCycle(ctx)
		if err != nil {
			select {
			case h.errChan <- PeerError{h.peerID, err}:
			case <-ctx.Done():
			}
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

	req := GetTopologyAtHeightRangeRequest{
		MinHeight: h.heightRange.MinHeight,
		MaxHeight: h.heightRange.MaxHeight,
	}
	resp := GetTopologyAtHeightRangeResponse{}
	subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	err := m.client.CallContext(subctx, pid, "SyncService", "GetTopologyAtHeightRangeReponse", req, &resp)
	if err != nil {
		log.Printf("%v: error calling GetTopologyAtHeightRange, error was %v\n", pid, err)
		return err
	}

	for _, b := range resp.Blocks {
		select {
		case hasBlockChan <- PeerHasBlock{h.peerID, b}:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
