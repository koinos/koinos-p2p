package protocol

import (
	"context"
	"log"

	types "github.com/koinos/koinos-types-golang"

	"github.com/koinos/koinos-p2p/internal/rpc"
)

// GetChainIDRequest args
type GetChainIDRequest struct{}

// GetChainIDResponse return
type GetChainIDResponse struct {
	ChainID types.Multihash
}

// GetHeadBlockRequest args
type GetHeadBlockRequest struct{}

// GetHeadBlockResponse return
type GetHeadBlockResponse struct {
	ID     types.Multihash
	Height types.BlockHeightType
}

// GetForkHeadsRequest args
type GetForkHeadsRequest struct{}

// GetForkHeadsResponse return
type GetForkHeadsResponse struct {
	ForkHeads []types.BlockTopology
	LastIrr   types.BlockTopology
}

// BroadcastPeerStatus is an enum which represent peer's response
type forkStatus int

// The possible peer status results
const (
	SameFork forkStatus = iota
	DifferentFork
)

// GetForkStatusRequest args
type GetForkStatusRequest struct {
	HeadID     types.Multihash
	HeadHeight types.BlockHeightType
}

// GetForkStatusResponse return
type GetForkStatusResponse struct {
	Status forkStatus
}

// GetBlocksRequest args
type GetBlocksRequest struct {
	HeadBlockID      types.Multihash
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
}

// GetBlocksResponse return
type GetBlocksResponse struct {
	BlockItems types.VectorBlockItem
}

// GetTopologyAtHeightRequest args
type GetTopologyAtHeightRequest struct {
	BlockHeight types.BlockHeightType
	NumBlocks   types.UInt32
}

// GetTopologyAtHeightResponse return
type GetTopologyAtHeightResponse struct {
	ForkHeads     *types.GetForkHeadsResponse
	BlockTopology []types.BlockTopology
}

// NewGetTopologyAtHeightRequest instantiates a new GetTopologyAtHeightRequest
func NewGetTopologyAtHeightRequest() *GetTopologyAtHeightRequest {
	req := GetTopologyAtHeightRequest{}
	return &req
}

// NewGetTopologyAtHeightResponse instantiates a new GetTopologyAtHeightResponse
func NewGetTopologyAtHeightResponse() *GetTopologyAtHeightResponse {
	resp := GetTopologyAtHeightResponse{
		ForkHeads:     types.NewGetForkHeadsResponse(),
		BlockTopology: make([]types.BlockTopology, 0),
	}
	return &resp
}

// GetBlocksByIDRequest args
type GetBlocksByIDRequest struct {
	BlockID types.VectorMultihash
}

// GetBlocksByIDResponse return
type GetBlocksByIDResponse struct {
	BlockItems types.VectorBlockItem
}

// SyncService handles broadcasting inventory to peers
// TODO: Rename RPC (to what?)
type SyncService struct {
	RPC rpc.RPC
}

// GetChainID p2p rpc
func (s *SyncService) GetChainID(ctx context.Context, request GetChainIDRequest, response *GetChainIDResponse) error {
	log.Printf("SyncService.ChainID() start\n")
	rpcResult, err := s.RPC.GetChainID()
	if err != nil {
		log.Printf("SyncService.ChainID() returning error\n")
		return err
	}

	response.ChainID = rpcResult.ChainID
	log.Printf("SyncService.ChainID() returning normally, chain ID is %v\n", response.ChainID)
	return nil
}

// GetHeadBlock p2p rpc
func (s *SyncService) GetHeadBlock(ctx context.Context, request GetHeadBlockRequest, response *GetHeadBlockResponse) error {
	rpcResult, err := s.RPC.GetHeadBlock()
	if err != nil {
		return err
	}

	response.ID = rpcResult.ID
	response.Height = rpcResult.Height
	return nil
}

// GetForkHeads p2p rpc
func (s *SyncService) GetForkHeads(ctx context.Context, request GetForkHeadsRequest, response *GetForkHeadsResponse) error {
	rpcResult, err := s.RPC.GetForkHeads()
	if err != nil {
		return err
	}

	response.ForkHeads = rpcResult.ForkHeads
	response.LastIrr = rpcResult.LastIrreversibleBlock
	return nil
}

// GetForkStatus p2p rpc
func (s *SyncService) GetForkStatus(ctx context.Context, request GetForkStatusRequest, response *GetForkStatusResponse) error {
	response.Status = SameFork

	// If peer is not in genesis state, check if they are on an ancestor chain of our's
	if request.HeadHeight > 0 {
		ancestor, err := s.RPC.GetBlocksByHeight(&request.HeadID, request.HeadHeight, 1)

		if err != nil { // Should not happen, requested blocks from ID that we do not have
			return err
		}

		if !ancestor.BlockItems[0].BlockID.Equals(&request.HeadID) { // Different fork
			response.Status = DifferentFork
		}
	}

	return nil
}

// GetBlocks p2p rpc
func (s *SyncService) GetBlocks(ctx context.Context, request GetBlocksRequest, response *GetBlocksResponse) error {
	// TODO: Re-implement this
	/*
	   blocks, err := s.RPC.GetBlocksByHeight(&request.HeadBlockID,
	      request.StartBlockHeight, types.UInt32(request.BatchSize))
	   if err != nil {
	      return err
	   }

	   response.VectorBlockItems = *blocks.BlockItems.Serialize(&response.VectorBlockItems)
	*/
	return nil
}

// GetBlocksByID p2p rpc
func (s *SyncService) GetBlocksByID(ctx context.Context, request GetBlocksByIDRequest, response *GetBlocksByIDResponse) error {
	blocks, err := s.RPC.GetBlocksByID(&request.BlockID)
	if err != nil {
		return err
	}

	response.BlockItems = blocks.BlockItems
	return nil
}

// GetTopologyAtHeight p2p rpc
func (s *SyncService) GetTopologyAtHeight(ctx context.Context, request GetTopologyAtHeightRequest, response *GetTopologyAtHeightResponse) error {
	forkHeads, blockTopology, err := s.RPC.GetTopologyAtHeight(request.BlockHeight, request.NumBlocks)
	if err != nil {
		return err
	}

	response.ForkHeads = forkHeads
	response.BlockTopology = blockTopology
	return nil
}

// NewSyncService constructs a new broadcast protocol object
func NewSyncService(rpc *rpc.RPC) *SyncService {
	p := &SyncService{RPC: *rpc}
	return p
}
