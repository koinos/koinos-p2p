package protocol

import (
	"context"

	types "github.com/koinos/koinos-types-golang"

	"github.com/koinos/koinos-p2p/internal/rpc"
)

// GetChainIDRequest args
type GetChainIDRequest struct{}

// GetChainIDResponse return
type GetChainIDResponse struct {
	ChainID types.Multihash
}

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
	StartBlockID     types.Multihash
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
}

// GetBlocksResponse return
type GetBlocksResponse struct {
	VectorBlockItems types.VariableBlob
}

// SyncService handles broadcasting inventory to peers
type SyncService struct {
	RPC rpc.RPC
}

// GetChainID p2p rpc
func (s *SyncService) GetChainID(ctx context.Context, request GetChainIDRequest, response *GetChainIDResponse) error {
	rpcResult, err := s.RPC.GetChainID()
	if err != nil {
		return nil
	}

	response.ChainID = rpcResult.ChainID
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
	blocks, err := s.RPC.GetBlocksByHeight(&request.StartBlockID,
		request.StartBlockHeight, types.UInt32(min(batchSize, request.BatchSize)))
	if err != nil {
		return err
	}

	response.VectorBlockItems = *blocks.BlockItems.Serialize(&response.VectorBlockItems)
	return nil
}

// NewSyncService constructs a new broadcast protocol object
func NewSyncService(rpc *rpc.RPC) *SyncService {
	p := &SyncService{RPC: *rpc}
	return p
}
