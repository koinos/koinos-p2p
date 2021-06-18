package protocol

import (
	"context"
	"errors"

	log "github.com/koinos/koinos-log-golang"
	types "github.com/koinos/koinos-types-golang"

	"github.com/koinos/koinos-p2p/internal/options"
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
	BlockItems [][]byte
}

// SyncService handles broadcasting inventory to peers
// TODO: Rename RPC (to what?)
type SyncService struct {
	RPC      rpc.RPC
	Provider *BdmiProvider
	MyTopo   *LocalTopologyCache
	Options  options.SyncServiceOptions
}

// GetChainID p2p rpc
func (s *SyncService) GetChainID(ctx context.Context, request GetChainIDRequest, response *GetChainIDResponse) error {
	log.Debugm("SyncService.ChainID() start")
	rpcResult, err := s.RPC.GetChainID(ctx)
	if err != nil {
		log.Errorm("SyncService.ChainID() returning error")
		return err
	}

	response.ChainID = rpcResult.ChainID
	log.Debugm("SyncService.ChainID() returning normally",
		"chainID", response.ChainID)
	return nil
}

// GetHeadBlock p2p rpc
func (s *SyncService) GetHeadBlock(ctx context.Context, request GetHeadBlockRequest, response *GetHeadBlockResponse) error {
	rpcResult, err := s.RPC.GetHeadBlock(ctx)
	if err != nil {
		return err
	}

	response.ID = rpcResult.HeadTopology.ID
	response.Height = rpcResult.HeadTopology.Height
	return nil
}

// GetForkHeads p2p rpc
func (s *SyncService) GetForkHeads(ctx context.Context, request GetForkHeadsRequest, response *GetForkHeadsResponse) error {
	rpcResult, err := s.RPC.GetForkHeads(ctx)
	if err != nil {
		return err
	}

	response.ForkHeads = rpcResult.ForkHeads
	response.LastIrr = rpcResult.LastIrreversibleBlock
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
	blocks, err := s.RPC.GetBlocksByID(ctx, &request.BlockID)
	if err != nil {
		return err
	}

	response.BlockItems = make([][]byte, len(blocks.BlockItems))
	for i := 0; i < len(blocks.BlockItems); i++ {
		vb := types.NewVariableBlob()
		response.BlockItems[i] = *blocks.BlockItems[i].Block.Serialize(vb)
	}
	return nil
}

// GetTopologyAtHeight p2p rpc
func (s *SyncService) GetTopologyAtHeight(ctx context.Context, request GetTopologyAtHeightRequest, response *GetTopologyAtHeightResponse) error {
	response.ForkHeads = (*types.GetForkHeadsResponse)(s.Provider.forkHeads)

	for _, head := range response.ForkHeads.ForkHeads {
		lastBlock := request.BlockHeight + types.BlockHeightType(request.NumBlocks) - 1
		if head.Height < lastBlock {
			lastBlock = head.Height
		}

		for i := request.BlockHeight; i <= lastBlock; i++ {
			if topos, ok := s.MyTopo.ByHeight(i); ok {
				for key := range topos {
					response.BlockTopology = append(response.BlockTopology, types.BlockTopology{
						ID: types.Multihash{
							ID:     key.ID.ID,
							Digest: types.VariableBlob(key.ID.Digest),
						},
						Previous: types.Multihash{
							ID:     key.Previous.ID,
							Digest: types.VariableBlob(key.Previous.Digest),
						},
						Height: key.Height,
					})
				}
			} else {
				resp, err := s.RPC.GetBlocksByHeight(ctx, &head.ID, i, 1)
				if err != nil {
					return err
				}
				for _, blockItem := range resp.BlockItems {
					topology := types.BlockTopology{
						ID:     blockItem.BlockID,
						Height: blockItem.BlockHeight,
					}

					if blockItem.BlockHeight != 0 {
						if !blockItem.Block.HasValue() {
							return errors.New("Optional block not present")
						}

						topology.Previous = blockItem.Block.Value.Header.Previous
					}

					response.BlockTopology = append(response.BlockTopology, topology)
				}
			}
		}
	}

	return nil
}

// NewSyncService constructs a new broadcast protocol object
func NewSyncService(rpc *rpc.RPC, provider *BdmiProvider, myTopo *LocalTopologyCache, opts options.SyncServiceOptions) *SyncService {
	p := &SyncService{RPC: *rpc, Provider: provider, MyTopo: myTopo, Options: opts}
	return p
}
