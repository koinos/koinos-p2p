package rpc

import (
	"context"

	types "github.com/koinos/koinos-types-golang"
)

// RPC interface for RPC methods required for koinos-p2p to function
// TODO:  Add context to all these functions
type RPC interface {
	GetHeadBlock(ctx context.Context) (*types.GetHeadInfoResponse, error)
	ApplyBlock(ctx context.Context, block *types.Block) (bool, error)
	ApplyTransaction(ctx context.Context, block *types.Transaction) (bool, error)
	GetBlocksByHeight(ctx context.Context, blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error)
	GetChainID(ctx context.Context) (*types.GetChainIDResponse, error)
	GetForkHeads(ctx context.Context) (*types.GetForkHeadsResponse, error)
	GetAncestorTopologyAtHeights(ctx context.Context, blockID *types.Multihash, heights []types.BlockHeightType) ([]types.BlockTopology, error)
	GetBlocksByID(ctx context.Context, blockID *types.VectorMultihash) (*types.GetBlocksByIDResponse, error)

	IsConnectedToBlockStore(ctx context.Context) (bool, error)
	IsConnectedToChain(ctx context.Context) (bool, error)
}
