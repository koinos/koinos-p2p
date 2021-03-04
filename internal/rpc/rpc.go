package rpc

import (
	types "github.com/koinos/koinos-types-golang"
)

// RPC interface for RPC methods required for koinos-p2p to function
// TODO:  Add context to all these functions
type RPC interface {
	GetHeadBlock() (*types.HeadInfo, error)
	ApplyBlock(block *types.Block, topology ...*types.BlockTopology) (bool, error)
	ApplyTransaction(block *types.Transaction) (bool, error)
	GetBlocksByHeight(blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error)
	GetChainID() (*types.GetChainIDResponse, error)
	SetBroadcastHandler(topic string, handler func(topic string, data []byte))
	GetForkHeads() (*types.GetForkHeadsResponse, error)
	GetAncestorTopologyAtHeights(blockID *types.Multihash, heights []types.BlockHeightType) ([]types.BlockTopology, error)
	GetBlocksByID(blockID *types.VectorMultihash) (*types.GetBlocksByIDResponse, error)

	GetTopologyAtHeightRange(minHeight types.BlockHeightType, maxHeight types.BlockHeightType) (*types.GetForkHeadsResponse, []types.BlockTopology, error)
}
