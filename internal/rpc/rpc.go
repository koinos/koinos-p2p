package rpc

import (
	koinos_types "github.com/koinos/koinos-types-golang"
)

// RPC Interface for RPC methods required for koinos-p2p to function
type RPC interface {
	GetHeadBlock() (*koinos_types.HeadInfo, error)
	ApplyBlock(block *koinos_types.Block, topology ...*koinos_types.BlockTopology) (bool, error)
	ApplyTransaction(block *koinos_types.Transaction) (bool, error)
	GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.BlockHeightType, numBlocks koinos_types.UInt32) (*koinos_types.GetBlocksByHeightResponse, error)
	GetChainID() (*koinos_types.GetChainIDResponse, error)
	SetBroadcastHandler(topic string, handler func(topic string, data []byte))
}
