package rpc

import (
	koinos_types "github.com/koinos/koinos-types-golang"
)

// RPC Iterface for RPC methods required for koinos-p2p to function
type RPC interface {
	GetHeadBlock() (*koinos_types.HeadInfo, error)
	ApplyBlock(block *koinos_types.Block) (bool, error)
	ApplyTransaction(block *koinos_types.Block) (bool, error)
	GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.BlockHeightType, numBlocks koinos_types.UInt32) (*koinos_types.GetBlocksByHeightResp, error)
	GetChainID() (*koinos_types.GetChainIDResult, error)
}
