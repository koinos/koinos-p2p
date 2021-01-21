package p2p

import (
	koinos_types "github.com/koinos/koinos-types-golang"
)

// RPC Iterface for RPC methods required for koinos-p2p to function
type RPC interface {
	GetHeadBlock() *koinos_types.BlockTopology
	ApplyBlock(block *koinos_types.Block) bool
	GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.UInt32, numBlock koinos_types.Uint32) *[]koinos_types.Block
	GetChainID() *koinos_types.Multihash
}
