package rpc

import (
	koinos_types "github.com/koinos/koinos-types-golang"
)

// KoinosRPC Implementation of RPC Interface
type KoinosRPC struct{}

// NewKoinosRPC factory
func NewKoinosRPC() *KoinosRPC {
	rpc := KoinosRPC{}
	return &rpc
}

// GetHeadBlock rpc call
func (k KoinosRPC) GetHeadBlock() *koinos_types.BlockTopology {
	return koinos_types.NewBlockTopology()
}

// ApplyBlock rpc call
func (k KoinosRPC) ApplyBlock(block *koinos_types.Block) bool {
	return true
}

// GetBlocksByHeight rpc call
func (k KoinosRPC) GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.UInt32, numBlock koinos_types.UInt32) *[]koinos_types.Block {
	blocks := make([]koinos_types.Block, 0)
	return &blocks
}

// GetChainID rpc call
func (k KoinosRPC) GetChainID() *koinos_types.Multihash {
	return koinos_types.NewMultihash()
}
