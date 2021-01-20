package p2p

import (
	koinos_types "github.com/koinos/koinos-types-golang"
)

// KoinosRPC Implementation of RPC Interface
type KoinosRPC struct {}

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
func (k KoinosRPC) ApplyBlock( block *koinos_types.Block ) bool {
	return true
}

// GetAncestorAtHeight rpc call
func (k KoinosRPC) GetAncestorAtHeight( blockID *koinos_types.Multihash, height koinos_types.UInt32 ) *koinos_types.Block {
	return koinos_types.NewBlock()
}

// GetChainID rpc call
func (k KoinosRPC) GetChainID() *koinos_types.Multihash {
	return koinos_types.NewMultihash()
}
