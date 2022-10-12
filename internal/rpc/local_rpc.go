package rpc

import (
	"context"

	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
)

// LocalRPC interface for local node RPC methods required for koinos-p2p to function
type LocalRPC interface {
	GetHeadBlock(ctx context.Context) (*chain.GetHeadInfoResponse, error)
	ApplyBlock(ctx context.Context, block *protocol.Block) (*chain.SubmitBlockResponse, error)
	ApplyTransaction(ctx context.Context, block *protocol.Transaction) (*chain.SubmitTransactionResponse, error)
	GetBlocksByHeight(ctx context.Context, blockIDs multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error)
	GetChainID(ctx context.Context) (*chain.GetChainIdResponse, error)
	GetForkHeads(ctx context.Context) (*chain.GetForkHeadsResponse, error)
	GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error)
	BroadcastGossipStatus(ctx context.Context, enabled bool) error

	IsConnectedToBlockStore(ctx context.Context) (bool, error)
	IsConnectedToChain(ctx context.Context) (bool, error)
}
