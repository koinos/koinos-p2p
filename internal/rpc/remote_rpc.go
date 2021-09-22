package rpc

import (
	"context"

	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/multiformats/go-multihash"
)

// RemoteRPC interface for remote node RPC methods required for koinos-p2p to function
type RemoteRPC interface {
	GetChainID(ctx context.Context) (id multihash.Multihash, err error)
	GetHeadBlock(ctx context.Context) (id multihash.Multihash, height uint64, err error)
	GetAncestorBlockID(ctx context.Context, parentID multihash.Multihash, childHeight uint64) (id multihash.Multihash, err error)
	GetBlocks(ctx context.Context, headBlockID multihash.Multihash, startBlockHeight uint64, batchSize uint32) (blocks []protocol.Block, err error)
}
