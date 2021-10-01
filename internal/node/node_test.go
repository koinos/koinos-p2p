package node

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
)

type TestRPC struct {
	ChainID          uint64
	Height           uint64
	LastIrreversible uint64
	HeadBlockIDDelta uint64 // To ensure unique IDs within a "test chain", the multihash ID of each block is its height + this delta
	ApplyBlocks      int    // Number of blocks to apply before failure. < 0 = always apply
	BlocksByID       map[string]*protocol.Block
	BlocksByHeight   map[uint64]*protocol.Block
	Mutex            sync.Mutex
}

// GetHeadBlock rpc call
func (k *TestRPC) GetHeadBlock(ctx context.Context) (*chain.GetHeadInfoResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	hi := chain.GetHeadInfoResponse{}
	hi.HeadTopology.Height = k.Height
	hi.HeadTopology.Id, _ = multihash.Encode(make([]byte, 0), k.Height+k.HeadBlockIDDelta)
	binary.PutUvarint(hi.HeadTopology.Id, k.Height+k.HeadBlockIDDelta)
	return &hi, nil
}

// ApplyBlock rpc call
func (k *TestRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chain.SubmitBlockResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	if k.ApplyBlocks >= 0 && len(k.BlocksByHeight) >= k.ApplyBlocks {
		return &chain.SubmitBlockResponse{}, nil
	}

	k.BlocksByID[string(block.Id)] = block
	k.BlocksByHeight[block.Header.Height] = block

	return &chain.SubmitBlockResponse{}, nil
}

func (k *TestRPC) ApplyTransaction(ctx context.Context, block *protocol.Transaction) (*chain.SubmitTransactionResponse, error) {
	return &chain.SubmitTransactionResponse{}, nil
}

func (k *TestRPC) GetForkHeads(ctx context.Context) (*chain.GetForkHeadsResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	fh := &chain.GetForkHeadsResponse{}
	fh.LastIrreversibleBlock = &koinos.BlockTopology{}
	fh.LastIrreversibleBlock.Height = k.LastIrreversible
	fh.LastIrreversibleBlock.Id, _ = multihash.Encode(make([]byte, 0), k.LastIrreversible+k.HeadBlockIDDelta)
	return fh, nil
}

func (k *TestRPC) GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error) {
	return nil, nil
}

// GetBlocksByHeight rpc call
func (k *TestRPC) GetBlocksByHeight(ctx context.Context, blockID multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	blocks := &block_store.GetBlocksByHeightResponse{}
	for i := uint64(0); i < uint64(numBlocks); i++ {
		blockItem := &block_store.BlockItem{}
		blockItem.BlockHeight = height + i
		blockItem.BlockId, _ = multihash.Encode(make([]byte, 0), blockItem.BlockHeight+k.HeadBlockIDDelta)
		blocks.BlockItems = append(blocks.BlockItems, blockItem)
	}

	return blocks, nil
}

// GetChainID rpc call
func (k *TestRPC) GetChainID(ctx context.Context) (*chain.GetChainIdResponse, error) {
	mh := &chain.GetChainIdResponse{}
	mh.ChainId, _ = multihash.Encode(make([]byte, 0), k.ChainID)
	return mh, nil
}

func (k *TestRPC) IsConnectedToBlockStore(ctx context.Context) (bool, error) {
	return true, nil
}

func (k *TestRPC) IsConnectedToChain(ctx context.Context) (bool, error) {
	return true, nil
}

func NewTestRPC(height uint64) *TestRPC {
	var lastIrr uint64
	if height > 5 {
		lastIrr = height - 5
	} else {
		lastIrr = 1
	}
	return &TestRPC{
		ChainID:          1,
		LastIrreversible: lastIrr,
		Height:           height,
		HeadBlockIDDelta: 0,
		ApplyBlocks:      -1,
		BlocksByID:       make(map[string]*protocol.Block),
		BlocksByHeight:   make(map[uint64]*protocol.Block),
	}
}

func TestBasicNode(t *testing.T) {
	ctx := context.Background()

	rpc := NewTestRPC(128)

	// With an explicit seed
	bn, err := NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, nil, "test1", options.NewConfig())
	if err != nil {
		t.Error(err)
	}

	addr := bn.GetPeerAddress()
	// Check peer address
	if !strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/tcp/8765/p2p/Qm") {
		t.Errorf("Peer address returned by node is not correct")
	}

	bn.Close()

	// With blank seed
	bn, err = NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, nil, "", options.NewConfig())
	if err != nil {
		t.Error(err)
	}

	bn.Close()

	// Give an invalid listen address
	bn, err = NewKoinosP2PNode(ctx, "---", rpc, nil, "", options.NewConfig())
	if err == nil {
		bn.Close()
		t.Error("Starting a node with an invalid address should give an error, but it did not")
	}
}
