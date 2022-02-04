package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

// TestRPC implements dummy blockchain RPC.
//
// Note:  This struct and all tests that use it will need to be massively redesigned to test forking.
//
type TestRPC struct {
	ChainID          uint64
	Height           uint64
	LastIrreversible uint64
	HeadBlockIDDelta uint64 // To ensure unique IDs within a "test chain", the multihash ID of each block is its height + this delta
	ApplyBlocks      int    // Number of blocks to apply before failure. < 0 = always apply
	BlocksApplied    []*protocol.Block
	BlocksByID       map[string]*protocol.Block
	Mutex            sync.Mutex
}

// getDummyBlockIDAtHeight() gets the ID of the dummy block at the given height
func (k *TestRPC) getDummyBlockIDAtHeight(height uint64) multihash.Multihash {
	result, _ := multihash.Encode(make([]byte, 0), height+k.HeadBlockIDDelta)
	return result
}

// getBlockTopologyAtHeight() gets the topology of the dummy block at the given height
func (k *TestRPC) getDummyTopologyAtHeight(height uint64) *koinos.BlockTopology {
	topo := &koinos.BlockTopology{}
	topo.Id = k.getDummyBlockIDAtHeight(height)
	topo.Height = height
	if height > 1 {
		topo.Previous = k.getDummyBlockIDAtHeight(height - 1)
	}
	return topo
}

// createDummyBlock() creates a dummy block at the given height
func (k *TestRPC) createDummyBlock(height uint64) *protocol.Block {
	topo := k.getDummyTopologyAtHeight(height)

	block := &protocol.Block{
		Id: topo.Id,
		Header: &protocol.BlockHeader{
			Height:   height,
			Previous: topo.Previous,
		},
	}

	return block
}

// GetHeadBlock rpc call
func (k *TestRPC) GetHeadBlock(ctx context.Context) (*chain.GetHeadInfoResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	hi := &chain.GetHeadInfoResponse{}
	hi.HeadTopology = &koinos.BlockTopology{}
	hi.HeadTopology.Height = k.Height
	hi.HeadTopology.Id = k.getDummyBlockIDAtHeight(hi.HeadTopology.Height)
	if k.Height > 0 {
		hi.HeadTopology.Previous = k.getDummyBlockIDAtHeight(hi.HeadTopology.Height)
	}
	return hi, nil
}

// ApplyBlock rpc call
func (k *TestRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chain.SubmitBlockResponse, error) {
	if k.ApplyBlocks >= 0 && len(k.BlocksApplied) >= k.ApplyBlocks {
		return &chain.SubmitBlockResponse{}, nil
	}

	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	if _, ok := k.BlocksByID[string(block.Id)]; !ok {
		k.BlocksApplied = append(k.BlocksApplied, block)

		k.BlocksByID[string(block.Id)] = block
		if block.Header.Height > k.Height {
			k.Height = block.Header.Height
			k.LastIrreversible = k.Height - 5
		}
	}

	return &chain.SubmitBlockResponse{}, nil
}

func (k *TestRPC) ApplyTransaction(ctx context.Context, block *protocol.Transaction) (*chain.SubmitTransactionResponse, error) {
	return &chain.SubmitTransactionResponse{}, nil
}

func (k *TestRPC) BroadcastGossipStatus(enabled bool) error {
	return nil
}

func (k *TestRPC) GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	resp := &block_store.GetBlocksByIdResponse{}
	for _, blockID := range blockIDs {
		block, hasBlock := k.BlocksByID[string(blockID)]
		if hasBlock {
			item := &block_store.BlockItem{}
			item.BlockId = blockID
			item.BlockHeight = block.Header.Height
			item.Block = block
			resp.BlockItems = append(resp.BlockItems, item)
		}
	}

	return resp, nil
}

// GetBlocksByHeight rpc call
func (k *TestRPC) GetBlocksByHeight(ctx context.Context, blockID multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	blocks := &block_store.GetBlocksByHeightResponse{}
	for i := uint64(0); i < uint64(numBlocks); i++ {
		if height+i > k.Height {
			break
		}

		blockItem := &block_store.BlockItem{}
		blockItem.Block = k.createDummyBlock(height + i)
		blockItem.BlockHeight = blockItem.Block.Header.Height
		blockItem.BlockId = blockItem.Block.Id

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

func (k *TestRPC) GetForkHeads(ctx context.Context) (*chain.GetForkHeadsResponse, error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	resp := &chain.GetForkHeadsResponse{}
	if k.Height > 0 {
		resp.ForkHeads = make([]*koinos.BlockTopology, 1)
		resp.ForkHeads[0] = k.getDummyTopologyAtHeight(k.Height)
		resp.LastIrreversibleBlock = k.getDummyTopologyAtHeight(k.LastIrreversible)
	}
	return resp, nil
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
	rpc := TestRPC{
		ChainID:          1,
		Height:           height,
		LastIrreversible: lastIrr,
		HeadBlockIDDelta: 0,
		ApplyBlocks:      -1,
		BlocksApplied:    make([]*protocol.Block, 0),
		BlocksByID:       make(map[string]*protocol.Block),
	}

	for h := uint64(1); h <= height; h++ {
		block := rpc.createDummyBlock(h)
		blockID := rpc.getDummyBlockIDAtHeight(h)
		rpc.BlocksByID[string(blockID)] = block
	}

	return &rpc
}

func SetUnitTestOptions(config *options.Config) {
}

func createTestClients(listenRPC rpc.LocalRPC, listenConfig *options.Config, sendRPC rpc.LocalRPC, sendConfig *options.Config) (*node.KoinosP2PNode, *node.KoinosP2PNode, multiaddr.Multiaddr, multiaddr.Multiaddr, error) {
	listenNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", listenRPC, nil, "test1", listenConfig)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	listenNode.Start(context.Background())

	sendNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", sendRPC, nil, "test2", sendConfig)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sendNode.Start(context.Background())

	return listenNode, sendNode, listenNode.GetAddress(), listenNode.GetAddress(), nil
}

func TestSyncNoError(t *testing.T) {
	// Test no error sync
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendConfig := options.NewConfig()
	sendConfig.PeerConnectionOptions.Checkpoints = []options.Checkpoint{{BlockHeight: 10, BlockID: listenRPC.getDummyBlockIDAtHeight(10)}}
	listenNode, sendNode, addr, _, err := createTestClients(listenRPC, options.NewConfig(), sendRPC, sendConfig)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	p, _ := peer.AddrInfoFromP2pAddr(addr)
	err = sendNode.ConnectToPeerAddress(context.Background(), p)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Duration(3000) * time.Duration(time.Millisecond))

	if len(sendRPC.BlocksApplied) != 123 {
		t.Errorf("Incorrect number of blocks applied. Exepcted 123, was %v", len(sendRPC.BlocksApplied))
	}

	if len(sendRPC.BlocksByID) != len(listenRPC.BlocksByID) {
		t.Errorf("Incorrect number of blocks by id. Expected %v, was %v", len(listenRPC.BlocksByID), len(sendRPC.BlocksByID))
	}
}

// Test different chain IDs
func TestSyncChainID(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendRPC.ChainID = 2
	listenNode, sendNode, addr, _, err := createTestClients(listenRPC, options.NewConfig(), sendRPC, options.NewConfig())
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	p, _ := peer.AddrInfoFromP2pAddr(addr)
	err = sendNode.ConnectToPeerAddress(context.Background(), p)
	if err != nil {
		t.Error("ConnectToPeer() returned unexpected error")
	}

	for i := 0; i < 20; i++ {
		time.Sleep(time.Duration(100) * time.Duration(time.Millisecond))
		if len(sendRPC.BlocksApplied) != 0 {
			t.Errorf("Incorrect number of blocks applied. Exepcted 0, was %v", len(sendRPC.BlocksApplied))
		}

		if len(sendRPC.BlocksByID) != 5 {
			t.Errorf("Incorrect number of blocks by id. Expected 5, was %v", len(sendRPC.BlocksByID))
		}
	}
}

// Test same head block
func TestApplyBlockFailure(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendRPC.ApplyBlocks = 18
	listenNode, sendNode, addr, _, err := createTestClients(listenRPC, options.NewConfig(), sendRPC, options.NewConfig())
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	p, _ := peer.AddrInfoFromP2pAddr(addr)
	err = sendNode.ConnectToPeerAddress(context.Background(), p)

	time.Sleep(time.Duration(800) * time.Duration(time.Millisecond))

	expectedBlocksApplied := 18

	// SendRPC should have applied 18 blocks
	if len(sendRPC.BlocksApplied) != expectedBlocksApplied {
		t.Errorf("Incorrect number of blocks applied, expected %d, got %d", expectedBlocksApplied, len(sendRPC.BlocksApplied))
	}
}

func TestCheckpointFailure(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendConfig := options.NewConfig()
	sendConfig.PeerConnectionOptions.Checkpoints = []options.Checkpoint{{BlockHeight: 10, BlockID: listenRPC.getDummyBlockIDAtHeight(11)}}
	listenNode, sendNode, addr, _, err := createTestClients(listenRPC, options.NewConfig(), sendRPC, sendConfig)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	p, _ := peer.AddrInfoFromP2pAddr(addr)
	err = sendNode.ConnectToPeerAddress(context.Background(), p)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Duration(1000) * time.Duration(time.Millisecond))

	expectedBlocksApplied := 0

	// SendRPC should have applied 0 blocks
	if len(sendRPC.BlocksApplied) != expectedBlocksApplied {
		t.Errorf("Incorrect number of blocks applied, expected %d, got %d", expectedBlocksApplied, len(sendRPC.BlocksApplied))
	}
}
