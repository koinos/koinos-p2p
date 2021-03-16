package protocol

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
	"github.com/multiformats/go-multiaddr"
)

// TestRPC implements dummy blockchain RPC.
//
// Note:  This struct and all tests that use it will need to be massively redesigned to test forking.
//
type TestRPC struct {
	ChainID          types.UInt64
	Height           types.BlockHeightType
	HeadBlockIDDelta types.UInt64 // To ensure unique IDs within a "test chain", the multihash ID of each block is its height + this delta
	ApplyBlocks      int          // Number of blocks to apply before failure. < 0 = always apply
	BlocksApplied    []*types.Block
	BlocksByID       map[util.MultihashCmp]*types.Block
}

// getDummyBlockIDAtHeight() gets the ID of the dummy block at the given height
func (k *TestRPC) getDummyBlockIDAtHeight(height types.BlockHeightType) *types.Multihash {
	result := types.NewMultihash()
	result.ID = types.UInt64(height) + k.HeadBlockIDDelta
	return result
}

// getBlockTopologyAtHeight() gets the topology of the dummy block at the given height
func (k *TestRPC) getDummyTopologyAtHeight(height types.BlockHeightType) *types.BlockTopology {
	topo := types.NewBlockTopology()
	topo.ID = *k.getDummyBlockIDAtHeight(height)
	topo.Height = height
	if height > 1 {
		topo.Previous = *k.getDummyBlockIDAtHeight(height - 1)
	}
	return topo
}

// createDummyBlock() creates a dummy block at the given height
func (k *TestRPC) createDummyBlock(height types.BlockHeightType) *types.Block {
	activeData := types.NewActiveBlockData()
	topo := k.getDummyTopologyAtHeight(height)
	activeData.PreviousBlock = topo.Previous
	activeData.Height = height

	block := types.NewBlock()
	block.ActiveData = *types.NewOpaqueActiveBlockDataFromNative(*activeData)
	return block
}

// GetHeadBlock rpc call
func (k *TestRPC) GetHeadBlock(ctx context.Context) (*types.GetHeadInfoResponse, error) {
	hi := types.NewGetHeadInfoResponse()
	hi.Height = k.Height
	hi.ID = *k.getDummyBlockIDAtHeight(hi.Height)
	return hi, nil
}

// ApplyBlock rpc call
func (k *TestRPC) ApplyBlock(ctx context.Context, block *types.Block, topology *types.BlockTopology) (bool, error) {
	if k.ApplyBlocks >= 0 && len(k.BlocksApplied) >= k.ApplyBlocks {
		return false, nil
	}

	if k.BlocksApplied != nil {
		k.BlocksApplied = append(k.BlocksApplied, block)
		k.BlocksByID[util.MultihashToCmp(topology.ID)] = block
	}

	return true, nil
}

func (k *TestRPC) ApplyTransaction(ctx context.Context, txn *types.Transaction) (bool, error) {
	return true, nil
}

func (k *TestRPC) GetBlocksByID(ctx context.Context, blockID *types.VectorMultihash) (*types.GetBlocksByIDResponse, error) {
	resp := types.NewGetBlocksByIDResponse()
	for i := 0; i < len(*blockID); i++ {
		blockID := (*blockID)[i]
		block, hasBlock := k.BlocksByID[util.MultihashToCmp(blockID)]
		if hasBlock {
			block.ActiveData.Unbox()
			activeData, err := block.ActiveData.GetNative()
			if err != nil {
				return nil, err
			}

			item := types.NewBlockItem()
			item.BlockID = blockID
			item.BlockHeight = activeData.Height
			item.Block = *types.NewOpaqueBlockFromNative(*block)
			resp.BlockItems = append(resp.BlockItems, *item)
		}
	}

	return resp, nil
}

// GetBlocksByHeight rpc call
func (k *TestRPC) GetBlocksByHeight(ctx context.Context, blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error) {
	if height+types.BlockHeightType(numBlocks) > k.Height+types.BlockHeightType(len(k.BlocksApplied)) {
		return nil, fmt.Errorf("Requested block exceeded height")
	}

	blocks := types.NewGetBlocksByHeightResponse()
	for i := types.UInt64(0); i < types.UInt64(numBlocks); i++ {
		blockItem := types.NewBlockItem()
		blockItem.BlockHeight = height + types.BlockHeightType(i)
		blockItem.BlockID = *k.getDummyBlockIDAtHeight(blockItem.BlockHeight)
		vb := types.NewVariableBlob()
		block := types.NewBlock()
		activeData := types.NewActiveBlockData()
		block.ActiveData = *types.NewOpaqueActiveBlockDataFromNative(*activeData)
		vb = block.Serialize(vb)
		blockItem.Block = *types.NewOpaqueBlockFromBlob(vb)
		blocks.BlockItems = append(blocks.BlockItems, *blockItem)
	}

	return blocks, nil
}

// GetChainID rpc call
func (k *TestRPC) GetChainID(ctx context.Context) (*types.GetChainIDResponse, error) {
	mh := types.NewGetChainIDResponse()
	mh.ChainID.ID = k.ChainID
	return mh, nil
}

// SetBroadcastHandler
func (k *TestRPC) SetBroadcastHandler(topic string, handler func(topic string, data []byte)) {
	// No test currently needs an implementation of this function,
	// but it has to be defined anyway to satisfy the RPC interface
}

func (k *TestRPC) GetForkHeads(ctx context.Context) (*types.GetForkHeadsResponse, error) {
	resp := types.NewGetForkHeadsResponse()
	if k.Height > 0 {
		resp.ForkHeads = types.VectorBlockTopology{*k.getDummyTopologyAtHeight(k.Height)}
		resp.LastIrreversibleBlock = *k.getDummyTopologyAtHeight(1)
	}
	return resp, nil
}

func (k *TestRPC) GetAncestorTopologyAtHeights(ctx context.Context, blockID *types.Multihash, heights []types.BlockHeightType) ([]types.BlockTopology, error) {
	result := make([]types.BlockTopology, len(heights))
	for i, h := range heights {
		resp, err := k.GetBlocksByHeight(ctx, blockID, h, 1)
		if err != nil {
			return nil, err
		}
		if len(resp.BlockItems) != 1 {
			return nil, errors.New("Unexpected multiple blocks returned")
		}
		resp.BlockItems[0].Block.Unbox()
		block, err := resp.BlockItems[0].Block.GetNative()
		if err != nil {
			return nil, err
		}
		block.ActiveData.Unbox()
		activeData, err := block.ActiveData.GetNative()
		if err != nil {
			return nil, err
		}
		result[i].ID = resp.BlockItems[0].BlockID
		result[i].Height = resp.BlockItems[0].BlockHeight
		result[i].Previous = activeData.PreviousBlock
	}

	return result, nil
}

func (k *TestRPC) GetTopologyAtHeight(ctx context.Context, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetForkHeadsResponse, []types.BlockTopology, error) {
	forkHeads, _ := k.GetForkHeads(ctx)
	result := make([]types.BlockTopology, 0)
	for i := types.UInt32(0); i < numBlocks; i++ {
		h := height + types.BlockHeightType(i)
		if h > k.Height {
			break
		}
		result = append(result, *k.getDummyTopologyAtHeight(h))
	}

	return forkHeads, result, nil
}

func NewTestRPC(height types.BlockHeightType) *TestRPC {
	rpc := TestRPC{ChainID: 1, Height: height, HeadBlockIDDelta: 0, ApplyBlocks: -1}
	rpc.BlocksApplied = make([]*types.Block, 0)
	rpc.BlocksByID = make(map[util.MultihashCmp]*types.Block)

	for h := types.BlockHeightType(1); h <= height; h++ {
		block := rpc.createDummyBlock(h)
		blockID := rpc.getDummyBlockIDAtHeight(h)
		rpc.BlocksByID[util.MultihashToCmp(*blockID)] = block
	}

	return &rpc
}

func createTestClients(listenRPC rpc.RPC, sendRPC rpc.RPC) (*node.KoinosP2PNode, *node.KoinosP2PNode, multiaddr.Multiaddr, multiaddr.Multiaddr, error) {
	listenNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", listenRPC, 1234, *node.NewKoinosP2POptions())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	sendNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", sendRPC, 2345, *node.NewKoinosP2POptions())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return listenNode, sendNode, listenNode.GetPeerAddress(), listenNode.GetPeerAddress(), nil
}

func TestSyncNoError(t *testing.T) {
	// Test no error sync
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	listenNode, sendNode, peer, _, err := createTestClients(listenRPC, sendRPC)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	_, err = sendNode.ConnectToPeer(peer.String())
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Duration(20000) * time.Duration(time.Millisecond))

	// SendRPC should have applied 122 blocks
	if len(sendRPC.BlocksApplied) != 122 {
		t.Errorf("Incorrect number of blocks applied")
	}
}

// Test different chain IDs
func TestSyncChainID(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendRPC.ChainID = 2
	listenNode, sendNode, peer, _, err := createTestClients(listenRPC, sendRPC)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	_, err = sendNode.ConnectToPeer(peer.String())
	if err == nil {
		t.Error("Nodes with different chain ids should return an error, but did not")
	}
}

// Test same head block
func TestSyncHeadBlock(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(128)
	listenNode, sendNode, peer, _, err := createTestClients(listenRPC, sendRPC)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	_, err = sendNode.ConnectToPeer(peer.String())
	if err == nil {
		t.Error("Nodes with same head block should return an error, but did not")
	}
}

// Test same head block
func TestDifferentFork(t *testing.T) {
	//listenRPC := TestRPC{Height: 128, ChainID: 1, HeadBlockIDDelta: 1, ApplyBlocks: -1}
	//sendRPC := TestRPC{Height: 15, ChainID: 1, ApplyBlocks: -1}
	listenRPC := NewTestRPC(128)
	listenRPC.HeadBlockIDDelta = 1
	sendRPC := NewTestRPC(15)
	listenNode, sendNode, peer, _, err := createTestClients(listenRPC, sendRPC)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	_, err = sendNode.ConnectToPeer(peer.String())
	if err == nil {
		t.Error("Nodes on different forks should return an error, but do not")
	}
}

// Test same head block
func TestApplyBlockFailure(t *testing.T) {
	listenRPC := NewTestRPC(128)
	sendRPC := NewTestRPC(5)
	sendRPC.ApplyBlocks = 18
	listenNode, sendNode, peer, _, err := createTestClients(listenRPC, sendRPC)
	if err != nil {
		t.Error(err)
	}
	defer listenNode.Close()
	defer sendNode.Close()

	_, err = sendNode.ConnectToPeer(peer.String())

	time.Sleep(time.Duration(200) * time.Duration(time.Millisecond))

	// SendRPC should have applied 18 blocks
	if len(sendRPC.BlocksApplied) != 18 {
		t.Errorf("Incorrect number of blocks applied")
	}
}

func getChannelError(errs chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}
