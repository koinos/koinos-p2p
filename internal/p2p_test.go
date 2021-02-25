package protocol

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/rpc"
	types "github.com/koinos/koinos-types-golang"
	"github.com/multiformats/go-multiaddr"
)

type TestRPC struct {
	ChainID          types.UInt64
	Height           types.BlockHeightType
	HeadBlockIDDelta types.UInt64 // To ensure unique IDs within a "test chain", the multihash ID of each block is its height + this delta
	ApplyBlocks      int          // Number of blocks to apply before failure. < 0 = always apply
	BlocksApplied    []*types.Block
}

// GetHeadBlock rpc call
func (k *TestRPC) GetHeadBlock() (*types.HeadInfo, error) {
	hi := types.NewHeadInfo()
	hi.Height = k.Height + types.BlockHeightType(len(k.BlocksApplied))
	hi.ID.ID = types.UInt64(k.Height) + k.HeadBlockIDDelta
	return hi, nil
}

// ApplyBlock rpc call
func (k *TestRPC) ApplyBlock(block *types.Block, topology ...*types.BlockTopology) (bool, error) {
	if k.ApplyBlocks >= 0 && len(k.BlocksApplied) >= k.ApplyBlocks {
		return false, nil
	}

	if k.BlocksApplied != nil {
		b := append(k.BlocksApplied, block)
		k.BlocksApplied = b
	}

	return true, nil
}

func (k *TestRPC) ApplyTransaction(txn *types.Transaction) (bool, error) {
	return true, nil
}

// GetBlocksByHeight rpc call
func (k *TestRPC) GetBlocksByHeight(blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error) {
	if height+types.BlockHeightType(numBlocks) > k.Height+types.BlockHeightType(len(k.BlocksApplied)) {
		return nil, fmt.Errorf("Requested block exceeded height")
	}

	blocks := types.NewGetBlocksByHeightResponse()
	for i := types.UInt64(0); i < types.UInt64(numBlocks); i++ {
		blockItem := types.NewBlockItem()
		blockItem.BlockHeight = height + types.BlockHeightType(i)
		blockItem.BlockID = *types.NewMultihash()
		blockItem.BlockID.ID = types.UInt64(blockItem.BlockHeight) + k.HeadBlockIDDelta
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
func (k *TestRPC) GetChainID() (*types.GetChainIDResponse, error) {
	mh := types.NewGetChainIDResponse()
	mh.ChainID.ID = k.ChainID
	return mh, nil
}

func NewTestRPC(height types.BlockHeightType) *TestRPC {
	rpc := TestRPC{ChainID: 1, Height: height, HeadBlockIDDelta: 0, ApplyBlocks: -1}
	rpc.BlocksApplied = make([]*types.Block, 0)

	return &rpc
}

func createTestClients(listenRPC rpc.RPC, sendRPC rpc.RPC) (*node.KoinosP2PNode, *node.KoinosP2PNode, multiaddr.Multiaddr, multiaddr.Multiaddr, error) {
	listenNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", listenRPC, 1234)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	sendNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", sendRPC, 2345)
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

	time.Sleep(time.Duration(200) * time.Duration(time.Millisecond))

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
