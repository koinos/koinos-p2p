package node

import (
	"context"
	"strings"
	"testing"

	types "github.com/koinos/koinos-types-golang"
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
	hi.Height = k.Height
	hi.ID.ID = types.UInt64(k.Height) + k.HeadBlockIDDelta
	return hi, nil
}

// ApplyBlock rpc call
func (k *TestRPC) ApplyBlock(block *types.Block) (bool, error) {
	if k.ApplyBlocks >= 0 && len(k.BlocksApplied) >= k.ApplyBlocks {
		return false, nil
	}

	if k.BlocksApplied != nil {
		b := append(k.BlocksApplied, block)
		k.BlocksApplied = b
	}

	return true, nil
}

func (k *TestRPC) ApplyTransaction(block *types.Block) (bool, error) {
	return true, nil
}

// GetBlocksByHeight rpc call
func (k *TestRPC) GetBlocksByHeight(blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResp, error) {
	blocks := types.NewGetBlocksByHeightResp()
	for i := types.UInt64(0); i < types.UInt64(numBlocks); i++ {
		blockItem := types.NewBlockItem()
		blockItem.BlockHeight = height + types.BlockHeightType(i)
		blockItem.BlockID = *types.NewMultihash()
		blockItem.BlockID.ID = types.UInt64(blockItem.BlockHeight) + k.HeadBlockIDDelta
		blockItem.Block = *types.NewOpaqueBlock()
		//vb := types.NewVariableBlob()
		//block := types.NewBlock()
		//blockItem.BlockBlob = *block.Serialize(vb)
		blocks.BlockItems = append(blocks.BlockItems, *blockItem)
	}

	return blocks, nil
}

// GetChainID rpc call
func (k *TestRPC) GetChainID() (*types.GetChainIDResult, error) {
	mh := types.NewGetChainIDResult()
	mh.ChainID.ID = k.ChainID
	return mh, nil
}

func NewTestRPC(height types.BlockHeightType) *TestRPC {
	rpc := TestRPC{ChainID: 1, Height: height, HeadBlockIDDelta: 0, ApplyBlocks: -1}
	rpc.BlocksApplied = make([]*types.Block, 0)

	return &rpc
}

func TestBasicNode(t *testing.T) {
	ctx := context.Background()

	rpc := NewTestRPC(128)

	// With an explicit seed
	bn, err := NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, 1234)
	if err != nil {
		t.Error(err)
	}

	addr := bn.GetPeerAddress()
	// Check peer address
	if !strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/tcp/8765/p2p/Qm") {
		t.Errorf("Peer address returned by node is not correct")
	}

	bn.Close()

	// With 0 seed
	bn, err = NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, 0)
	if err != nil {
		t.Error(err)
	}

	bn.Close()

	// Give an invalid listen address
	bn, err = NewKoinosP2PNode(ctx, "---", rpc, 0)
	if err == nil {
		bn.Close()
		t.Error("Starting a node with an invalid address should give an error, but it did not")
	}
}