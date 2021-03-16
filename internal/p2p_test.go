package protocol

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
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
	// log.Printf("ApplyBlock %d\n", topology.Height)
	if k.ApplyBlocks >= 0 && len(k.BlocksApplied) >= k.ApplyBlocks {
		return false, nil
	}

	k.BlocksApplied = append(k.BlocksApplied, block)
	k.BlocksByID[util.MultihashToCmp(topology.ID)] = block
	if topology.Height > k.Height {
		k.Height = topology.Height
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
		log.Printf("Error in GetBlocksByHeight()\n")
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
		// log.Printf("GetForkHeads() response: %v\n", resp.ForkHeads)
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

func SetUnitTestOptions(config *options.Config) {
	config.SyncManagerOptions.RpcTimeoutMs = 30000
	config.SyncManagerOptions.BlacklistMs = 60000
	config.BdmiProviderOptions.PollMyTopologyMs = 20
	config.BdmiProviderOptions.HeightRangeTimeoutMs = 10000
	config.BdmiProviderOptions.HeightInterestReach = 5
	config.BdmiProviderOptions.RescanIntervalMs = 15
	config.DownloadManagerOptions.MaxDownloadDepth = 3
	config.DownloadManagerOptions.MaxDownloadsInFlight = 8
	config.PeerHandlerOptions.HeightRangePollTimeMs = 700
	config.PeerHandlerOptions.DownloadTimeoutMs = 50000
	config.PeerHandlerOptions.RpcTimeoutMs = 10000
}

func createTestClients(listenRPC rpc.RPC, sendRPC rpc.RPC) (*node.KoinosP2PNode, *node.KoinosP2PNode, multiaddr.Multiaddr, multiaddr.Multiaddr, error) {
	config := options.NewConfig()
	SetUnitTestOptions(config)
	config.PeerHandlerOptions.HeightRangePollTimeMs = 100
	config.BdmiProviderOptions.HeightInterestReach = 50
	config.DownloadManagerOptions.MaxDownloadDepth = 15
	config.DownloadManagerOptions.MaxDownloadsInFlight = 25
	config.SetEnableDebugMessages(false)
	listenNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", listenRPC, 1234, config)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	sendNode, err := node.NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", sendRPC, 2345, config)
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

	time.Sleep(time.Duration(2000) * time.Duration(time.Millisecond))

	// SendRPC should have applied 123 blocks
	if len(sendRPC.BlocksApplied) != 123 {
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
	if err != nil {
		t.Error("ConnectToPeer() returned unexpected error")
	}

	ok := false
	for i := 0; i < 20; i++ {
		time.Sleep(time.Duration(100) * time.Duration(time.Millisecond))
		listenError, hasListenError := listenNode.SyncManager.Blacklist.GetBlacklistEntry(sendNode.Host.ID())
		sendError, hasSendError := sendNode.SyncManager.Blacklist.GetBlacklistEntry(listenNode.Host.ID())
		if hasListenError &&
			hasSendError &&
			strings.HasSuffix(listenError.Error.Error(), "peer's chain id does not match") &&
			strings.HasSuffix(sendError.Error.Error(), "peer's chain id does not match") {
			ok = true
			break
		}
	}
	if !ok {
		t.Error("Never got expected error")
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
