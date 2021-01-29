package p2p

import (
	"context"
	"strings"
	"testing"

	"github.com/koinos/koinos-p2p/internal/p2p/rpc"
	types "github.com/koinos/koinos-types-golang"
	"github.com/libp2p/go-libp2p-core/peer"
)

type TestRPC struct {
	ChainID            types.UInt64
	Height             types.BlockHeightType
	HeadBlockIDDelta   types.UInt64 // To ensure unique IDs within a "test chain", the multihash ID of each block is its height + this delta
	ApplyBlockResponse bool
}

// GetHeadBlock rpc call
func (k TestRPC) GetHeadBlock() (*types.HeadInfo, error) {
	hi := types.NewHeadInfo()
	hi.Height = k.Height
	return hi, nil
}

// ApplyBlock rpc call
func (k TestRPC) ApplyBlock(block *types.Block) (bool, error) {
	return true, nil
}

// GetBlocksByHeight rpc call
func (k TestRPC) GetBlocksByHeight(blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResp, error) {
	blocks := types.NewGetBlocksByHeightResp()
	for i := types.UInt64(0); i < types.UInt64(numBlocks); i++ {
		blockItem := types.NewBlockItem()
		blockItem.BlockHeight = height + types.BlockHeightType(i)
		blockItem.BlockID = *types.NewMultihash()
		blockItem.BlockID.ID = types.UInt64(blockItem.BlockHeight) + k.HeadBlockIDDelta
		blocks.BlockItems = append(blocks.BlockItems, *blockItem)
	}

	return blocks, nil
}

// GetChainID rpc call
func (k TestRPC) GetChainID() (*types.GetChainIDResult, error) {
	mh := types.NewGetChainIDResult()
	mh.ChainID.ID = k.ChainID
	return mh, nil
}

func TestBasicNode(t *testing.T) {
	ctx := context.Background()

	rpc := rpc.NewKoinosRPC()

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

	// With 0 seed
	bn, err = NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, 0)
	if err != nil {
		t.Error(err)
	}
	bn.Close()

	// Give an invalid listen address
	bn, err = NewKoinosP2PNode(ctx, "---", rpc, 0)
	if err == nil {
		t.Error("Starting a node with an invalid address should give an error, but it did not")
	}
}

func TestBroadcastProtocol(t *testing.T) {
	rpc := rpc.NewKoinosRPC()
	listenNode, sendNode, peer, err := createTestClients(rpc, rpc)
	if err != nil {
		t.Error(err)
	}

	sendNode.Protocols.Broadcast.InitiateProtocol(context.Background(), peer.ID)

	listenNode.Close()
	sendNode.Close()
}

func createTestClients(listenRPC rpc.RPC, sendRPC rpc.RPC) (*KoinosP2PNode, *KoinosP2PNode, *peer.AddrInfo, error) {
	listenNode, err := NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", listenRPC, 1234)
	if err != nil {
		return nil, nil, nil, err
	}

	sendNode, err := NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", sendRPC, 2345)
	if err != nil {
		return nil, nil, nil, err
	}

	// Connect to the listener
	peerAddr := listenNode.GetPeerAddress()
	peer, err := sendNode.ConnectToPeer(peerAddr.String())
	if err != nil {
		return nil, nil, nil, err
	}

	return listenNode, sendNode, peer, nil
}

func TestSyncProtocol(t *testing.T) {
	{
		// Test no error sync
		{
			listenRPC := TestRPC{Height: 128, ChainID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 5, ChainID: 1, ApplyBlockResponse: true}
			listenNode, sendNode, peer, err := createTestClients(listenRPC, sendRPC)
			if err != nil {
				t.Error(err)
			}

			errs := make(chan error, 1)
			sendNode.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID, errs)
			err = getChannelError(errs)
			if err != nil {
				t.Error(err)
			}

			listenNode.Close()
			sendNode.Close()
		}

		// Test different chain IDs
		{
			listenRPC := TestRPC{Height: 128, ChainID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 5, ChainID: 2, ApplyBlockResponse: true}
			listenNode, sendNode, peer, err := createTestClients(listenRPC, sendRPC)

			errs := make(chan error, 1)
			sendNode.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID, errs)
			err = getChannelError(errs)
			if err == nil {
				t.Error("Nodes with different chain ids should return an error, but did not")
			}

			listenNode.Close()
			sendNode.Close()
		}

		// Test same head block
		{
			listenRPC := TestRPC{Height: 128, ChainID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 128, ChainID: 1, ApplyBlockResponse: true}
			listenNode, sendNode, peer, err := createTestClients(listenRPC, sendRPC)

			errs := make(chan error, 1)
			sendNode.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID, errs)
			err = getChannelError(errs)
			if err == nil {
				t.Error("Nodes with same head block should return an error, but did not")
			}

			listenNode.Close()
			sendNode.Close()
		}
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
