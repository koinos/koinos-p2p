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
	Height             types.BlockHeightType
	MultihashID        types.UInt64
	ApplyBlockResponse bool
}

// GetHeadBlock rpc call
func (k TestRPC) GetHeadBlock() *types.BlockTopology {
	bt := types.NewBlockTopology()
	bt.Height = k.Height
	return bt
}

// ApplyBlock rpc call
func (k TestRPC) ApplyBlock(block *types.Block) bool {
	return true
}

// GetBlocksByHeight rpc call
func (k TestRPC) GetBlocksByHeight(blockID *types.Multihash, height types.UInt32, numBlock types.UInt32) *[]types.Block {
	blocks := make([]types.Block, 0)
	return &blocks
}

// GetChainID rpc call
func (k TestRPC) GetChainID() *types.Multihash {
	mh := types.NewMultihash()
	mh.ID = k.MultihashID
	return mh
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

	bnListen, err := NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8765", rpc, 1234)
	if err != nil {
		t.Error(err)
	}

	bnSend, err := NewKoinosP2PNode(context.Background(), "/ip4/127.0.0.1/tcp/8888", rpc, 2345)
	if err != nil {
		t.Error(err)
	}

	// Connect to the listener
	peerAddr := bnListen.GetPeerAddress()
	peer, err := bnSend.ConnectToPeer(peerAddr.String())
	if err != nil {
		t.Error(err)
	}

	bnSend.Protocols.Broadcast.InitiateProtocol(context.Background(), peer.ID)

	bnListen.Close()
	bnSend.Close()
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
			listenRPC := TestRPC{Height: 128, MultihashID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 5, MultihashID: 1, ApplyBlockResponse: true}
			listenNode, sendNode, peer, err := createTestClients(listenRPC, sendRPC)

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
			listenRPC := TestRPC{Height: 128, MultihashID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 5, MultihashID: 2, ApplyBlockResponse: true}
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
			listenRPC := TestRPC{Height: 128, MultihashID: 1, ApplyBlockResponse: true}
			sendRPC := TestRPC{Height: 128, MultihashID: 1, ApplyBlockResponse: true}
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
