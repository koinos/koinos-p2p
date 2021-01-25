package p2p

import (
	"context"
	"strings"
	"testing"

	"github.com/koinos/koinos-p2p/internal/p2p/rpc"
)

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

func TestSyncProtocol(t *testing.T) {
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

	bnSend.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID)

	bnListen.Close()
	bnSend.Close()
}
