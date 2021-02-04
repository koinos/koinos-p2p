package node

import (
	"context"
	"strings"
	"testing"
)

func NewTestRPC(height types.BlockHeightType) *TestRPC {
	rpc := TestRPC{ChainID: 1, Height: height, HeadBlockIDDelta: 0, ApplyBlocks: -1}
	rpc.BlocksApplied = make([]*types.Block, 0)

	return &rpc
}

func TestBasicNode(t *testing.T) {
	ctx := context.Background()

	rpc := NewTestRPC(128)

	// With an explicit seed
	bn, err := node.NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, 1234)
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
	bn, err = node.NewKoinosP2PNode(ctx, "/ip4/127.0.0.1/tcp/8765", rpc, 0)
	if err != nil {
		t.Error(err)
	}

	bn.Close()

	// Give an invalid listen address
	bn, err = node.NewKoinosP2PNode(ctx, "---", rpc, 0)
	if err == nil {
		bn.Close()
		t.Error("Starting a node with an invalid address should give an error, but it did not")
	}
}
