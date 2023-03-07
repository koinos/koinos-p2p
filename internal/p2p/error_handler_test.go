package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type testProvider struct {
	store map[peer.ID]ma.Multiaddr
}

func (t *testProvider) GetPeerAddress(_ context.Context, id peer.ID) ma.Multiaddr {
	if addr, ok := t.store[id]; ok {
		return addr
	}

	return nil
}

func TestErrorHandler(t *testing.T) {
	disconnectPeerChan := make(chan peer.ID)
	peerErrorChan := make(chan PeerError)
	opts := options.NewPeerErrorHandlerOptions()
	ctx := context.Background()

	opts.BlockApplicationErrorScore = 10
	opts.ErrorScoreThreshold = 100
	opts.ErrorScoreReconnectThreshold = 50
	opts.ErrorScoreDecayHalflife = time.Second * 2

	peerStore := &testProvider{
		store: make(map[peer.ID]ma.Multiaddr),
	}
	peerAddrA, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/80")
	peerAddrB, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/81")
	peerStore.store["peerA"] = peerAddrA
	peerStore.store["peerB"] = peerAddrB

	errorHandler := NewPeerErrorHandler(disconnectPeerChan, peerErrorChan, *opts)
	errorHandler.SetPeerAddressProvider(peerStore)
	errorHandler.Start(ctx)

	for i := 0; i < 12; i++ {
		peerErrorChan <- PeerError{id: "peerA", err: p2perrors.ErrBlockApplication}
	}

	exp, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	select {
	case peer := <-disconnectPeerChan:
		if peer != "peerA" {
			t.Errorf("Incorrect peer requested for disconnect. Expected: peerA, Was %s", peer)
		}
	case <-exp.Done():
		t.Errorf("Expected request to disconnect from peerA never received")
	}
	cancel()

	if errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected failed connection to peerA")
	}

	time.Sleep(time.Millisecond * 2500)

	if !errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected successful connection to peerA")
	}

	peerErrorChan <- PeerError{id: "peerA", err: p2perrors.ErrChainIDMismatch}

	exp, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	select {
	case peer := <-disconnectPeerChan:
		if peer != "peerA" {
			t.Errorf("Incorrect peer requested for disconnect. Expected: peerA, Was %s", peer)
		}
	case <-exp.Done():
		t.Errorf("Expected request to disconnect from peerA never received")
	}
	cancel()

	if errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected failed connection to peerA")
	}

	time.Sleep(time.Second)

	if errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected failed connection to peerA")
	}

	if errorHandler.CanConnect(ctx, "peerB") {
		t.Errorf("Expected failed connection to peerB")
	}

	if !errorHandler.CanConnect(ctx, "peerC") {
		t.Errorf("Expected succesful connection to peerC")
	}
}
