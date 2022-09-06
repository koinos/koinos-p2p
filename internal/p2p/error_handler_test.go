package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type testPeerStore struct {
	store map[peer.ID]peer.AddrInfo
}

func (t *testPeerStore) PeerInfo(id peer.ID) peer.AddrInfo {
	if info, ok := t.store[id]; ok {
		return info
	}

	return peer.AddrInfo{
		ID:    id,
		Addrs: make([]ma.Multiaddr, 0),
	}
}

func TestErrorHandler(t *testing.T) {
	disconnectPeerChan := make(chan peer.ID)
	peerErrorChan := make(chan PeerError)
	opts := options.NewPeerErrorHandlerOptions()
	ctx := context.Background()

	opts.BlockApplicationErrorScore = 10
	opts.ErrorScoreThreshold = 100
	opts.ErrorScoreDecayHalflife = time.Second * 2

	peerStore := &testPeerStore{
		store: make(map[peer.ID]peer.AddrInfo),
	}
	peerAddr, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/80")
	peerStore.store["peerA"] = peer.AddrInfo{ID: "peerA", Addrs: []ma.Multiaddr{peerAddr}}
	peerStore.store["peerB"] = peer.AddrInfo{ID: "peerB", Addrs: []ma.Multiaddr{peerAddr}}

	errorHandler := NewPeerErrorHandler(disconnectPeerChan, peerErrorChan, *opts)
	errorHandler.SetPeerStore(peerStore)
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

	time.Sleep(time.Millisecond * 500)

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
