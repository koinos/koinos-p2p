package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestErrorHanlder(t *testing.T) {
	disconnectPeerChan := make(chan peer.ID)
	peerErrorChan := make(chan PeerError)
	opts := options.NewPeerErrorHandlerOptions()
	ctx := context.Background()

	opts.BlockApplicationErrorScore = 10
	opts.ErrorScoreThreshold = 100
	opts.ErrorScoreDecayHalflife = time.Second * 2

	errorHandler := NewPeerErrorHandler(disconnectPeerChan, peerErrorChan, *opts)
	errorHandler.Start(ctx)

	
	for i := 0; i < 12; i++ {
		peerErrorChan <- PeerError{id: "peerA", err: p2perrors.ErrBlockApplication}
	}

	exp, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	select {
	case peer := <-disconnectPeerChan:
		cancel()
		if peer != "peerA" {
			t.Errorf("Incorrect peer requested for disconnect. Expected: peerA, Was %s", peer)
		}
	case <-exp.Done():
		t.Errorf("Expected request to disconnect from peerA never received")
	}

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
		cancel()
		if peer != "peerA" {
			t.Errorf("Incorrect peer requested for disconnect. Expected: peerA, Was %s", peer)
		}
	case <-exp.Done():
		t.Errorf("Expected request to disconnect from peerA never received")
	}

	if errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected failed connection to peerA")
	}

	time.Sleep(time.Second)

	if errorHandler.CanConnect(ctx, "peerA") {
		t.Errorf("Expected failed connection to peerA")
	}
}
