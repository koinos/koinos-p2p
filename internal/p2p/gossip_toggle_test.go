package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
)

type TestGossipEnableHandler struct {
	enabled bool
}

func (t *TestGossipEnableHandler) EnableGossip(ctx context.Context, enabled bool) {
	t.enabled = enabled
}

func TestNormalGossipToggle(t *testing.T) {
	ctx := context.Background()
	testHandler := TestGossipEnableHandler{false}
	opts := options.NewGossipToggleOptions()
	opts.AlwaysDisable = false
	opts.AlwaysEnable = false

	gossipToggle := NewGossipToggle(&testHandler, *opts)
	gossipToggle.Start(ctx)

	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled on startup")
	}

	timePoint := time.Now().UnixMilli()
	gossipToggle.UpdateHeadTime(uint64(timePoint))

	time.Sleep(2 * time.Second)

	if !testHandler.enabled {
		t.Error("Gossip should be enabled")
	}

	timePoint = time.Now().Add(-1 * (time.Second * 65)).UnixMilli()

	gossipToggle.UpdateHeadTime(uint64(timePoint))

	time.Sleep(2 * time.Second)

	if testHandler.enabled {
		t.Error("Gossip should be disabled")
	}

	timePoint = time.Now().UnixMilli()

	gossipToggle.UpdateHeadTime(uint64(timePoint))

	time.Sleep(2 * time.Second)

	if !testHandler.enabled {
		t.Error("Gossip should be enabled")
	}
}

func TestAlwaysEnabledGossipToggle(t *testing.T) {
	ctx := context.Background()
	testHandler := TestGossipEnableHandler{false}
	opts := options.NewGossipToggleOptions()
	opts.AlwaysDisable = false
	opts.AlwaysEnable = true

	gossipToggle := NewGossipToggle(&testHandler, *opts)
	gossipToggle.Start(ctx)
	time.Sleep(time.Millisecond * 5)

	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled on startup")
	}

	timePoint := time.Now().Add(-1 * (time.Second * 65)).UnixMilli()

	gossipToggle.UpdateHeadTime(uint64(timePoint))

	time.Sleep(2 * time.Second)

	if !testHandler.enabled {
		t.Error("Gossip should be enabled")
	}
}

func TestAlwaysDisabledGossipToggle(t *testing.T) {
	ctx := context.Background()
	testHandler := TestGossipEnableHandler{false}
	opts := options.NewGossipToggleOptions()
	opts.AlwaysDisable = true
	opts.AlwaysEnable = false

	gossipToggle := NewGossipToggle(&testHandler, *opts)
	gossipToggle.Start(ctx)
	time.Sleep(time.Millisecond * 5)

	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled on startup")
	}

	timePoint := time.Now().UnixMilli()
	gossipToggle.UpdateHeadTime(uint64(timePoint))

	time.Sleep(2 * time.Second)

	if testHandler.enabled {
		t.Error("Gossip should be disabled")
	}
}
