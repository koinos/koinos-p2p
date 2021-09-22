package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/libp2p/go-libp2p-core/peer"
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
	voteChan := make(chan GossipVote)
	peerDisconnectedChan := make(chan peer.ID)
	opts := options.NewGossipToggleOptions()
	opts.EnableThreshold = 2.0 / 3.0
	opts.DisableThreshold = 1.0 / 3.0
	opts.AlwaysDisable = false
	opts.AlwaysEnable = false

	gossipToggle := NewGossipToggle(&testHandler, voteChan, peerDisconnectedChan, *opts)
	gossipToggle.Start(ctx)
	time.Sleep(time.Millisecond * 5)

	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled on startup")
	}

	peers := []peer.ID{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	for _, p := range peers {
		voteChan <- GossipVote{p, false}
	}

	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled when adding peers")
	}

	// 0-4: yes, 5-8: no, 0.55%
	for i := 0; i < 5; i++ {
		voteChan <- GossipVote{peers[i], true}
		time.Sleep(time.Millisecond * 5)
		if testHandler.enabled {
			t.Errorf("Gossip was incorrectly enabled too soon")
		}
	}

	// 0-5: yes, 6-8: no, 0.66%
	voteChan <- GossipVote{peers[5], true}
	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was not enabled when it should be")
	}

	for i := 0; i < 5; i++ {
		voteChan <- GossipVote{peers[8], false}
		time.Sleep(time.Millisecond * 5)
		if !testHandler.enabled {
			t.Errorf("Gossip was disabled from a double vote")
		}
	}

	// 0-4: yes, 5-8: no, 0.55%
	voteChan <- GossipVote{peers[5], false}
	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled too soon")
	}

	// 0-3: yes, 4-8: no, 0.44%
	voteChan <- GossipVote{peers[4], false}
	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled too soon")
	}

	// 0-2: yes, 3-8: no, 0.33%
	voteChan <- GossipVote{peers[3], false}
	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was not disabled when it should be")
	}

	for i := 0; i < 5; i++ {
		voteChan <- GossipVote{peers[0], true}
		time.Sleep(time.Millisecond * 5)
		if testHandler.enabled {
			t.Errorf("Gossip was enabled from a double vote")
		}
	}

	// 0-2: yes, 3-4: no, 0.6%
	for i := 5; i <= 8; i++ {
		peerDisconnectedChan <- peers[i]
		time.Sleep(time.Millisecond * 5)
		if testHandler.enabled {
			t.Errorf("Gossip was enabled when it should not have been")
		}
	}

	peerDisconnectedChan <- peers[5]
	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was enabled from duplicate disconnect")
	}

	// 0-2: yes, 3: no, 0.75%
	peerDisconnectedChan <- peers[4]
	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was not enabled when it should have been")
	}

	// 2: yes, 3: no
	for i := 0; i < 2; i++ {
		peerDisconnectedChan <- peers[i]
		time.Sleep(time.Millisecond * 5)
		if !testHandler.enabled {
			t.Errorf("Gossip was disabled when it should not have been")
		}
	}

	// 3: no
	peerDisconnectedChan <- peers[2]
	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was not disabled when it should have been")
	}

	// no votes, should not change
	peerDisconnectedChan <- peers[3]
	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was enabled when it should not have been")
	}

	voteChan <- GossipVote{peers[0], true}
	peerDisconnectedChan <- peers[0]
	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was disabled when it should not have been")
	}
}

func TestAlwaysEnabledGossipToggle(t *testing.T) {
	ctx := context.Background()
	testHandler := TestGossipEnableHandler{false}
	voteChan := make(chan GossipVote)
	peerDisconnectedChan := make(chan peer.ID)
	opts := options.NewGossipToggleOptions()
	opts.AlwaysDisable = false
	opts.AlwaysEnable = true

	gossipToggle := NewGossipToggle(&testHandler, voteChan, peerDisconnectedChan, *opts)
	gossipToggle.Start(ctx)
	time.Sleep(time.Millisecond * 5)

	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled on startup")
	}

	peers := []peer.ID{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	for _, p := range peers {
		voteChan <- GossipVote{p, false}
	}

	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled from votes")
	}

	for _, p := range peers {
		peerDisconnectedChan <- p
	}

	time.Sleep(time.Millisecond * 5)
	if !testHandler.enabled {
		t.Errorf("Gossip was incorrectly disabled from votes")
	}
}

func TestAlwaysDisabledGossipToggle(t *testing.T) {
	ctx := context.Background()
	testHandler := TestGossipEnableHandler{false}
	voteChan := make(chan GossipVote)
	peerDisconnectedChan := make(chan peer.ID)
	opts := options.NewGossipToggleOptions()
	opts.AlwaysDisable = true
	opts.AlwaysEnable = false

	gossipToggle := NewGossipToggle(&testHandler, voteChan, peerDisconnectedChan, *opts)
	gossipToggle.Start(ctx)
	time.Sleep(time.Millisecond * 5)

	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled on startup")
	}

	peers := []peer.ID{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	for _, p := range peers {
		voteChan <- GossipVote{p, true}
	}

	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled from votes")
	}

	for _, p := range peers {
		peerDisconnectedChan <- p
	}

	time.Sleep(time.Millisecond * 5)
	if testHandler.enabled {
		t.Errorf("Gossip was incorrectly enabled from votes")
	}
}
