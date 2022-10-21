package p2p

import (
	"context"
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
)

// GossipToggle tracks our head block time and toggles gossip accordingly
type GossipToggle struct {
	gossipEnabler GossipEnableHandler
	enabled       bool
	enabledMutex  sync.Mutex
	headTime      uint64
	headMutex     sync.Mutex

	opts options.GossipToggleOptions
}

// IsEnabled returns whether gossip is enabled
func (g *GossipToggle) IsEnabled() bool {
	g.enabledMutex.Lock()
	defer g.enabledMutex.Unlock()
	return g.enabled
}

// UpdateHeadTime updates the head block time
func (g *GossipToggle) UpdateHeadTime(blockTime uint64) {
	g.headMutex.Lock()
	defer g.headMutex.Unlock()
	g.headTime = blockTime
}

// Start begins checking if we are in gossip range
func (g *GossipToggle) Start(ctx context.Context) {
	go func() {
		if g.opts.AlwaysEnable {
			log.Infof("Gossip always enabled")
			g.gossipEnabler.EnableGossip(ctx, true)
			g.enabledMutex.Lock()
			g.enabled = true
			g.enabledMutex.Unlock()
			return
		} else if g.opts.AlwaysDisable {
			log.Infof("Gossip always disabled")
			g.enabledMutex.Lock()
			g.enabled = false
			g.enabledMutex.Unlock()
			return
		}

		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()

		// Check if head block is within wall clock time
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			g.headMutex.Lock()
			t := time.Unix(0, int64(g.headTime)*int64(1000000) /* Conversion to nanoseconds */)
			g.headMutex.Unlock()

			// We disable gossip when we are 1 minute behind the current time
			if time.Since(t) <= 30*time.Second {
				if !g.enabled {
					g.gossipEnabler.EnableGossip(ctx, true)
					g.enabledMutex.Lock()
					g.enabled = true
					g.enabledMutex.Unlock()
				}
			} else {
				if g.enabled {
					g.gossipEnabler.EnableGossip(ctx, false)
					g.enabledMutex.Lock()
					g.enabled = false
					g.enabledMutex.Unlock()
				}
			}
		}
	}()
}

// NewGossipToggle creates a GossipToggle
func NewGossipToggle(gossipEnabler GossipEnableHandler, opts options.GossipToggleOptions) *GossipToggle {
	return &GossipToggle{
		gossipEnabler: gossipEnabler,
		enabled:       false,
		opts:          opts,
		headTime:      0,
	}
}
