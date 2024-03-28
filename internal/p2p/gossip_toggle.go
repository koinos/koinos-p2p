package p2p

import (
	"context"
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-p2p/internal/options"
)

// NumConnectionsProvider returns the current number of peer connections
type NumConnectionsProvider interface {
	GetNumConnections(ctx context.Context) int
}

// GossipToggle tracks our head block time and toggles gossip accordingly
type GossipToggle struct {
	gossipEnabler    GossipEnableHandler
	enabled          bool
	enabledMutex     sync.Mutex
	headTime         uint64
	headMutex        sync.Mutex
	numConnsProvider NumConnectionsProvider

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

			if g.numConnsProvider.GetNumConnections(ctx) == 0 {
				if g.enabled {
					g.gossipEnabler.EnableGossip(ctx, false)
					g.enabledMutex.Lock()
					g.enabled = false
					g.enabledMutex.Unlock()
				}
			} else {
				g.headMutex.Lock()
				t := time.Unix(0, int64(g.headTime)*int64(1000000) /* Conversion to nanoseconds */)
				g.headMutex.Unlock()

				// We disable gossip when we are 30 seconds behind the current time
				if time.Since(t) <= 45*time.Second {
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
		}
	}()
}

// NewGossipToggle creates a GossipToggle
func NewGossipToggle(gossipEnabler GossipEnableHandler, numConnsProvider NumConnectionsProvider, opts options.GossipToggleOptions) *GossipToggle {
	return &GossipToggle{
		gossipEnabler:    gossipEnabler,
		enabled:          false,
		opts:             opts,
		headTime:         0,
		numConnsProvider: numConnsProvider,
	}
}
