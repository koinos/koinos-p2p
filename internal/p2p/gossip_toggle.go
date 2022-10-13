package p2p

import (
	"context"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/libp2p/go-libp2p-core/peer"
)

// https://stackoverflow.com/questions/22185636/easiest-way-to-get-the-machine-epsilon-in-go
const epsilon = float64(7.)/3 - float64(4.)/3 - float64(1.)

// GossipVote is a vote from a peer to enable gossip or not
type GossipVote struct {
	peer   peer.ID
	synced bool
}

// GossipToggle tracks peer gossip votes and toggles gossip accordingly
type GossipToggle struct {
	gossipEnabler        GossipEnableHandler
	enabled              bool
	peerVotes            map[peer.ID]bool
	yesCount             int
	voteChan             <-chan GossipVote
	peerDisconnectedChan <-chan peer.ID

	opts options.GossipToggleOptions
}

// IsEnabled returns whether gossip is enabled
func (g *GossipToggle) IsEnabled() bool {
	return g.enabled
}

func (g *GossipToggle) checkThresholds(ctx context.Context) {
	if len(g.peerVotes) == 0 {
		if g.enabled && !g.opts.AlwaysEnable {
			g.enabled = false
			g.gossipEnabler.EnableGossip(ctx, false)
		}
		return
	}

	threshold := float64(g.yesCount) / float64(len(g.peerVotes))

	if threshold-g.opts.EnableThreshold >= -epsilon && !g.enabled {
		g.enabled = true
		g.gossipEnabler.EnableGossip(ctx, true)
	} else if g.opts.DisableThreshold-threshold >= -epsilon && g.enabled {
		g.enabled = false
		g.gossipEnabler.EnableGossip(ctx, false)
	}
}

func (g *GossipToggle) handleVote(ctx context.Context, vote GossipVote) {
	if g.opts.AlwaysEnable || g.opts.AlwaysDisable {
		return
	}

	if synced, ok := g.peerVotes[vote.peer]; ok {
		switch {
		case !synced && vote.synced:
			g.yesCount++
		case synced && !vote.synced:
			g.yesCount--
		}
	} else {
		if vote.synced {
			g.yesCount++
		}
	}

	g.peerVotes[vote.peer] = vote.synced
	g.checkThresholds(ctx)
}

func (g *GossipToggle) handlepeerDisconnected(ctx context.Context, peer peer.ID) {
	if g.opts.AlwaysEnable || g.opts.AlwaysDisable {
		return
	}

	if vote, ok := g.peerVotes[peer]; ok {
		if vote {
			g.yesCount--
		}

		delete(g.peerVotes, peer)

		g.checkThresholds(ctx)
	}
}

// Start begins gossip vote processing
func (g *GossipToggle) Start(ctx context.Context) {
	go func() {
		if g.opts.AlwaysEnable {
			log.Infof("Gossip always enabled")
			g.gossipEnabler.EnableGossip(ctx, true)
		} else if g.opts.AlwaysDisable {
			log.Infof("Gossip always disabled")
		}

		for {
			select {
			case vote := <-g.voteChan:
				g.handleVote(ctx, vote)
			case peer := <-g.peerDisconnectedChan:
				g.handlepeerDisconnected(ctx, peer)

			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewGossipToggle creates a GossipToggle
func NewGossipToggle(gossipEnabler GossipEnableHandler, voteChan <-chan GossipVote, peerDisconnectedChan <-chan peer.ID, opts options.GossipToggleOptions) *GossipToggle {
	return &GossipToggle{
		gossipEnabler:        gossipEnabler,
		enabled:              false,
		peerVotes:            make(map[peer.ID]bool),
		yesCount:             0,
		voteChan:             voteChan,
		peerDisconnectedChan: peerDisconnectedChan,
		opts:                 opts,
	}
}
