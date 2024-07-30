package options

import "github.com/libp2p/go-libp2p/core/peer"

// NodeOptions is options that affect the whole node
type NodeOptions struct {
	// Peers to initially connect
	InitialPeers []peer.AddrInfo

	// Force gossip mode on startup
	ForceGossip bool

	// DHT local peer discovery
	DHTLocalDiscovery bool
}

// NewNodeOptions creates a NodeOptions object which controls how p2p works
func NewNodeOptions() *NodeOptions {
	return &NodeOptions{
		InitialPeers:      make([]peer.AddrInfo, 0),
		ForceGossip:       false,
		DHTLocalDiscovery: false,
	}
}
