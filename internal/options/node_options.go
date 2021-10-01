package options

// NodeOptions is options that affect the whole node
type NodeOptions struct {
	// Set to true to enable peer exchange, where peers are given to / accepted from other nodes
	EnablePeerExchange bool

	// Peers to initially connect
	InitialPeers []string

	// Peers to directly connect
	DirectPeers []string

	// Force gossip mode on startup
	ForceGossip bool
}

// NewNodeOptions creates a NodeOptions object which controls how p2p works
func NewNodeOptions() *NodeOptions {
	return &NodeOptions{
		EnablePeerExchange: true,
		InitialPeers:       make([]string, 0),
		DirectPeers:        make([]string, 0),
		ForceGossip:        false,
	}
}
