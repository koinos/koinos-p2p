package options

// NodeOptions is options that affect the whole node
type NodeOptions struct {
	// Set to true to enable peer exchange, where peers are given to / accepted from other nodes
	EnablePeerExchange bool

	// Set to true to enable bootstrap mode, where incoming connections are referred to other nodes
	EnableBootstrap bool

	// Set to true to enable gossip mode at all times
	EnableGossip bool

	// Set to true to enable gossip mode at all times
	ForceGossip bool

	// Set to true to enable verbose logging
	EnableDebugMessages bool

	// Peers to initially connect
	InitialPeers []string

	// Peers to directly connect
	DirectPeers []string
}

// NewNodeOptions creates a NodeOptions object which controls how p2p works
func NewNodeOptions() *NodeOptions {
	return &NodeOptions{
		EnablePeerExchange:  true,
		EnableBootstrap:     false,
		EnableGossip:        true,
		ForceGossip:         false,
		EnableDebugMessages: false,
		InitialPeers:        make([]string, 0),
		DirectPeers:         make([]string, 0),
	}
}
