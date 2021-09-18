package options

// Config is the entire configuration file
type Config struct {
	NodeOptions             NodeOptions
	PeerConnectionOptions   PeerConnectionOptions
	PeerErrorHandlerOptions PeerErrorHandlerOptions
	GossipToggleOptions     GossipToggleOptions
}

// NewConfig creates a new Config
func NewConfig() *Config {
	config := Config{
		NodeOptions:             *NewNodeOptions(),
		PeerConnectionOptions:   *NewPeerConnectionOptions(),
		PeerErrorHandlerOptions: *NewPeerErrorHandlerOptions(),
		GossipToggleOptions:     *NewGossipToggleOptions(),
	}
	return &config
}
