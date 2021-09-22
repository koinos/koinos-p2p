package options

// Config is the entire configuration file
type Config struct {
	NodeOptions             NodeOptions
	PeerErrorHandlerOptions PeerErrorHandlerOptions
	GossipToggleOptions     GossipToggleOptions
}

// NewConfig creates a new Config
func NewConfig() *Config {
	config := Config{
		NodeOptions:             *NewNodeOptions(),
		PeerErrorHandlerOptions: *NewPeerErrorHandlerOptions(),
		GossipToggleOptions:     *NewGossipToggleOptions(),
	}
	return &config
}
