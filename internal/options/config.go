package options

// Config is the entire configuration file
type Config struct {
	NodeOptions             NodeOptions
	PeerErrorHandlerOptions PeerErrorHandlerOptions
}

// NewConfig creates a new Config
func NewConfig() *Config {
	config := Config{
		NodeOptions:             *NewNodeOptions(),
		PeerErrorHandlerOptions: *NewPeerErrorHandlerOptions(),
	}
	return &config
}
