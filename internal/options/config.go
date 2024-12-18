package options

// Config is the entire configuration file
type Config struct {
	NodeOptions              NodeOptions
	PeerConnectionOptions    PeerConnectionOptions
	PeerErrorHandlerOptions  PeerErrorHandlerOptions
	GossipToggleOptions      GossipToggleOptions
	ApplicatorOptions        ApplicatorOptions
	ConnectionManagerOptions ConnectionManagerOptions
	GossipOptions            GossipOptions
}

// NewConfig creates a new Config
func NewConfig() *Config {
	config := Config{
		NodeOptions:              *NewNodeOptions(),
		PeerConnectionOptions:    *NewPeerConnectionOptions(),
		PeerErrorHandlerOptions:  *NewPeerErrorHandlerOptions(),
		GossipToggleOptions:      *NewGossipToggleOptions(),
		ApplicatorOptions:        *NewApplicatorOptions(),
		ConnectionManagerOptions: *NewConnectionManagerOptions(),
		GossipOptions:            *NewGossipOptions(),
	}
	return &config
}
