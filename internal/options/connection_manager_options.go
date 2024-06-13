package options

import "time"

const (
	protocolVersionRetryTimeDefault = time.Millisecond * 50
	protocolVersionTimeoutDefault   = time.Second * 5
)

// ConnectionManagerOptions are options for ConnectionManager
type ConnectionManagerOptions struct {
	ProtocolVersionRetryTime time.Duration
	ProtocolVersionTimeout   time.Duration
}

// NewConnectionManagerOptions returns default initialized ConnectionManagerOptions
func NewConnectionManagerOptions() *ConnectionManagerOptions {
	return &ConnectionManagerOptions{
		ProtocolVersionRetryTime: protocolVersionRetryTimeDefault,
		ProtocolVersionTimeout:   protocolVersionTimeoutDefault,
	}
}
