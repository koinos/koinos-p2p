package options

const (
	alwaysEnableDefault  = false
	alwaysDisableDefault = false
)

// GossipToggleOptions are options for GossipToggle
type GossipToggleOptions struct {
	AlwaysEnable  bool
	AlwaysDisable bool
}

// NewGossipToggleOptions returns default initialized GossipToggleOptions
func NewGossipToggleOptions() *GossipToggleOptions {
	return &GossipToggleOptions{
		AlwaysEnable:  alwaysEnableDefault,
		AlwaysDisable: alwaysDisableDefault,
	}
}
