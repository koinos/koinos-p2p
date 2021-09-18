package options

const (
	enableThresholdDefault  = 2.0 / 3.0
	disableThresholdDefault = 1.0 / 3.0
	alwaysEnableDefault     = false
	alwaysDisableDefault    = false
)

// GossipToggleOptions are options for GossipToggle
type GossipToggleOptions struct {
	EnableThreshold  float64
	DisableThreshold float64
	AlwaysEnable     bool
	AlwaysDisable    bool
}

// NewGossipToggleOptions returns default initialized GossipToggleOptions
func NewGossipToggleOptions() *GossipToggleOptions {
	return &GossipToggleOptions{
		EnableThreshold:  enableThresholdDefault,
		DisableThreshold: disableThresholdDefault,
		AlwaysEnable:     alwaysEnableDefault,
		AlwaysDisable:    alwaysDisableDefault,
	}
}
