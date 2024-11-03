package options

import "time"

const (
	alwaysEnableDefault       = false
	alwaysDisableDefault      = false
	headDelayThresholdDefault = 38 * time.Second
)

// GossipToggleOptions are options for GossipToggle
type GossipToggleOptions struct {
	AlwaysEnable       bool
	AlwaysDisable      bool
	HeadDelayThreshold time.Duration
}

// NewGossipToggleOptions returns default initialized GossipToggleOptions
func NewGossipToggleOptions() *GossipToggleOptions {
	return &GossipToggleOptions{
		AlwaysEnable:       alwaysEnableDefault,
		AlwaysDisable:      alwaysDisableDefault,
		HeadDelayThreshold: headDelayThresholdDefault,
	}
}
