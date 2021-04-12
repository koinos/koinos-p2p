package options

// BdmiProviderOptions is parameters that control the BdmiProvider
type BdmiProviderOptions struct {
	PollMyTopologyMs     uint64
	HeightRangeTimeoutMs uint64

	HeightInterestReach uint64
	RescanIntervalMs    uint64

	PeerHasBlockQueueSize int
}

// NewBdmiProviderOptions creates a BdmiProviderOptions with default field values
func NewBdmiProviderOptions() *BdmiProviderOptions {
	options := BdmiProviderOptions{
		PollMyTopologyMs:     2000,
		HeightRangeTimeoutMs: 10000,

		HeightInterestReach:   100,
		RescanIntervalMs:      20,
		PeerHasBlockQueueSize: 300,
	}
	return &options
}
