package options

type BdmiProviderOptions struct {
	EnableDebugMessages  bool
	PollMyTopologyMs     uint64
	HeightRangeTimeoutMs uint64

	HeightInterestReach uint64
	RescanIntervalMs    uint64

	PeerHasBlockQueueSize int
}

func NewBdmiProviderOptions() *BdmiProviderOptions {
	options := BdmiProviderOptions{
		EnableDebugMessages:  false,
		PollMyTopologyMs:     2000,
		HeightRangeTimeoutMs: 10000,

		HeightInterestReach:   100,
		RescanIntervalMs:      20,
		PeerHasBlockQueueSize: 300,
	}
	return &options
}
