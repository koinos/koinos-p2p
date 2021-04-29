package options

// DownloadManagerOptions is parameters that control the DownloadManager
type DownloadManagerOptions struct {
	MaxDownloadsInFlight int
	MaxDownloadDepth     int
	GossipDisableBp      int
	GossipEnableBp       int
	GossipAlwaysDisable  bool
	GossipAlwaysEnable   bool
}

// NewDownloadManagerOptions creates a DownloadManagerOptions with default field values
func NewDownloadManagerOptions() *DownloadManagerOptions {
	options := DownloadManagerOptions{
		MaxDownloadsInFlight: 30,
		MaxDownloadDepth:     10,
		GossipDisableBp:      3000,
		GossipEnableBp:       6000,
		GossipAlwaysDisable:  false,
		GossipAlwaysEnable:   false,
	}
	return &options
}
