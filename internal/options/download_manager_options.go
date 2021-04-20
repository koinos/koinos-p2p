package options

// DownloadManagerOptions is parameters that control the DownloadManager
type DownloadManagerOptions struct {
	MaxDownloadsInFlight int
	MaxDownloadDepth     int
	GossipDisableBp      int
	GossipEnableBp       int
}

// NewDownloadManagerOptions creates a DownloadManagerOptions with default field values
func NewDownloadManagerOptions() *DownloadManagerOptions {
	options := DownloadManagerOptions{
		MaxDownloadsInFlight: 30,
		MaxDownloadDepth:     10,
		GossipDisableBp:      3000,
		GossipEnableBp:       6000,
	}
	return &options
}
