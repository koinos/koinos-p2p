package options

// DownloadManagerOptions is parameters that control the DownloadManager
type DownloadManagerOptions struct {
	MaxDownloadsInFlight int
	MaxDownloadDepth     int
}

// NewDownloadManagerOptions creates a DownloadManagerOptions with default field values
func NewDownloadManagerOptions() *DownloadManagerOptions {
	options := DownloadManagerOptions{
		MaxDownloadsInFlight: 30,
		MaxDownloadDepth:     10,
	}
	return &options
}
