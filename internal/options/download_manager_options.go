package options

type DownloadManagerOptions struct {
	MaxDownloadsInFlight int
	MaxDownloadDepth     int
	EnableDebugMessages  bool
}

func NewDownloadManagerOptions() *DownloadManagerOptions {
	options := DownloadManagerOptions{
		MaxDownloadsInFlight: 30,
		MaxDownloadDepth:     10,
		EnableDebugMessages:  false,
	}
	return &options
}
