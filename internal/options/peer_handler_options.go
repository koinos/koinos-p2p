package options

// PeerHandlerOptions is parameters that control PeerHandler objects
type PeerHandlerOptions struct {
	HeightRangePollTimeMs uint64
	DownloadTimeoutMs     uint64
	RPCTimeoutMs          uint64
	EnableDebugMessages   bool
}

// NewPeerHandlerOptions creates a PeerHandlerOptions with default field values
func NewPeerHandlerOptions() *PeerHandlerOptions {
	options := PeerHandlerOptions{
		HeightRangePollTimeMs: 2000,
		DownloadTimeoutMs:     50000,
		RPCTimeoutMs:          10000,
		EnableDebugMessages:   false,
	}
	return &options
}
