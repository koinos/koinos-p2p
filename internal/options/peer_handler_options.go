package options

type PeerHandlerOptions struct {
	HeightRangePollTimeMs uint64
	DownloadTimeoutMs     uint64
	RpcTimeoutMs          uint64
	EnableDebugMessages   bool
}

func NewPeerHandlerOptions() *PeerHandlerOptions {
	options := PeerHandlerOptions{
		HeightRangePollTimeMs: 2000,
		DownloadTimeoutMs:     50000,
		RpcTimeoutMs:          10000,
		EnableDebugMessages:   false,
	}
	return &options
}
