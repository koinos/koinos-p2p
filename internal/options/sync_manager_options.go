package options

type SyncManagerOptions struct {
	RpcTimeoutMs        uint64
	BlacklistMs         uint64
	EnableDebugMessages bool
}

func NewSyncManagerOptions() *SyncManagerOptions {
	options := SyncManagerOptions{
		RpcTimeoutMs:        30000,
		BlacklistMs:         60000,
		EnableDebugMessages: false,
	}
	return &options
}
