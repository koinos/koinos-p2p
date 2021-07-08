package options

// SyncManagerOptions is parameters that control the SyncManager
type SyncManagerOptions struct {
	RPCTimeoutMs uint64
	BlacklistMs  uint64
	Checkpoints  []string
}

// NewSyncManagerOptions creates a SyncManagerOptions with default field values
func NewSyncManagerOptions() *SyncManagerOptions {
	options := SyncManagerOptions{
		RPCTimeoutMs: 30000,
		BlacklistMs:  60000,
		Checkpoints:  make([]string, 0),
	}
	return &options
}
