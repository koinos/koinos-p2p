package options

// SyncServiceOptions is parameters that control the SyncService
type SyncServiceOptions struct {
	EnableDebugMessages bool
}

// NewSyncServiceOptions creates a new SyncServiceOptions with default field values
func NewSyncServiceOptions() *SyncServiceOptions {
	options := SyncServiceOptions{
		EnableDebugMessages: false,
	}
	return &options
}
