package options

type SyncServiceOptions struct {
	EnableDebugMessages bool
}

func NewSyncServiceOptions() *SyncServiceOptions {
	options := SyncServiceOptions{
		EnableDebugMessages: false,
	}
	return &options
}
