package options

type BlacklistOptions struct {
	// How long blacklisting lasts
	BlacklistMs uint64

	// How often blacklist is rescanned for expired entries to be removed
	BlacklistRescanMs uint64
}

func NewBlacklistOptions() *BlacklistOptions {
	options := BlacklistOptions{
		BlacklistMs:       60000,
		BlacklistRescanMs: 10000,
	}
	return &options
}
