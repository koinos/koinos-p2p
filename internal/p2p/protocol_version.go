package p2p

import "github.com/Masterminds/semver/v3"

const (
	koinosProtocolPrefix = "koinos/p2p/"
	versionMajor         = 1
	versionMinor         = 0
	versionPatch         = 0
	versionPrerelease    = ""
	versionMetadata      = ""
)

var (
	koinosProtocolVersion *semver.Version
)

func KoinosProtocolVersion() *semver.Version {
	if koinosProtocolVersion == nil {
		koinosProtocolVersion = semver.New(versionMajor, versionMinor, versionPatch, versionPrerelease, versionMetadata)
	}

	return koinosProtocolVersion
}

func KoinosProtocolVersionString() string {
	return koinosProtocolPrefix + KoinosProtocolVersion().String()
}
