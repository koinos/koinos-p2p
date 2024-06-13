package p2p

import (
	"context"

	"github.com/Masterminds/semver/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ProtocolVersionProvider is an interface for a peer's protocol version to the PeerConnection
type ProtocolVersionProvider interface {
	GetProtocolVersion(ctx context.Context, pid peer.ID) (*semver.Version, error)
}
