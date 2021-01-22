package p2p

import (
	"context"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

const syncID = "/koinos/sync/1.0.0"

// SyncProtocol handles broadcasting inventory to peers
type SyncProtocol struct {
	Node *KoinosP2PNode
}

// NewSyncProtocol constructs a new broadcast protocol object
func NewSyncProtocol(host *KoinosP2PNode) *SyncProtocol {
	p := &SyncProtocol{Node: host}
	host.Host.SetStreamHandler(broadcastID, p.handleStream)
	return p
}

func (c SyncProtocol) handleStream(s network.Stream) {
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (c SyncProtocol) InitiateProtocol(ctx context.Context, p peer.ID) {
	// Start a stream with the given peer
	s, err := c.Node.Host.NewStream(ctx, p, broadcastID)
	if err != nil {
		s.Reset()
		return
	}

	if ctx.Err() == nil {
		s.Reset()
	}
}
