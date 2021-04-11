package protocol

import (
	"context"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	ma "github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

// SyncManagerPeerAdder adds the existing peers and any new peers using the network.Notifiee interface.
type SyncManagerPeerAdder struct {
	// Golang docs advise to prefer passing context in a function parameter rather than a struct field.
	// However we can't follow this advice, since our function parameters are constrained to what is
	// specified in the network.Notifiee interface, which doesn't include a context parameter.
	ctx context.Context

	host        host.Host
	syncManager *SyncManager
}

// NewSyncManagerPeerAddr creates a new SyncManagerPeerAdder object
func NewSyncManagerPeerAddr(ctx context.Context, host host.Host, syncManager *SyncManager) SyncManagerPeerAdder {
	peerAdder := SyncManagerPeerAdder{ctx, host, syncManager}
	peerAdder.addCurrentPeers()
	return peerAdder
}

// OpenedStream is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) OpenedStream(n network.Network, s network.Stream) {
}

// ClosedStream is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) ClosedStream(n network.Network, s network.Stream) {
}

// Connected is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) Connected(n network.Network, c network.Conn) {
	zap.S().Info("Connected to peer %s", c.RemotePeer())
	peerAdder.syncManager.AddPeer(peerAdder.ctx, c.RemotePeer())
}

// Disconnected is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) Disconnected(n network.Network, c network.Conn) {
	zap.S().Info("Disconnected from peer %s", c.RemotePeer())
}

// Listen is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) Listen(n network.Network, _ ma.Multiaddr) {
}

// ListenClose is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) ListenClose(n network.Network, _ ma.Multiaddr) {
}

// Listen is part of the libp2p network.Notifiee interface
func (peerAdder *SyncManagerPeerAdder) addCurrentPeers() {
	for _, peer := range peerAdder.host.Network().Peers() {
		peerAdder.syncManager.AddPeer(peerAdder.ctx, peer)
	}
}
