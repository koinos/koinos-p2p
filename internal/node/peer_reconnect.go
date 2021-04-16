package node

import (
	"context"
	"time"

	log "github.com/koinos/koinos-log-golang"
	util "github.com/koinos/koinos-util-golang"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
)

const maxSleepBackoff = 30

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// PeerConnectionManager attempts to reconnect to peers using the network.Notifiee interface.
type PeerConnectionManager struct {
	node         *KoinosP2PNode
	initialPeers map[peer.ID]peer.AddrInfo
}

// NewPeerConnectionManager creates a new PeerReconnectManager object
func NewPeerConnectionManager(n *KoinosP2PNode, initialPeers []string) *PeerConnectionManager {
	reconnectManager := PeerConnectionManager{node: n, initialPeers: make(map[peer.ID]peer.AddrInfo)}
	for _, peerStr := range initialPeers {
		ma, err := multiaddr.NewMultiaddr(peerStr)
		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		addr, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		reconnectManager.initialPeers[addr.ID] = *addr
	}
	return &reconnectManager
}

// OpenedStream is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) OpenedStream(n network.Network, s network.Stream) {
}

// ClosedStream is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) ClosedStream(n network.Network, s network.Stream) {
}

// Connected is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) Connected(n network.Network, c network.Conn) {
}

// Disconnected is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) Disconnected(n network.Network, c network.Conn) {
	if addr, ok := p.initialPeers[c.RemotePeer()]; ok {
		go func() {
			sleepTimeSeconds := 1
			for {
				log.Infof("Attempting to connect to peer %v", addr.ID)
				if err := p.connectToPeer(addr); err == nil {
					return
				}

				time.Sleep(time.Duration(sleepTimeSeconds) * time.Second)
				sleepTimeSeconds = min(maxSleepBackoff, sleepTimeSeconds*2)
			}
		}()
	}
}

// Listen is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) Listen(n network.Network, _ ma.Multiaddr) {
}

// ListenClose is part of the libp2p network.Notifiee interface
func (p *PeerConnectionManager) ListenClose(n network.Network, _ ma.Multiaddr) {
}

// ConnectInitialPeers connects to the initial peers with exponential backoff,
// blocking until all peers have been connected to
func (p *PeerConnectionManager) ConnectInitialPeers() {
	newlyConnectedPeers := make(map[peer.ID]util.Void)
	peersToConnect := make(map[peer.ID]peer.AddrInfo)
	sleepTimeSeconds := 1

	for k, v := range p.initialPeers {
		peersToConnect[k] = v
	}

	for len(peersToConnect) > 0 {
		for peer, addr := range p.initialPeers {
			log.Infof("Attempting to connect to peer %v", peer)
			if err := p.connectToPeer(addr); err == nil {
				newlyConnectedPeers[peer] = util.Void{}
			}
		}

		for peer := range newlyConnectedPeers {
			delete(peersToConnect, peer)
		}

		newlyConnectedPeers = make(map[peer.ID]util.Void)

		time.Sleep(time.Duration(sleepTimeSeconds) * time.Second)
		sleepTimeSeconds = min(maxSleepBackoff, sleepTimeSeconds*2)
	}
}

func (p *PeerConnectionManager) connectToPeer(addr peer.AddrInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.node.Host.Connect(ctx, addr)
}
