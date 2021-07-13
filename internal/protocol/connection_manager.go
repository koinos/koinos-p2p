package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

const maxSleepBackoff = 30

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type connectionMessage struct {
	net  network.Network
	conn network.Conn
}

// ConnectionManager attempts to reconnect to peers using the network.Notifiee interface.
type ConnectionManager struct {
	host        host.Host
	syncManager *SyncManager
	gossip      *KoinosGossip

	Blacklist    *Blacklist
	initialPeers map[peer.ID]peer.AddrInfo
	rescanTicker *time.Ticker

	peerConnectedChan    chan connectionMessage
	peerDisconnectedChan chan connectionMessage
	peerErrorChan        <-chan PeerError
	rescanBlacklist      <-chan time.Time
}

// NewConnectionManager creates a new PeerReconnectManager object
func NewConnectionManager(host host.Host, syncManager *SyncManager, gossip *KoinosGossip, blacklistOptions *options.BlacklistOptions, initialPeers []string, peerErrorChan <-chan PeerError) *ConnectionManager {
	connectionManager := ConnectionManager{
		host:                 host,
		syncManager:          syncManager,
		gossip:               gossip,
		Blacklist:            NewBlacklist(*blacklistOptions),
		initialPeers:         make(map[peer.ID]peer.AddrInfo),
		rescanTicker:         time.NewTicker(time.Duration(blacklistOptions.BlacklistRescanMs) * time.Millisecond),
		peerConnectedChan:    make(chan connectionMessage),
		peerDisconnectedChan: make(chan connectionMessage),
		peerErrorChan:        peerErrorChan,
	}

	for _, peerStr := range initialPeers {
		ma, err := multiaddr.NewMultiaddr(peerStr)
		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		addr, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		connectionManager.initialPeers[addr.ID] = *addr
	}

	connectionManager.rescanBlacklist = connectionManager.rescanTicker.C

	return &connectionManager
}

// OpenedStream is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) OpenedStream(n network.Network, s network.Stream) {
}

// ClosedStream is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) ClosedStream(n network.Network, s network.Stream) {
}

// Connected is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) Connected(n network.Network, c network.Conn) {
	p.peerConnectedChan <- connectionMessage{net: n, conn: c}
}

// Disconnected is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) Disconnected(n network.Network, c network.Conn) {
	p.peerDisconnectedChan <- connectionMessage{net: n, conn: c}
}

// Listen is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) Listen(n network.Network, _ multiaddr.Multiaddr) {
}

// ListenClose is part of the libp2p network.Notifiee interface
func (p *ConnectionManager) ListenClose(n network.Network, _ multiaddr.Multiaddr) {
}

func (p *ConnectionManager) handleConnected(ctx context.Context, msg connectionMessage) {
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), msg.conn.RemotePeer())

	if p.Blacklist.IsPeerBlacklisted(msg.conn.RemotePeer()) {
		p.host.Network().ClosePeer(msg.conn.RemotePeer())
		log.Infof("Rejecting connection from blacklisted peer: %s", s)
	}

	log.Infof("Connected to peer: %s", s)

	p.syncManager.AddPeer(ctx, msg.conn.RemotePeer())

	vb := types.VariableBlob((s))
	p.gossip.Peer.PublishMessage(ctx, &vb)
}

func (p *ConnectionManager) handleDisconnected(ctx context.Context, msg connectionMessage) {
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), msg.conn.RemotePeer())
	log.Infof("Disconnected from peer: %s", s)
	p.syncManager.RemovePeer(ctx, msg.conn.RemotePeer())

	if addr, ok := p.initialPeers[msg.conn.RemotePeer()]; ok {
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

func (p *ConnectionManager) handlePeerError(ctx context.Context, peerErr PeerError) {
	if errors.Is(peerErr.Error, ErrGossip) {
		return
	}

	// TODO: When we implenent naughty points, here might be a good place to switch on different errors (#5)
	// If peer quits with an error, blacklist it for a while so we don't spam reconnection attempts
	p.Blacklist.AddPeerToBlacklist(peerErr)
	p.host.Network().ClosePeer(peerErr.PeerID)
}

func (p *ConnectionManager) connectInitialPeers() {
	newlyConnectedPeers := make(map[peer.ID]util.Void)
	peersToConnect := make(map[peer.ID]peer.AddrInfo)
	sleepTimeSeconds := 1

	for k, v := range p.initialPeers {
		peersToConnect[k] = v
	}

	for len(peersToConnect) > 0 {
		for peer, addr := range p.initialPeers {
			log.Infof("Attempting to connect to peer %v", peer)
			err := p.connectToPeer(addr)
			if err != nil {
				log.Infof("Error connecting to peer %v: %s", peer, err)
			} else {
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

func (p *ConnectionManager) connectToPeer(addr peer.AddrInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.host.Connect(ctx, addr)
}

func (p *ConnectionManager) managerLoop(ctx context.Context) {
	defer p.rescanTicker.Stop()

	for {
		select {
		case connMsg := <-p.peerConnectedChan:
			p.handleConnected(ctx, connMsg)
		case connMsg := <-p.peerDisconnectedChan:
			p.handleDisconnected(ctx, connMsg)
		case peerErr := <-p.peerErrorChan:
			p.handlePeerError(ctx, peerErr)
		case <-p.rescanBlacklist:
			p.Blacklist.RemoveExpiredBlacklistEntries()

		case <-ctx.Done():
			return
		}
	}
}

// Start the connection manager
func (p *ConnectionManager) Start(ctx context.Context) {
	go func() {
		for _, peer := range p.host.Network().Peers() {
			p.syncManager.AddPeer(ctx, peer)
		}

		go p.connectInitialPeers()
		go p.managerLoop(ctx)
	}()
}
