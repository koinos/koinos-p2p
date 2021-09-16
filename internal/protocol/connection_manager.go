package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	util "github.com/koinos/koinos-util-golang"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

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

type PeerError struct {
	id  peer.ID
	err error
}

type peerConnectionContext struct {
	peer   *PeerConnection
	cancel context.CancelFunc
}

// ConnectionManager attempts to reconnect to peers using the network.Notifiee interface.
type ConnectionManager struct {
	host   host.Host
	server *gorpc.Server
	client *gorpc.Client

	gossip   *KoinosGossip
	localRPC rpc.LocalRPC

	initialPeers   map[peer.ID]peer.AddrInfo
	connectedPeers map[peer.ID]*peerConnectionContext

	lastIrreversibleBlock koinos.BlockTopology

	peerConnectedChan    chan connectionMessage
	peerDisconnectedChan chan connectionMessage
	forkHeadsChan        chan *broadcast.ForkHeads
	peerErrorChan        chan PeerError
}

// NewConnectionManager creates a new PeerReconnectManager object
func NewConnectionManager(host host.Host, gossip *KoinosGossip, localRPC rpc.LocalRPC, initialPeers []string, peerErrorChan chan PeerError, lib *koinos.BlockTopology) *ConnectionManager {
	connectionManager := ConnectionManager{
		host:                  host,
		client:                gorpc.NewClient(host, rpc.PeerRPCID),
		server:                gorpc.NewServer(host, rpc.PeerRPCID),
		gossip:                gossip,
		localRPC:              localRPC,
		initialPeers:          make(map[peer.ID]peer.AddrInfo),
		connectedPeers:        make(map[peer.ID]*peerConnectionContext),
		lastIrreversibleBlock: *lib,
		peerConnectedChan:     make(chan connectionMessage),
		peerDisconnectedChan:  make(chan connectionMessage),
		forkHeadsChan:         make(chan *broadcast.ForkHeads),
		peerErrorChan:         peerErrorChan,
	}

	log.Debug("Registering Peer RPC Service")
	err := connectionManager.server.Register(rpc.NewPeerRPCService(connectionManager.localRPC))
	if err != nil {
		log.Errorf("Error registering Peer RPC Service: %s", err.Error())
		panic(err)
	}
	log.Debug("Peer RPC Servce successfully registered")

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

	return &connectionManager
}

// OpenedStream is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) OpenedStream(n network.Network, s network.Stream) {
}

// ClosedStream is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) ClosedStream(n network.Network, s network.Stream) {
}

// Connected is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) Connected(net network.Network, conn network.Conn) {
	c.peerConnectedChan <- connectionMessage{net: net, conn: conn}
}

// Disconnected is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) Disconnected(net network.Network, conn network.Conn) {
	c.peerDisconnectedChan <- connectionMessage{net: net, conn: conn}
}

// Listen is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) Listen(n network.Network, _ multiaddr.Multiaddr) {
}

// ListenClose is part of the libp2p network.Notifiee interface
func (c *ConnectionManager) ListenClose(n network.Network, _ multiaddr.Multiaddr) {
}

// HandleForkHeads updates peers with fork head information
func (c *ConnectionManager) HandleForkHeads(fh *broadcast.ForkHeads) {
	c.forkHeadsChan <- fh
}

func (c *ConnectionManager) handleConnected(ctx context.Context, msg connectionMessage) {
	pid := msg.conn.RemotePeer()
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), pid)

	log.Infof("Connected to peer: %s", s)

	if _, ok := c.connectedPeers[pid]; !ok {
		childCtx, cancel := context.WithCancel(ctx)
		peerConn := &peerConnectionContext{
			peer: NewPeerConnection(
				pid,
				&c.lastIrreversibleBlock,
				c.localRPC,
				rpc.NewPeerRPC(c.client, pid),
				c.peerErrorChan,
			),
			cancel: cancel,
		}

		peerConn.peer.Start(childCtx)
		c.connectedPeers[pid] = peerConn
	}

	c.gossip.Peer.PublishMessage(ctx, []byte(s))
}

func (c *ConnectionManager) handleDisconnected(ctx context.Context, msg connectionMessage) {
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), msg.conn.RemotePeer())
	log.Infof("Disconnected from peer: %s", s)
	pid := msg.conn.RemotePeer()

	if peerConn, ok := c.connectedPeers[pid]; ok {
		peerConn.cancel()
		delete(c.connectedPeers, pid)
	}

	if addr, ok := c.initialPeers[pid]; ok {
		go func() {
			sleepTimeSeconds := 1
			for {
				log.Infof("Attempting to connect to peer %v", addr.ID)
				if err := c.connectToPeer(addr); err == nil {
					return
				}

				time.Sleep(time.Duration(sleepTimeSeconds) * time.Second)
				sleepTimeSeconds = min(maxSleepBackoff, sleepTimeSeconds*2)
			}
		}()
	}
}

func (c *ConnectionManager) handlePeerError(ctx context.Context, peerErr PeerError) {
	if errors.Is(peerErr.err, ErrGossip) {
		return
	}

	log.Infof("Encountered peer error: %s, %s", peerErr.id, peerErr.err.Error())

	// TODO: When we implenent naughty points, here might be a good place to switch on different errors (#5)
	// If peer quits with an error, blacklist it for a while so we don't spam reconnection attempts
	c.host.Network().ClosePeer(peerErr.id)
}

func (c *ConnectionManager) handleForkHeads(ctx context.Context, forkHeads *broadcast.ForkHeads) {
	c.lastIrreversibleBlock = *forkHeads.LastIrreversibleBlock
	for _, peerConn := range c.connectedPeers {
		peerConn.peer.UpdateLastIrreversibleBlock(&c.lastIrreversibleBlock)
	}
}

func (c *ConnectionManager) connectInitialPeers() {
	newlyConnectedPeers := make(map[peer.ID]util.Void)
	peersToConnect := make(map[peer.ID]peer.AddrInfo)
	sleepTimeSeconds := 1

	for k, v := range c.initialPeers {
		peersToConnect[k] = v
	}

	for len(peersToConnect) > 0 {
		for peer, addr := range c.initialPeers {
			log.Infof("Attempting to connect to peer %v", peer)
			err := c.connectToPeer(addr)
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

func (c *ConnectionManager) connectToPeer(addr peer.AddrInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.host.Connect(ctx, addr)
}

func (c *ConnectionManager) managerLoop(ctx context.Context) {
	for {
		select {
		case connMsg := <-c.peerConnectedChan:
			c.handleConnected(ctx, connMsg)
		case connMsg := <-c.peerDisconnectedChan:
			c.handleDisconnected(ctx, connMsg)
		case peerErr := <-c.peerErrorChan:
			c.handlePeerError(ctx, peerErr)
		case forkHeads := <-c.forkHeadsChan:
			c.handleForkHeads(ctx, forkHeads)

		case <-ctx.Done():
			return
		}
	}
}

// Start the connection manager
func (c *ConnectionManager) Start(ctx context.Context) {
	go func() {
		for _, peer := range c.host.Network().Peers() {
			conns := c.host.Network().ConnsToPeer(peer)
			if len(conns) > 0 {
				c.peerConnectedChan <- connectionMessage{net: c.host.Network(), conn: conns[0]}
			}
		}

		go c.connectInitialPeers()
		go c.managerLoop(ctx)
	}()
}
