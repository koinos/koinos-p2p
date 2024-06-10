package p2p

import (
	"context"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

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

type peerAddressMessage struct {
	id         peer.ID
	returnChan chan<- multiaddr.Multiaddr
}

type numConnectionsMessage struct {
	returnChan chan<- int
}

type isConnectedMessage struct {
	id         peer.ID
	returnChan chan<- bool
}

type peerConnectionContext struct {
	peer   *PeerConnection
	conn   network.Conn
	cancel context.CancelFunc
}

// ConnectionManager attempts to reconnect to peers using the network.Notifiee interface.
type ConnectionManager struct {
	host   host.Host
	server *gorpc.Server
	client *gorpc.Client

	localRPC    rpc.LocalRPC
	peerOpts    *options.PeerConnectionOptions
	libProvider LastIrreversibleBlockProvider
	applicator  *Applicator

	initialPeers   map[peer.ID]peer.AddrInfo
	connectedPeers map[peer.ID]*peerConnectionContext

	peerConnectedChan    chan connectionMessage
	peerDisconnectedChan chan connectionMessage
	peerErrorChan        chan<- PeerError
	peerAddressChan      chan *peerAddressMessage
	numConnectionsChan   chan *numConnectionsMessage
	isConnectedChan      chan *isConnectedMessage
}

// NewConnectionManager creates a new PeerReconnectManager object
func NewConnectionManager(
	host host.Host,
	localRPC rpc.LocalRPC,
	peerOpts *options.PeerConnectionOptions,
	libProvider LastIrreversibleBlockProvider,
	initialPeers []peer.AddrInfo,
	peerErrorChan chan<- PeerError,
	applicator *Applicator) *ConnectionManager {

	connectionManager := ConnectionManager{
		host:                 host,
		client:               gorpc.NewClient(host, rpc.PeerRPCID),
		server:               gorpc.NewServer(host, rpc.PeerRPCID),
		localRPC:             localRPC,
		peerOpts:             peerOpts,
		libProvider:          libProvider,
		applicator:           applicator,
		initialPeers:         make(map[peer.ID]peer.AddrInfo),
		connectedPeers:       make(map[peer.ID]*peerConnectionContext),
		peerConnectedChan:    make(chan connectionMessage),
		peerDisconnectedChan: make(chan connectionMessage),
		peerErrorChan:        peerErrorChan,
		peerAddressChan:      make(chan *peerAddressMessage),
		numConnectionsChan:   make(chan *numConnectionsMessage),
		isConnectedChan:      make(chan *isConnectedMessage),
	}

	log.Debug("Registering Peer RPC Service")
	err := connectionManager.server.Register(rpc.NewPeerRPCService(connectionManager.localRPC))
	if err != nil {
		log.Errorf("Error registering Peer RPC Service: %s", err.Error())
		panic(err)
	}
	log.Debug("Peer RPC Service successfully registered")

	for _, peer := range initialPeers {
		connectionManager.initialPeers[peer.ID] = peer
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

func (c *ConnectionManager) GetPeerAddress(ctx context.Context, id peer.ID) multiaddr.Multiaddr {
	returnChan := make(chan multiaddr.Multiaddr)

	c.peerAddressChan <- &peerAddressMessage{id, returnChan}

	select {
	case addr := <-returnChan:
		return addr
	case <-ctx.Done():
		return nil
	}
}

func (c *ConnectionManager) GetNumConnections(ctx context.Context) int {
	returnChan := make(chan int)

	c.numConnectionsChan <- &numConnectionsMessage{returnChan}

	select {
	case num := <-returnChan:
		return num
	case <-ctx.Done():
		return 0
	}
}

func (c *ConnectionManager) IsConnected(ctx context.Context, pid peer.ID) bool {
	returnChan := make(chan bool)

	c.isConnectedChan <- &isConnectedMessage{pid, returnChan}

	select {
	case connected := <-returnChan:
		return connected
	case <-ctx.Done():
		return false
	}
}

func (c *ConnectionManager) handleConnected(ctx context.Context, msg connectionMessage) {
	pid := msg.conn.RemotePeer()
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), pid)

	log.Debugf("Connected to peer: %s", s)

	if _, ok := c.connectedPeers[pid]; !ok {
		childCtx, cancel := context.WithCancel(ctx)
		peerConn := &peerConnectionContext{
			peer: NewPeerConnection(
				pid,
				c.libProvider,
				c.localRPC,
				rpc.NewPeerRPC(c.client, pid),
				c.peerErrorChan,
				c.peerOpts,
				c.applicator,
			),
			conn:   msg.conn,
			cancel: cancel,
		}

		peerConn.peer.Start(childCtx)
		c.connectedPeers[pid] = peerConn
	}
}

func (c *ConnectionManager) handleDisconnected(ctx context.Context, msg connectionMessage) {
	pid := msg.conn.RemotePeer()

	if peerConn, ok := c.connectedPeers[pid]; ok {
		peerConn.cancel()
		delete(c.connectedPeers, pid)
	} else {
		return
	}

	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), msg.conn.RemotePeer())
	log.Debugf("Disconnected from peer: %s", s)
}

func (c *ConnectionManager) handleGetPeerAddress(ctx context.Context, msg *peerAddressMessage) {
	var addr multiaddr.Multiaddr
	if peer, ok := c.connectedPeers[msg.id]; ok {
		addr = peer.conn.RemoteMultiaddr()
	}

	select {
	case msg.returnChan <- addr:
	case <-ctx.Done():
	}
}

func (c *ConnectionManager) handleGetNumConnections(ctx context.Context, msg *numConnectionsMessage) {
	select {
	case msg.returnChan <- len(c.connectedPeers):
	case <-ctx.Done():
	}
}

func (c *ConnectionManager) handleIsConnected(ctx context.Context, msg *isConnectedMessage) {
	_, connected := c.connectedPeers[msg.id]

	select {
	case msg.returnChan <- connected:
	case <-ctx.Done():
	}
}

func (c *ConnectionManager) connectInitialPeers(ctx context.Context) {
	peersToConnect := make(map[peer.ID]peer.AddrInfo)
	sleepTimeSeconds := 10

	for k, v := range c.initialPeers {
		peersToConnect[k] = v
	}

	for {
		for peer, addr := range c.initialPeers {
			if !c.IsConnected(ctx, peer) {
				log.Infof("Attempting to connect to seed %v", peer)
				if err := c.host.Connect(ctx, addr); err != nil {
					log.Infof("Error connecting to seed %v: %s", peer, err)
				}
			}
		}

		select {
		case <-time.After(time.Duration(sleepTimeSeconds) * time.Second):
		case <-ctx.Done():
		}
	}
}

func (c *ConnectionManager) managerLoop(ctx context.Context) {
	for {
		select {
		case connMsg := <-c.peerConnectedChan:
			c.handleConnected(ctx, connMsg)
		case connMsg := <-c.peerDisconnectedChan:
			c.handleDisconnected(ctx, connMsg)
		case peerAddrMsg := <-c.peerAddressChan:
			c.handleGetPeerAddress(ctx, peerAddrMsg)
		case numConnectionsMsg := <-c.numConnectionsChan:
			c.handleGetNumConnections(ctx, numConnectionsMsg)
		case isConnectedMsg := <-c.isConnectedChan:
			c.handleIsConnected(ctx, isConnectedMsg)

		case <-ctx.Done():
			for _, conn := range c.connectedPeers {
				conn.cancel()
			}

			c.connectedPeers = make(map[peer.ID]*peerConnectionContext)
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

		go c.connectInitialPeers(ctx)
		go c.managerLoop(ctx)
	}()
}
