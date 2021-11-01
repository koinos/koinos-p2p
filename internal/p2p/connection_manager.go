package p2p

import (
	"context"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
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

type peerConnectionContext struct {
	peer   *PeerConnection
	cancel context.CancelFunc
}

type libValue struct {
	num uint64
	id  []byte
}

type connectionRequest struct {
	addr       *peer.AddrInfo
	returnChan chan<- error
}

type connectionStatus struct {
	addr       *peer.AddrInfo
	err        error
	returnChan chan<- error
}

// ConnectionManager attempts to reconnect to peers using the network.Notifiee interface.
type ConnectionManager struct {
	host   host.Host
	server *gorpc.Server
	client *gorpc.Client

	gossip       *KoinosGossip
	errorHandler *PeerErrorHandler
	localRPC     rpc.LocalRPC
	peerOpts     *options.PeerConnectionOptions
	libProvider  LastIrreversibleBlockProvider

	initialPeers       map[peer.ID]peer.AddrInfo
	connectedPeers     map[peer.ID]*peerConnectionContext
	pendingConnections map[peer.ID]util.Void

	peerConnectedChan        chan connectionMessage
	peerDisconnectedChan     chan connectionMessage
	peerErrorChan            chan<- PeerError
	gossipVoteChan           chan<- GossipVote
	signalPeerDisconnectChan chan<- peer.ID
	connectToPeerChan        chan connectionRequest
	connectionStatusChan     chan connectionStatus
}

// NewConnectionManager creates a new PeerReconnectManager object
func NewConnectionManager(
	host host.Host,
	gossip *KoinosGossip,
	errorHandler *PeerErrorHandler,
	localRPC rpc.LocalRPC,
	peerOpts *options.PeerConnectionOptions,
	libProvider LastIrreversibleBlockProvider,
	initialPeers []string,
	peerErrorChan chan<- PeerError,
	gossipVoteChan chan<- GossipVote,
	signalPeerDisconnectChan chan<- peer.ID) *ConnectionManager {

	connectionManager := ConnectionManager{
		host:                     host,
		client:                   gorpc.NewClient(host, rpc.PeerRPCID),
		server:                   gorpc.NewServer(host, rpc.PeerRPCID),
		gossip:                   gossip,
		errorHandler:             errorHandler,
		localRPC:                 localRPC,
		peerOpts:                 peerOpts,
		libProvider:              libProvider,
		initialPeers:             make(map[peer.ID]peer.AddrInfo),
		connectedPeers:           make(map[peer.ID]*peerConnectionContext),
		pendingConnections:       make(map[peer.ID]util.Void),
		peerConnectedChan:        make(chan connectionMessage),
		peerDisconnectedChan:     make(chan connectionMessage),
		peerErrorChan:            peerErrorChan,
		gossipVoteChan:           gossipVoteChan,
		signalPeerDisconnectChan: signalPeerDisconnectChan,
		connectToPeerChan:        make(chan connectionRequest),
		connectionStatusChan:     make(chan connectionStatus),
	}

	log.Debug("Registering Peer RPC Service")
	err := connectionManager.server.Register(rpc.NewPeerRPCService(connectionManager.localRPC))
	if err != nil {
		log.Errorf("Error registering Peer RPC Service: %s", err.Error())
		panic(err)
	}
	log.Debug("Peer RPC Service successfully registered")

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

func (c *ConnectionManager) handleConnected(ctx context.Context, msg connectionMessage) {
	pid := msg.conn.RemotePeer()
	s := fmt.Sprintf("%s/p2p/%s", msg.conn.RemoteMultiaddr(), pid)

	log.Infof("Connected to peer: %s", s)

	if _, ok := c.connectedPeers[pid]; !ok {
		childCtx, cancel := context.WithCancel(ctx)
		peerConn := &peerConnectionContext{
			peer: NewPeerConnection(
				pid,
				c.libProvider,
				c.localRPC,
				rpc.NewPeerRPC(c.client, pid),
				c.peerErrorChan,
				c.gossipVoteChan,
				c.peerOpts,
			),
			cancel: cancel,
		}

		peerConn.peer.Start(childCtx)
		c.connectedPeers[pid] = peerConn
	}

	c.gossip.Peer.PublishMessage(ctx, []byte(s))
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
	log.Infof("Disconnected from peer: %s", s)

	if addr, ok := c.initialPeers[pid]; ok {
		go func() {
			sleepTimeSeconds := 1
			for {
				log.Infof("Attempting to connect to peer %v", addr.ID)
				if err := c.ConnectToPeer(ctx, &addr); err == nil {
					return
				}

				time.Sleep(time.Duration(sleepTimeSeconds) * time.Second)
				sleepTimeSeconds = min(maxSleepBackoff, sleepTimeSeconds*2)
			}
		}()
	}

	go func() {
		select {
		case c.signalPeerDisconnectChan <- pid:
		case <-ctx.Done():
		}
	}()
}

func (c *ConnectionManager) connectInitialPeers(ctx context.Context) {
	newlyConnectedPeers := make(map[peer.ID]util.Void)
	peersToConnect := make(map[peer.ID]peer.AddrInfo)
	sleepTimeSeconds := 1

	for k, v := range c.initialPeers {
		peersToConnect[k] = v
	}

	for len(peersToConnect) > 0 {
		for peer, addr := range c.initialPeers {
			log.Infof("Attempting to connect to peer %v", peer)
			err := c.ConnectToPeer(ctx, &addr)
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

// ConnectToPeer attempts to connect to the peer at the given address
func (c *ConnectionManager) ConnectToPeer(ctx context.Context, addr *peer.AddrInfo) error {
	returnChan := make(chan error, 1)
	c.connectToPeerChan <- connectionRequest{
		addr:       addr,
		returnChan: returnChan,
	}

	select {
	case err := <-returnChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ConnectionManager) handleConnectToPeer(ctx context.Context, req connectionRequest) {
	if !c.errorHandler.CanConnect(ctx, req.addr.ID) {
		req.returnChan <- fmt.Errorf("cannot connect to peer %s, error score too high", req.addr.ID)
	}

	if _, ok := c.connectedPeers[req.addr.ID]; ok {
		go func() {
			req.returnChan <- nil
		}()
		return
	}

	if _, ok := c.pendingConnections[req.addr.ID]; ok {
		go func() {
			req.returnChan <- fmt.Errorf("already attempting connection to peer %s", req.addr.ID)
		}()
		return
	}

	c.pendingConnections[req.addr.ID] = util.Void{}

	go func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err := c.host.Connect(ctx, *req.addr)
		c.connectionStatusChan <- connectionStatus{
			addr:       req.addr,
			err:        err,
			returnChan: req.returnChan,
		}
	}()
}

func (c *ConnectionManager) handleConnectionStatus(ctx context.Context, status connectionStatus) {
	delete(c.pendingConnections, status.addr.ID)
	status.returnChan <- status.err
}

func (c *ConnectionManager) managerLoop(ctx context.Context) {
	for {
		select {
		case connMsg := <-c.peerConnectedChan:
			c.handleConnected(ctx, connMsg)
		case connMsg := <-c.peerDisconnectedChan:
			c.handleDisconnected(ctx, connMsg)
		case req := <-c.connectToPeerChan:
			c.handleConnectToPeer(ctx, req)
		case status := <-c.connectionStatusChan:
			c.handleConnectionStatus(ctx, status)

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
