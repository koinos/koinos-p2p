package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2p"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	prpc "github.com/koinos/koinos-proto-golang/koinos/rpc"
	rpcp2p "github.com/koinos/koinos-proto-golang/koinos/rpc/p2p"
	util "github.com/koinos/koinos-util-golang"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	multiaddr "github.com/multiformats/go-multiaddr"

	"google.golang.org/protobuf/proto"
)

// KoinosP2PNode is the core object representing
type KoinosP2PNode struct {
	Host              host.Host
	localRPC          rpc.LocalRPC
	Applicator        *p2p.Applicator
	Gossip            *p2p.KoinosGossip
	ConnectionManager *p2p.ConnectionManager
	PeerErrorHandler  *p2p.PeerErrorHandler
	GossipToggle      *p2p.GossipToggle
	TransactionCache  *p2p.TransactionCache
	libValue          atomic.Value

	PeerErrorChan        chan p2p.PeerError
	DisconnectPeerChan   chan peer.ID
	PeerDisconnectedChan chan peer.ID

	Options options.NodeOptions
}

const (
	transactionCacheDuration = 10 * time.Minute
	pubsubTimeCacheDuration  = time.Minute
	gossipHeartbeatInterval  = 500 * time.Millisecond
	gossipIWantFollowupTime  = time.Second
)

// NewKoinosP2PNode creates a libp2p node object listening on the given multiaddress
// uses secio encryption on the wire
// listenAddr is a multiaddress string on which to listen
// seed is the random seed to use for key generation. Use 0 for a random seed.
func NewKoinosP2PNode(ctx context.Context, listenAddr string, localRPC rpc.LocalRPC, requestHandler *koinosmq.RequestHandler, seed string, config *options.Config) (*KoinosP2PNode, error) {
	privateKey, err := generatePrivateKey(seed)
	if err != nil {
		return nil, err
	}

	node := new(KoinosP2PNode)

	node.Options = config.NodeOptions
	node.PeerErrorChan = make(chan p2p.PeerError)
	node.DisconnectPeerChan = make(chan peer.ID)
	node.PeerDisconnectedChan = make(chan peer.ID)

	node.PeerErrorHandler = p2p.NewPeerErrorHandler(
		node.DisconnectPeerChan,
		node.PeerErrorChan,
		config.PeerErrorHandlerOptions)

	var idht *dht.IpfsDHT

	options := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Identity(privateKey),
		// Attempt to open ports using uPNP for NATed hosts.
		libp2p.NATPortMap(),
		// Let this host use the DHT to find other hosts
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			idht, err = dht.New(ctx, h)
			return idht, err
		}),
		// Let this host use relays and advertise itself on relays if
		// it finds it is behind NAT. Use libp2p.Relay(options...) to
		// enable active relays and more.
		libp2p.EnableAutoRelay(),
		// If you want to help other peers to figure out if they are behind
		// NATs, you can launch the server-side of AutoNAT too (AutoRelay
		// already runs the client)
		//
		// This service is highly rate-limited and should not cause any
		// performance issues.
		libp2p.EnableNATService(),
		libp2p.ConnectionGater(node.PeerErrorHandler),
	}

	host, err := libp2p.New(options...)
	if err != nil {
		return nil, err
	}

	node.Host = host
	node.localRPC = localRPC

	if requestHandler != nil {
		requestHandler.SetBroadcastHandler("koinos.block.accept", node.handleBlockBroadcast)
		requestHandler.SetBroadcastHandler("koinos.mempool.accept", node.handleTransactionBroadcast)
		requestHandler.SetBroadcastHandler("koinos.block.forks", node.handleForkUpdate)
		requestHandler.SetRPCHandler("p2p", node.handleRPC)
	} else {
		log.Info("Starting P2P node without broadcast listeners")
	}

	pubsub.TimeCacheDuration = pubsubTimeCacheDuration
	gossipOpts := pubsub.DefaultGossipSubParams()
	gossipOpts.HeartbeatInterval = gossipHeartbeatInterval
	gossipOpts.IWantFollowupTime = gossipIWantFollowupTime
	ps, err := pubsub.NewGossipSub(
		ctx, node.Host,
		pubsub.WithMessageIdFn(generateMessageID),
		pubsub.WithPeerExchange(true),
		pubsub.WithPeerScore(
			&pubsub.PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 {
					rawScore := float64(node.PeerErrorHandler.GetPeerErrorScore(ctx, p))
					return -rawScore + float64(node.PeerErrorHandler.GetOptions().ErrorScoreReconnectThreshold)
				},
				AppSpecificWeight: 1,
				DecayInterval:     1 * time.Minute,
				DecayToZero:       0.01,
			},
			&pubsub.PeerScoreThresholds{
				GossipThreshold:             -1,
				PublishThreshold:            -1,
				GraylistThreshold:           -1,
				AcceptPXThreshold:           5000,
				OpportunisticGraftThreshold: .1,
			},
		),
	)
	if err != nil {
		return nil, err
	}

	node.TransactionCache = p2p.NewTransactionCache(transactionCacheDuration)

	node.Applicator, err = p2p.NewApplicator(
		ctx,
		node.localRPC,
		config.ApplicatorOptions,
	)

	if err != nil {
		return nil, err
	}

	node.Gossip = p2p.NewKoinosGossip(
		ctx,
		node.localRPC,
		ps,
		node.PeerErrorChan,
		node.Host.ID(),
		node,
		node.TransactionCache,
		node.Applicator)

	node.GossipToggle = p2p.NewGossipToggle(
		node.Gossip,
		config.GossipToggleOptions)

	node.ConnectionManager = p2p.NewConnectionManager(
		node.Host,
		node.localRPC,
		&config.PeerConnectionOptions,
		node,
		node.Options.InitialPeers,
		node.PeerErrorChan,
		node.PeerDisconnectedChan,
		node.Applicator)

	node.PeerErrorHandler.SetPeerAddressProvider(node.ConnectionManager)

	return node, nil
}

func (n *KoinosP2PNode) handleBlockBroadcast(topic string, data []byte) {
	log.Debug("Received koinos.block.accept broadcast")
	blockBroadcast := &broadcast.BlockAccepted{}
	err := proto.Unmarshal(data, blockBroadcast)
	if err != nil {
		log.Warnf("Unable to parse koinos.block.accept broadcast: %v", err.Error())
		return
	}

	go func() {
		if blockBroadcast.Head {
			n.GossipToggle.UpdateHeadTime(blockBroadcast.Block.Header.Timestamp)
		}
	}()

	go func() {
		n.Applicator.HandleBlockBroadcast(blockBroadcast)
	}()

	// If gossip is enabled publish the block
	if n.GossipToggle.IsEnabled() {
		err = n.Gossip.PublishBlock(context.Background(), blockBroadcast.Block)
		if err != nil {
			log.Warnf("Unable to serialize block from broadcast: %v", err.Error())
			return
		}
	}
}

func (n *KoinosP2PNode) handleTransactionBroadcast(topic string, data []byte) {
	log.Debug("Received koinos.mempool.accept broadcast")
	trxBroadcast := &broadcast.MempoolAccepted{}
	err := proto.Unmarshal(data, trxBroadcast)
	if err != nil {
		log.Warnf("Unable to parse koinos.transaction.accept broadcast: %v", err.Error())
		return
	}

	// If gossip is enabled publish the transaction
	if n.GossipToggle.IsEnabled() {
		err = n.Gossip.PublishTransaction(context.Background(), trxBroadcast.Transaction)
		if err != nil {
			log.Warnf("Unable to serialize transaction from broadcast: %v", err.Error())
			return
		}
	}
}

func (n *KoinosP2PNode) handleForkUpdate(topic string, data []byte) {
	log.Debug("Received koinos.block.forks broadcast")
	forkHeads := &broadcast.ForkHeads{}
	err := proto.Unmarshal(data, forkHeads)
	if err != nil {
		log.Warnf("Unable to parse koinos.block.forks broadcast: %s", err.Error())
		return
	}

	go func() {
		n.Applicator.HandleForkHeads(forkHeads)
	}()

	n.libValue.Store(forkHeads.LastIrreversibleBlock)
}

func (n *KoinosP2PNode) handleRPC(rpcType string, data []byte) ([]byte, error) {
	req := &rpcp2p.P2PRequest{}
	resp := &rpcp2p.P2PResponse{}

	err := proto.Unmarshal(data, req)
	if err != nil {
		log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(data))
		eResp := prpc.ErrorResponse{Message: err.Error()}
		rErr := rpcp2p.P2PResponse_Error{Error: &eResp}
		resp.Response = &rErr
	} else {
		log.Debugf("Received RPC request: 0x%v", hex.EncodeToString(data))
		resp = n.handleRequest(req)
	}

	var outputBytes []byte
	outputBytes, err = proto.Marshal(resp)

	return outputBytes, err
}

func (n *KoinosP2PNode) handleRequest(req *rpcp2p.P2PRequest) *rpcp2p.P2PResponse {
	response := rpcp2p.P2PResponse{}
	var err error

	if req.Request != nil {
		switch req.Request.(type) {
		case *rpcp2p.P2PRequest_GetGossipStatus:
			result := rpcp2p.GetGossipStatusResponse{Enabled: n.GossipToggle.IsEnabled()}
			respVal := rpcp2p.P2PResponse_GetGossipStatus{GetGossipStatus: &result}
			response.Response = &respVal
		default:
			err = errors.New("unknown request")
		}
	} else {
		err = errors.New("expected request was nil")
	}

	if err != nil {
		result := prpc.ErrorResponse{Message: err.Error()}
		respVal := rpcp2p.P2PResponse_Error{Error: &result}
		response.Response = &respVal
	}

	return &response
}

// PeerStringToAddress Creates a peer.AddrInfo object based on the given connection string
func (n *KoinosP2PNode) PeerStringToAddress(peerAddr string) (*peer.AddrInfo, error) {
	addr, err := multiaddr.NewMultiaddr(peerAddr)
	if err != nil {
		return nil, err
	}
	peer, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return nil, err
	}

	return peer, nil
}

// ConnectToPeerAddress connects to the given peer address
func (n *KoinosP2PNode) ConnectToPeerAddress(ctx context.Context, peer *peer.AddrInfo) error {
	return n.Host.Connect(ctx, *peer)
}

// GetConnections returns the host's current peer connections
func (n *KoinosP2PNode) GetConnections() []network.Conn {
	return n.Host.Network().Conns()
}

// GetAddressInfo returns the node's address info
func (n *KoinosP2PNode) GetAddressInfo() *peer.AddrInfo {
	return &peer.AddrInfo{
		ID:    n.Host.ID(),
		Addrs: n.Host.Addrs(),
	}
}

// GetAddress returns the peer multiaddress
func (n *KoinosP2PNode) GetAddress() multiaddr.Multiaddr {
	addrs, _ := peer.AddrInfoToP2pAddrs(n.GetAddressInfo())
	return addrs[0]
}

// GetLastIrreversibleBlock returns last irreversible block height and block id of connected node
func (n *KoinosP2PNode) GetLastIrreversibleBlock() *koinos.BlockTopology {
	return n.libValue.Load().(*koinos.BlockTopology)
}

// Close closes the node
func (n *KoinosP2PNode) Close() error {
	n.Gossip.EnableGossip(context.Background(), false)

	if err := n.Host.Close(); err != nil {
		return err
	}

	return nil
}

func (n *KoinosP2PNode) logConnectionsLoop(ctx context.Context) {
	for {
		select {
		case <-time.After(time.Minute * 1):
			log.Info("My address:")
			log.Infof(" - %s", n.GetAddress())
			log.Info("Connected peers:")
			for i, conn := range n.GetConnections() {
				log.Infof(" - %s/p2p/%s", conn.RemoteMultiaddr(), conn.RemotePeer())
				if i > 10 {
					log.Infof("   and %v more...", len(n.GetConnections())-i)
					break
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Start starts background goroutines
func (n *KoinosP2PNode) Start(ctx context.Context) {
	n.Host.Network().Notify(n.ConnectionManager)

	forkHeads, err := n.localRPC.GetForkHeads(ctx)

	for err != nil {
		forkHeads, err = n.localRPC.GetForkHeads(ctx)
	}

	n.libValue.Store(forkHeads.LastIrreversibleBlock)

	// Start peer gossip
	go n.logConnectionsLoop(ctx)
	n.PeerErrorHandler.Start(ctx)
	n.GossipToggle.Start(ctx)
	n.ConnectionManager.Start(ctx)
	n.Applicator.Start(ctx)

	go func() {
		for {
			select {
			case id := <-n.DisconnectPeerChan:
				_ = n.Host.Network().ClosePeer(id)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ----------------------------------------------------------------------------
// Utility Functions
// ----------------------------------------------------------------------------

func seedStringToInt64(seed string) int64 {
	// Hash the seed string
	h := sha256.New()
	h.Write([]byte(seed))
	sum := h.Sum(nil)

	return int64(binary.BigEndian.Uint64(sum[:8]))
}

func generatePrivateKey(seed string) (crypto.PrivKey, error) {
	var r io.Reader

	// If blank seed, generate a new randomized seed
	if seed == "" {
		seed = util.GenerateBase58ID(8)
		log.Infof("Using random seed: %s", seed)
	}

	// Convert the seed to int64 and construct the random source
	iseed := seedStringToInt64(seed)
	r = rand.New(rand.NewSource(iseed))

	privateKey, _, err := crypto.GenerateECDSAKeyPair(r)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func generateMessageID(msg *pb.Message) string {
	// Use the default unique ID function for peer exchange
	switch *msg.Topic {
	case p2p.BlockTopicName, p2p.TransactionTopicName:
		// Hash the data
		h := sha256.New()
		h.Write(msg.Data)
		sum := h.Sum(nil)

		// Base-64 encode it for compactness
		return base64.RawStdEncoding.EncodeToString(sum)
	default:
		return pubsub.DefaultMsgIdFn(msg)
	}
}
