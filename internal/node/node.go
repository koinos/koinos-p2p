package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"time"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/protocol"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	util "github.com/koinos/koinos-util-golang"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	multiaddr "github.com/multiformats/go-multiaddr"

	"google.golang.org/protobuf/proto"
)

// KoinosP2PNode is the core object representing
type KoinosP2PNode struct {
	Host              host.Host
	localRPC          rpc.LocalRPC
	Gossip            *protocol.KoinosGossip
	ConnectionManager *protocol.ConnectionManager
	PeerErrorChan     chan protocol.PeerError

	Options options.NodeOptions
}

// NewKoinosP2PNode creates a libp2p node object listening on the given multiaddress
// uses secio encryption on the wire
// listenAddr is a multiaddress string on which to listen
// seed is the random seed to use for key generation. Use 0 for a random seed.
func NewKoinosP2PNode(ctx context.Context, listenAddr string, localRPC rpc.LocalRPC, requestHandler *koinosmq.RequestHandler, seed string, config *options.Config) (*KoinosP2PNode, error) {
	privateKey, err := generatePrivateKey(seed)
	if err != nil {
		return nil, err
	}

	options := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Identity(privateKey),
		libp2p.NATPortMap(),
	}

	host, err := libp2p.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	node := new(KoinosP2PNode)
	node.Host = host
	node.localRPC = localRPC

	if requestHandler != nil {
		requestHandler.SetBroadcastHandler("koinos.block.accept", node.handleBlockBroadcast)
		requestHandler.SetBroadcastHandler("koinos.transaction.accept", node.handleTransactionBroadcast)
		requestHandler.SetBroadcastHandler("koinos.block.forks", node.handleForkUpdate)
	} else {
		log.Info("Starting P2P node without broadcast listeners")
	}

	node.Options = config.NodeOptions
	node.PeerErrorChan = make(chan protocol.PeerError)

	// Create the pubsub gossip
	if node.Options.EnableBootstrap {
		// TODO:  When https://github.com/libp2p/go-libp2p-pubsub/issues/364 is fixed, don't monkey-patch global variables like this
		log.Info("Bootstrap node enabled")
		pubsub.GossipSubD = 0
		pubsub.GossipSubDlo = 0
		pubsub.GossipSubDhi = 0
		pubsub.GossipSubDscore = 0
	} else {
		pubsub.GossipSubD = 6
		pubsub.GossipSubDlo = 5
		pubsub.GossipSubDhi = 12
		pubsub.GossipSubDscore = 4
	}

	if !node.Options.EnablePeerExchange {
		pubsub.GossipSubPrunePeers = 0
	} else {
		pubsub.GossipSubPrunePeers = 16
	}

	ps, err := pubsub.NewGossipSub(
		ctx, node.Host,
		pubsub.WithPeerExchange(node.Options.EnablePeerExchange),
		pubsub.WithMessageIdFn(generateMessageID),
	)
	if err != nil {
		return nil, err
	}
	node.Gossip = protocol.NewKoinosGossip(ctx, localRPC, ps, node.PeerErrorChan, node, node.Host.ID())
	node.ConnectionManager = protocol.NewConnectionManager(
		node.Host,
		node.Gossip,
		node.Options.InitialPeers,
		node.PeerErrorChan)

	return node, nil
}

func (n *KoinosP2PNode) handleBlockBroadcast(topic string, data []byte) {
	log.Debugf("Received koinos.block.accept broadcast: %v", string(data))
	blockBroadcast := &broadcast.BlockAccepted{}
	err := proto.Unmarshal(data, blockBroadcast)
	if err != nil {
		log.Warnf("Unable to parse koinos.block.accept broadcast: %v", string(data))
		return
	}
	binary, err := proto.Marshal(blockBroadcast.Block)
	if err != nil {
		log.Warnf("Unable to serialize block from broadcast: %v", err.Error())
		return
	}
	n.Gossip.Block.PublishMessage(context.Background(), binary)
	log.Infof("Publishing block - %s", util.BlockString(blockBroadcast.Block))
}

func (n *KoinosP2PNode) handleTransactionBroadcast(topic string, data []byte) {
	log.Debugf("Received koinos.transction.accept broadcast: %v", string(data))
	trxBroadcast := &broadcast.TransactionAccepted{}
	err := proto.Unmarshal(data, trxBroadcast)
	if err != nil {
		log.Warnf("Unable to parse koinos.transaction.accept broadcast: %v", string(data))
		return
	}
	binary, err := proto.Marshal(trxBroadcast.Transaction)
	if err != nil {
		log.Warnf("Unable to serialize transaction from broadcast: %v", err.Error())
		return
	}
	log.Infof("Publishing transaction - %s", util.TransactionString(trxBroadcast.Transaction))
	n.Gossip.Transaction.PublishMessage(context.Background(), binary)
}

func (n *KoinosP2PNode) handleForkUpdate(topic string, data []byte) {
	log.Debugf("Received koinos.block.forks broadcast: %v", string(data))
	forkHeads := &broadcast.ForkHeads{}
	err := proto.Unmarshal(data, forkHeads)
	if err != nil {
		log.Warnf("Unable to parse koinos.block.forks broadcast: %v", string(data))
		return
	}
	n.Gossip.HandleForkHeads(forkHeads)
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

// ConnectToPeerString connects the node to the given peer
func (n *KoinosP2PNode) ConnectToPeerString(peerAddr string) (*peer.AddrInfo, error) {
	peer, err := n.PeerStringToAddress(peerAddr)
	if err != nil {
		return nil, err
	}

	if err = n.ConnectToPeerAddress(peer); err != nil {
		return peer, err
	}

	return peer, nil
}

// ConnectToPeerAddress connects to the given peer address
func (n *KoinosP2PNode) ConnectToPeerAddress(peer *peer.AddrInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, *peer); err != nil {
		return err
	}

	return nil
}

// GetConnections returns the host's current peer connections
func (n *KoinosP2PNode) GetConnections() []network.Conn {
	return n.Host.Network().Conns()
}

// GetListenAddress returns the multiaddress on which the node is listening
func (n *KoinosP2PNode) GetListenAddress() multiaddr.Multiaddr {
	return n.Host.Addrs()[0]
}

// GetPeerAddress returns the ipfs multiaddress to which other peers should connect
func (n *KoinosP2PNode) GetPeerAddress() multiaddr.Multiaddr {
	hostAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ipfs/%s", n.Host.ID().Pretty()))
	return n.GetListenAddress().Encapsulate(hostAddr)
}

// Close closes the node
func (n *KoinosP2PNode) Close() error {
	if err := n.Host.Close(); err != nil {
		return err
	}

	return nil
}

// Start starts background goroutines
func (n *KoinosP2PNode) Start(ctx context.Context) {
	n.Host.Network().Notify(n.ConnectionManager)

	// Start gossip if forced
	if n.Options.ForceGossip {
		n.Gossip.StartGossip(ctx)
	}

	// Start peer gossip
	n.Gossip.StartPeerGossip(ctx)
	n.ConnectionManager.Start(ctx)
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
	case protocol.BlockTopicName, protocol.TransactionTopicName, protocol.PeerTopicName:
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
