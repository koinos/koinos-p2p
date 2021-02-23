package node

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"time"

	"github.com/koinos/koinos-p2p/internal/inventory"
	"github.com/koinos/koinos-p2p/internal/protocol"
	"github.com/koinos/koinos-p2p/internal/rpc"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type nodeProtocols struct {
	//Sync   protocol.SyncProtocol
	Gossip protocol.GossipProtocol
}

// create new node protocol object
func newNodeProtocols(node *KoinosP2PNode) *nodeProtocols {
	np := new(nodeProtocols)

	data := protocol.Data{Inventory: &node.Inventory, RPC: node.RPC, Host: node.Host}

	//np.Sync = *protocol.NewSyncProtocol(&data)
	//node.registerProtocol(np.Sync)

	np.Gossip = *protocol.NewGossipProtocol(&data)
	node.registerProtocol(&np.Gossip)

	return np
}

// KoinosP2PNode is the core object representing
type KoinosP2PNode struct {
	Host        host.Host
	Inventory   inventory.Inventory
	Protocols   nodeProtocols
	RPC         rpc.RPC
	Gossip      *KoinosGossip
	SyncServer  *gorpc.Server
	SyncManager *protocol.SyncManager
}

// NewKoinosP2PNode creates a libp2p node object listening on the given multiaddress
// uses secio encryption on the wire
// listenAddr is a multiaddress string on which to listen
// seed is the random seed to use for key generation. Use a negative number for a random seed.
func NewKoinosP2PNode(ctx context.Context, listenAddr string, rpc rpc.RPC, seed int64) (*KoinosP2PNode, error) {
	var r io.Reader
	if seed == 0 {
		r = crand.Reader
	} else {
		r = mrand.New(mrand.NewSource(seed))
	}

	privateKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		return nil, err
	}

	options := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Identity(privateKey),
	}

	host, err := libp2p.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	node := new(KoinosP2PNode)
	node.Host = host
	node.RPC = rpc
	node.Protocols = *newNodeProtocols(node)
	node.Inventory = *inventory.NewInventory(time.Minute * time.Duration(30))
	node.SyncServer = gorpc.NewServer(host, protocol.SyncID)
	err = node.SyncServer.Register(&protocol.SyncService{})
	if err != nil {
		return nil, err
	}

	node.SyncManager = protocol.NewSyncManager(node.Host, node.RPC)
	node.SyncManager.Start()

	node.Gossip, err = NewKoinosGossip(ctx, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (n *KoinosP2PNode) registerProtocol(p protocol.Protocol) {
	pid, handler := p.GetProtocolRegistration()
	n.Host.SetStreamHandler(pid, handler)
}

// ConnectToPeer connects the node to the given peer
func (n *KoinosP2PNode) ConnectToPeer(peerAddr string) (*peer.AddrInfo, error) {

	addr, err := multiaddr.NewMultiaddr(peerAddr)
	if err != nil {
		return nil, err
	}
	peer, err := peerstore.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, *peer); err != nil {
		return nil, err
	}

	err = n.SyncManager.AddPeer(peer.ID)
	if err != nil {
		return nil, err
	}

	return peer, nil
}

// MakeContext creates and returns the canonical context which should be used for peer connections
// TODO: create this from configuration
func (n *KoinosP2PNode) MakeContext() (ctx context.Context, cancel context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
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
