package protocol

import (
	"context"
	"fmt"
	"log"
	"time"

	"math/rand"

	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-p2p/internal/util"

	types "github.com/koinos/koinos-types-golang"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
)

// SyncID Identifies the koinos sync protocol
const SyncID = "/koinos/sync/1.0.0"

const (
	timeoutSeconds   = uint64(30)
	blacklistSeconds = uint64(60)
)

// BatchBlockRequest a batch block request
type BatchBlockRequest struct {
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
}

// PeerBlockResponse is a peer block response
type PeerBlockResponse struct {
	Topology  types.BlockTopology
	Responder peer.ID
	Block     *types.OpaqueBlock
}

// PeerBlockResponseError is a peer block response error
type PeerBlockResponseError struct {
	Topology  types.BlockTopology
	Responder peer.ID
	Error     error
}

// SyncManager syncs blocks using multiple peers.
//
// SyncManager is responsible for:
//
// - Creating BlockDownloadManager
// - Creating BdmiProvider
// - Informing BdmiProvider when new peers join (TODO: canceling peers that left)
// - Hooking together the above components
// - Starting (and TODO: canceling) all loops
//
// TODO: Move blacklist to separate struct
//
type SyncManager struct {
	rng *rand.Rand

	server *gorpc.Server
	client *gorpc.Client
	rpc    rpc.RPC

	downloadManager *BlockDownloadManager
	bdmiProvider    *BdmiProvider

	// Channel for new peer ID's we want to connect to
	newPeers chan peer.ID

	// Channel for peer to notify when handshake is done
	handshakeDonePeers chan peer.ID

	// Channel for peer to notify when error occurs
	errPeers chan PeerError

	// Channel for peer ID's coming off a blacklist
	unblacklistPeers chan peer.ID

	// Peer ID map.  Only updated in the internal SyncManager thread
	peers map[peer.ID]util.Void

	// Blacklisted peers.
	blacklist map[peer.ID]util.Void
}

// NewSyncManager factory
func NewSyncManager(ctx context.Context, h host.Host, rpc rpc.RPC) *SyncManager {

	// TODO pass rng as parameter
	// TODO initialize RNG from cryptographically secure source
	manager := SyncManager{
		rng: rand.New(rand.NewSource(99)),

		server: gorpc.NewServer(h, SyncID),
		client: gorpc.NewClient(h, SyncID),
		rpc:    rpc,

		newPeers:           make(chan peer.ID),
		handshakeDonePeers: make(chan peer.ID),
		errPeers:           make(chan PeerError),
		unblacklistPeers:   make(chan peer.ID),

		peers:     make(map[peer.ID]util.Void),
		blacklist: make(map[peer.ID]util.Void),
	}
	manager.bdmiProvider = NewBdmiProvider(manager.client, rpc)
	manager.downloadManager = NewBlockDownloadManager(manager.rng, manager.bdmiProvider)

	log.Printf("Registering SyncService\n")
	err := manager.server.Register(NewSyncService(&rpc))
	if err != nil {
		panic(err)
	}
	log.Printf("SyncService successfully registered\n")

	// TODO: What is context?
	peerAdder := NewSyncManagerPeerAddr(ctx, h, &manager)
	h.Network().Notify(&peerAdder)

	return &manager
}

// AddPeer adds a peer to the SyncManager.
// Will connect to the peer in the background.
func (m *SyncManager) AddPeer(ctx context.Context, pid peer.ID) {

	go func() {
		select {
		case m.newPeers <- pid:
		case <-ctx.Done():
		}
	}()

	return
}

func (m *SyncManager) doPeerHandshake(ctx context.Context, pid peer.ID) {

	err := func() error {
		log.Printf("connecting to peer for sync: %v", pid)

		peerChainID := types.NewGetChainIDResponse()
		{
			req := types.NewGetChainIDRequest()
			subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
			defer cancel()
			err := m.client.CallContext(subctx, pid, "SyncService", "GetChainID", req, peerChainID)
			if err != nil {
				log.Printf("%v: error getting peer chain id, %v", pid, err)
				return err
			}
		}

		chainID, err := m.rpc.GetChainID()
		if err != nil {
			log.Printf("%v: error getting chain id, %v", pid, err)
			return err
		}

		if !chainID.ChainID.Equals(&peerChainID.ChainID) {
			log.Printf("%v: peer's chain id does not match", pid)
			return fmt.Errorf("%v: peer's chain id does not match", pid)
		}

		select {
		case m.handshakeDonePeers <- pid:
		case <-ctx.Done():
		}

		log.Printf("%v: connected!", pid)
		return nil
	}()

	if err != nil {
		select {
		case m.errPeers <- PeerError{pid, err}:
		case <-ctx.Done():
		}
	}
}

func (m *SyncManager) doPeerEnableDownload(ctx context.Context, pid peer.ID) {
	// Handoff to BdmiProvider
	select {
	case m.bdmiProvider.newPeerChan <- pid:
	case <-ctx.Done():
	}
}

// Blacklist a peer.  Runs in the main thread.
func (m *SyncManager) blacklistPeer(ctx context.Context, pid peer.ID, blacklistTime time.Duration) {
	// Add to the blacklist now
	m.blacklist[pid] = util.Void{}

	// Un-blacklist it after some time has passed
	go func() {
		select {
		case <-time.After(blacklistTime):
		case <-ctx.Done():
			return
		}

		select {
		case m.unblacklistPeers <- pid:
		case <-ctx.Done():
		}
	}()
}

func (m *SyncManager) run(ctx context.Context) {
	for {
		select {
		case pid := <-m.newPeers:
			_, isBlacklisted := m.blacklist[pid]
			if !isBlacklisted {
				go m.doPeerHandshake(ctx, pid)
			}
		case pid := <-m.handshakeDonePeers:
			m.peers[pid] = util.Void{}
			// Now that our data structures are all set up, we're ready to send it off to the BdmiProvider
			go m.doPeerEnableDownload(ctx, pid)
		case perr := <-m.errPeers:
			// If peer quit with error, blacklist it for a while so we don't spam reconnection attempts
			m.blacklistPeer(ctx, perr.PeerID, time.Duration(blacklistSeconds)*time.Second)
			delete(m.peers, perr.PeerID)
		case pid := <-m.unblacklistPeers:
			delete(m.blacklist, pid)
		case <-ctx.Done():
			return
		}
	}
}

// Start syncing blocks from peers
func (m *SyncManager) Start(ctx context.Context) {
	m.downloadManager.Start(ctx)
	m.bdmiProvider.Start(ctx)
	go m.run(ctx)
}
