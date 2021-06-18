package protocol

import (
	"context"
	"time"

	"math/rand"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	util "github.com/koinos/koinos-util-golang"

	types "github.com/koinos/koinos-types-golang"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
)

// SyncID Identifies the koinos sync protocol
const SyncID = "/koinos/sync/1.0.0"

// BatchBlockRequest a batch block request
type BatchBlockRequest struct {
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
}

// PeerBlockResponse is a peer block response
type PeerBlockResponse struct {
	Topology  types.BlockTopology
	Responder peer.ID
	Block     *types.OptionalBlock
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
type SyncManager struct {
	rng *rand.Rand

	server *gorpc.Server
	client *gorpc.Client
	rpc    rpc.RPC

	Options         options.SyncManagerOptions
	downloadManager *BlockDownloadManager
	bdmiProvider    *BdmiProvider

	// Channel for new peer ID's we want to connect to
	newPeers chan peer.ID

	// Channel for peers to remove
	removedPeers chan peer.ID

	// Channel for reporting peer errors
	PeerErrorChan chan PeerError

	// Channel for peer to notify when handshake is done
	handshakeDonePeers chan peer.ID

	// rescanBlacklist is a ticker channel to rescan the Blacklist
	rescanBlacklist <-chan time.Time

	// Peer ID map.  Only updated in the internal SyncManager thread
	peers map[peer.ID]util.Void

	// Blacklist of peers.
	Blacklist *Blacklist
}

// NewSyncManager factory
func NewSyncManager(
	ctx context.Context,
	h host.Host,
	rpc rpc.RPC,
	peerErrorChan chan PeerError,
	config *options.Config) *SyncManager {

	// TODO pass rng as parameter
	// TODO initialize RNG from cryptographically secure source
	manager := SyncManager{
		rng: rand.New(rand.NewSource(99)),

		server: gorpc.NewServer(h, SyncID),
		client: gorpc.NewClient(h, SyncID),
		rpc:    rpc,

		Options: config.SyncManagerOptions,

		newPeers:           make(chan peer.ID),
		handshakeDonePeers: make(chan peer.ID),
		removedPeers:       make(chan peer.ID),
		PeerErrorChan:      peerErrorChan,

		peers:     make(map[peer.ID]util.Void),
		Blacklist: NewBlacklist(config.BlacklistOptions),
	}
	manager.bdmiProvider = NewBdmiProvider(manager.client, rpc, config.BdmiProviderOptions, config.PeerHandlerOptions)
	manager.downloadManager = NewBlockDownloadManager(manager.rng, manager.bdmiProvider, config.DownloadManagerOptions)
	// TODO: Find a good place to call ticker.Stop() to avoid leak
	ticker := time.NewTicker(time.Duration(config.BlacklistOptions.BlacklistRescanMs) * time.Millisecond)
	manager.rescanBlacklist = ticker.C

	log.Debugm("Registering SyncService")
	err := manager.server.Register(NewSyncService(&rpc, manager.bdmiProvider, &manager.downloadManager.MyTopoCache, config.SyncServiceOptions))
	if err != nil {
		log.Errorm("Error registering sync service",
			"err", err)
		panic(err)
	}
	log.Debugm("SyncService successfully registered")

	// TODO: What is context?
	peerAdder := NewSyncManagerPeerAdder(ctx, h, &manager)
	h.Network().Notify(&peerAdder)

	return &manager
}

// SetGossipEnableHandler adds a handler to receive enable/disable gossip.
func (m *SyncManager) SetGossipEnableHandler(geh GossipEnableHandler) {
	m.bdmiProvider.GossipEnableHandler = geh
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

// RemovePeer removes a peer from the SyncManager
// Will cleanup the peer in the background.
func (m *SyncManager) RemovePeer(ctx context.Context, pid peer.ID) {
	go func() {
		select {
		case m.removedPeers <- pid:
		case <-ctx.Done():
		}
	}()

	return
}

func (m *SyncManager) doPeerHandshake(ctx context.Context, pid peer.ID) {

	err := func() error {
		log.Debugm("Connecting to peer for sync",
			"peer", pid)

		peerChainID := GetChainIDResponse{}
		{
			req := GetChainIDRequest{}
			subctx, cancel := context.WithTimeout(ctx, time.Duration(m.Options.RPCTimeoutMs)*time.Millisecond)
			defer cancel()
			err := m.client.CallContext(subctx, pid, "SyncService", "GetChainID", req, &peerChainID)
			if err != nil {
				log.Warnm("Error getting peer chain id",
					"peer", pid,
					"err", err)
				return err
			}
		}

		chainID, err := m.rpc.GetChainID(ctx)
		if err != nil {
			log.Errorm("Error getting chain id",
				"peer", pid,
				"err", err)
			return err
		}

		if !chainID.ChainID.Equals(&peerChainID.ChainID) {
			log.Warnm("Peer's chain ID does not match my chain ID",
				"peer", pid,
				"peerChainID", peerChainID.ChainID,
				"myChainID", chainID.ChainID)
			return log.NewErrorm("Peer's chain ID does not match my chain ID",
				"peer", pid,
				"peerChainID", peerChainID.ChainID,
				"myChainID", chainID.ChainID)
		}

		select {
		case m.handshakeDonePeers <- pid:
		case <-ctx.Done():
		}

		log.Infom("Connected to peer for sync", "pid", pid)
		return nil
	}()

	if err != nil {
		select {
		case m.PeerErrorChan <- PeerError{pid, err}:
		case <-ctx.Done():
		}
	}
}

func (m *SyncManager) doPeerEnableDownload(ctx context.Context, pid peer.ID) {
	m.bdmiProvider.NewPeer(ctx, pid)
}

func (m *SyncManager) doRemovePeer(ctx context.Context, pid peer.ID) {
	// Handoff to BdmiProvider
	m.bdmiProvider.RemovePeer(ctx, pid)
}

func (m *SyncManager) run(ctx context.Context) {
	for {
		select {
		case pid := <-m.newPeers:
			isBlacklisted := m.Blacklist.IsPeerBlacklisted(pid)
			if !isBlacklisted {
				go m.doPeerHandshake(ctx, pid)
			}
		case pid := <-m.handshakeDonePeers:
			m.peers[pid] = util.Void{}
			// Now that our data structures are all set up, we're ready to send it off to the BdmiProvider
			go m.doPeerEnableDownload(ctx, pid)
		case pid := <-m.removedPeers:
			go m.doRemovePeer(ctx, pid)
			delete(m.peers, pid)
		case perr := <-m.PeerErrorChan:
			// If peer quit with error, blacklist it for a while so we don't spam reconnection attempts
			m.Blacklist.AddPeerToBlacklist(perr)
			delete(m.peers, perr.PeerID)
		case <-m.rescanBlacklist:
			m.Blacklist.RemoveExpiredBlacklistEntries()
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

// HandleBlockBroadcast handles block broadcast
func (m *SyncManager) HandleBlockBroadcast(ctx context.Context, blockBroadcast *types.BlockAccepted) {
	m.bdmiProvider.HandleBlockBroadcast(ctx, blockBroadcast)
}

// HandleForkHeads handles fork heads broadcast
func (m *SyncManager) HandleForkHeads(ctx context.Context, forkHeads *types.ForkHeads) {
	m.bdmiProvider.UpdateForkHeads(ctx, forkHeads)
}
