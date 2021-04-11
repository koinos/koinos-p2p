package protocol

import (
	"context"
	"fmt"
	"time"

	"math/rand"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-p2p/internal/util"
	"go.uber.org/zap"

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

	// Channel for peers that have errors
	errPeers chan PeerError

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
func NewSyncManager(ctx context.Context, h host.Host, rpc rpc.RPC, config *options.Config) *SyncManager {

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
		errPeers:           make(chan PeerError),

		peers:     make(map[peer.ID]util.Void),
		Blacklist: NewBlacklist(config.BlacklistOptions),
	}
	manager.bdmiProvider = NewBdmiProvider(manager.client, rpc, config.BdmiProviderOptions, config.PeerHandlerOptions)
	manager.downloadManager = NewBlockDownloadManager(manager.rng, manager.bdmiProvider, config.DownloadManagerOptions)
	// TODO: Find a good place to call ticker.Stop() to avoid leak
	ticker := time.NewTicker(time.Duration(config.BlacklistOptions.BlacklistRescanMs) * time.Millisecond)
	manager.rescanBlacklist = ticker.C

	zap.L().Debug("Registering SyncService")
	err := manager.server.Register(NewSyncService(&rpc, config.SyncServiceOptions))
	if err != nil {
		zap.S().Error("Error registering sync service: %s", err.Error())
		panic(err)
	}
	zap.S().Debug("SyncService successfully registered")

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
		zap.S().Debug("connecting to peer for sync: %v", pid)

		peerChainID := GetChainIDResponse{}
		{
			req := GetChainIDRequest{}
			subctx, cancel := context.WithTimeout(ctx, time.Duration(m.Options.RPCTimeoutMs)*time.Millisecond)
			defer cancel()
			err := m.client.CallContext(subctx, pid, "SyncService", "GetChainID", req, &peerChainID)
			if err != nil {
				zap.S().Warn("%v: error getting peer chain id, %v", pid, err)
				return err
			}
		}

		chainID, err := m.rpc.GetChainID(ctx)
		if err != nil {
			zap.S().Error("%v: error getting chain id, %v", pid, err)
			return err
		}

		if !chainID.ChainID.Equals(&peerChainID.ChainID) {
			zap.S().Warn("%v: peer's chain id %v does not match my chain ID %v", pid, peerChainID.ChainID, chainID.ChainID)
			return fmt.Errorf("%v: peer's chain id does not match", pid)
		}

		select {
		case m.handshakeDonePeers <- pid:
		case <-ctx.Done():
		}

		zap.S().Info("Connected to peer for sync: %v", pid)
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
		case perr := <-m.errPeers:
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
