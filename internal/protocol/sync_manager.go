package protocol

import (
	"context"
	"encoding/json"
	"fmt"
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

	// Checkpoints
	checkpoints []Checkpoint

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

		checkpoints: make([]Checkpoint, 0),

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

	log.Debug("Initializing checkpoints")
	for _, checkpointStr := range manager.Options.Checkpoints {
		c, err := ParseCheckpoint(checkpointStr)
		if err != nil {
			panic("Couldn't parse checkpoint")
		}
		manager.checkpoints = append(manager.checkpoints, c)
	}

	log.Debug("Registering SyncService")
	err := manager.server.Register(NewSyncService(&rpc, manager.bdmiProvider, &manager.downloadManager.MyTopoCache, config.SyncServiceOptions))
	if err != nil {
		log.Errorf("Error registering sync service: %s", err.Error())
		panic(err)
	}
	log.Debugf("SyncService successfully registered")

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

func (m *SyncManager) checkChainID(ctx context.Context, pid peer.ID) error {
	peerChainID := GetChainIDResponse{}
	{
		req := GetChainIDRequest{}
		subctx, cancel := context.WithTimeout(ctx, time.Duration(m.Options.RPCTimeoutMs)*time.Millisecond)
		defer cancel()
		err := m.client.CallContext(subctx, pid, "SyncService", "GetChainID", req, &peerChainID)
		if err != nil {
			log.Warnf("%v: error getting peer chain id, %v", pid, err)
			return err
		}
	}

	chainID, err := m.rpc.GetChainID(ctx)
	if err != nil {
		log.Errorf("%v: error getting chain id, %v", pid, err)
		return err
	}

	if !chainID.ChainID.Equals(&peerChainID.ChainID) {
		log.Warnf("%v: peer's chain id %v does not match my chain ID %v", pid, peerChainID.ChainID, chainID.ChainID)
		return fmt.Errorf("%v: peer's chain id does not match", pid)
	}
	return nil
}

// blockIDtoString returns a string representation of the BlockID
func blockIDtoString(h types.Multihash) string {
	id, err := json.Marshal(h)
	if err != nil {
		id = []byte("ERR")
	} else {
		id = id[1 : len(id)-1]
	}
	return string(id)
}

func (m *SyncManager) checkCheckpoints(ctx context.Context, pid peer.ID) error {
	blkid := blockIDtoString

	headBlockResp := GetHeadBlockResponse{}
	{
		req := GetHeadBlockRequest{}
		subctx, cancel := context.WithTimeout(ctx, time.Duration(m.Options.RPCTimeoutMs)*time.Millisecond)
		defer cancel()
		err := m.client.CallContext(subctx, pid, "SyncService", "GetHeadBlock", req, &headBlockResp)
		if err != nil {
			log.Warnf("%v: error getting peer head block, %v", pid, err)
			return err
		}
	}
	log.Infof("%v: peer has head block %v at height %d", pid, blkid(headBlockResp.ID), headBlockResp.Height)

	//
	// For each checkpoint, we call GetBlocksResponse / GetBlocksRequest
	// and throw away the block because the block store doesn't expose GetAncestorIDAtHeight().
	//
	// TODO: For speed and bandwidth efficiency, we should optimize these API calls
	// to a single batch call that only returns the needed information,
	// so we don't need an RPC round-trip to process each checkpoint,
	// nor do we get sent a block that we're going to throw away.
	//
	numPassedCheckpoints := 0
	for _, checkpoint := range m.checkpoints {
		if checkpoint.Height > headBlockResp.Height {
			continue
		}
		if checkpoint.Height == headBlockResp.Height {
			if !checkpoint.ID.Equals(&headBlockResp.ID) {
				log.Warnf("%v: peer's head block at height %d is %v and does not match checkpoint %v",
					pid, headBlockResp.Height, blkid(headBlockResp.ID), blkid(checkpoint.ID))
				return fmt.Errorf("%v: peer's head block at height %d is %v and does not match checkpoint %v",
					pid, headBlockResp.Height, blkid(headBlockResp.ID), blkid(checkpoint.ID))
			}
			continue
		}
		req := GetTopologyAtHeightRequest{}
		req.BlockHeight = checkpoint.Height
		req.NumBlocks = 1
		resp := GetTopologyAtHeightResponse{}
		subctx, cancel := context.WithTimeout(ctx, time.Duration(m.Options.RPCTimeoutMs)*time.Millisecond)
		defer cancel()
		err := m.client.CallContext(subctx, pid, "SyncService", "GetTopologyAtHeight", req, &resp)
		if err != nil {
			log.Warnf("%v: error calling GetTopologyAtHeight, %v", pid, err)
			return err
		}
		if len(resp.BlockTopology) == 0 {
			log.Warnf("%v: peer claimed head height of %d but could not provide checkpoint at height %d",
				pid, headBlockResp.Height, checkpoint.Height)
			return fmt.Errorf("%v: peer claimed head height of %d but could not provide checkpoint at height %d",
				pid, headBlockResp.Height, checkpoint.Height)
		}
		hasCheckpoint := false
		for _, topo := range resp.BlockTopology {
			if topo.Height != checkpoint.Height {
				log.Warnf("%v: peer sent unexpected height %d when height %d was requested",
					pid, topo.Height, checkpoint.Height)
				return fmt.Errorf("%v: peer sent unexpected height %d when height %d was requested",
					pid, topo.Height, checkpoint.Height)
			}
			if topo.ID.Equals(&checkpoint.ID) {
				hasCheckpoint = true
			}
		}

		if !hasCheckpoint {
			for _, topo := range resp.BlockTopology {
				log.Warnf("%v: peer has non-matching checkpoint block at height %d, peer block is %v, checkpoint is %v",
					pid, checkpoint.Height, blkid(topo.ID), blkid(checkpoint.ID))
			}
			return fmt.Errorf("%v: peer has non-matching checkpoint block at height %d",
				pid, checkpoint.Height)
		}
		numPassedCheckpoints += 1
	}
	log.Infof("%v: successful handshake to peer with head block %v at height %d, %d checkpoints passed",
		pid, blkid(headBlockResp.ID), headBlockResp.Height, numPassedCheckpoints)
	return nil
}

func (m *SyncManager) doPeerHandshake(ctx context.Context, pid peer.ID) {

	err := func() error {
		log.Debugf("connecting to peer for sync: %v", pid)
		err := m.checkChainID(ctx, pid)
		if err != nil {
			return err
		}

		err = m.checkCheckpoints(ctx, pid)
		if err != nil {
			return err
		}

		select {
		case m.handshakeDonePeers <- pid:
		case <-ctx.Done():
		}

		log.Infof("Connected to peer for sync: %v", pid)
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
