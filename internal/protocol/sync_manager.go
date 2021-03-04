package protocol

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/koinos/koinos-p2p/internal/rpc"

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

type PeerBlockResponse struct {
	Topology  types.BlockTopology
	Responder peer.ID
	Block     *types.OpaqueBlock
}

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
	server *gorpc.Server
	client *gorpc.Client
	rpc    rpc.RPC

	// Channel for new peer ID's we want to connect to
	newPeers chan peer.ID

	// Channel for peer to notify when handshake is done
	handshakeDonePeers chan peer.ID

	// Channel for peer to notify when error occurs
	errPeers chan PeerError

	// Channel for peer ID's coming off a blacklist
	unblacklistPeers chan peer.ID

	// Peer ID map.  Only updated in the internal SyncManager thread
	peers map[peer.ID]void

	// Blacklisted peers.
	blacklist map[peer.ID]void
}

type void struct{}

// NewSyncManager factory
func NewSyncManager(ctx context.Context, h host.Host, rpc rpc.RPC) *SyncManager {

	manager := SyncManager{
		server: gorpc.NewServer(h, SyncID),
		client: gorpc.NewClient(h, SyncID),
		rpc:    rpc,

		newPeers:           make(chan peer.ID),
		handshakeDonePeers: make(chan peer.ID),
		errPeers:           make(chan PeerError),
		unblacklistPeers:   make(chan peer.ID),

		peers:     make(map[peer.ID]void),
		blacklist: make(map[peer.ID]void),
	}

	err := manager.server.Register(NewSyncService(&rpc))
	if err != nil {
		panic(err)
	}

	// TODO: What is context?
	peerAdder := NewSyncManagerPeerAddr(ctx, h, &manager)
	h.Network().Notify(&peerAdder)

	return &manager
}

// Add a peer to the SyncManager.
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

		peerChainID := GetChainIDResponse{}
		{
			subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
			defer cancel()
			err := m.client.CallContext(subctx, pid, "SyncService", "GetChainID", GetChainIDRequest{}, &peerChainID)
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

		headBlock, err := m.rpc.GetHeadBlock()
		if err != nil {
			log.Printf("%v: error getting head block, %v", pid, err)
			return err
		}

		peerForkStatus := GetForkStatusResponse{}
		{
			subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
			defer cancel()
			err = m.client.CallContext(subctx, pid, "SyncService", "GetForkStatus", GetForkStatusRequest{HeadID: headBlock.ID, HeadHeight: headBlock.Height}, &peerForkStatus)
			if err != nil {
				log.Printf("%v: error getting peer fork status, %v", pid, err)
				return err
			}
		}

		if peerForkStatus.Status == DifferentFork {
			log.Printf("%v: peer is on a different fork", pid)
			return fmt.Errorf("%v: peer is on a different fork", pid)
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

// Blacklist a peer.  Runs in the main thread.
func (m *SyncManager) blacklistPeer(ctx context.Context, pid peer.ID, blacklistTime time.Duration) {
	// Add to the blacklist now
	m.blacklist[pid] = void{}

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
	/*
	   for {
	      var peer peer.ID

	      select {
	      case pid <- m.newPeers:
	         _, isBlacklisted := m.blacklist[pid]
	         if !isBlacklisted {
	            go doPeerHandshake(ctx, pid)
	         }
	      case pid <- m.handshakeDonePeers:
	         m.peers[pid] = void{}
	      case perr <- m.errPeers:
	         // If peer quit with error, blacklist it for a while so we don't spam reconnection attempts
	         m.blacklistPeer(perr.PeerID, time.Duration(blacklistSeconds)*time.Second)
	         delete(m.peers, perr.PeerID)
	      case pid <- m.unblacklistPeers:
	         delete(m.blacklist, pid)
	      case <-ctx.Done():
	         return
	      }
	   }

	   smallestHeadBlock := ^uint64(0)
	   peerHeads := make(map[peer.ID]types.Multihash)
	   peersToDisconnect := make(map[peer.ID]void)
	   peersToGossip := make(map[peer.ID]void)

	   for peer := range peers {
	      peerHeadBlock := GetHeadBlockResponse{}
	      ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	      defer cancel()
	      err := m.client.CallContext(ctx, peer, "SyncService", "GetHeadBlock", GetHeadBlockRequest{}, &peerHeadBlock)
	      if err != nil {
	         log.Printf("%v: error getting peer head block, disconnecting from peer", peer)
	         peersToDisconnect[peer] = void{}
	      } else {
	         log.Print(peerHeadBlock.Height)
	         if peerHeadBlock.Height <= headBlock.Height+types.BlockHeightType(syncDelta) {
	            peersToGossip[peer] = void{}
	         } else if uint64(peerHeadBlock.Height) < smallestHeadBlock {
	            smallestHeadBlock = uint64(peerHeadBlock.Height)
	         }

	         peerHeads[peer] = peerHeadBlock.ID
	      }
	   }

	   m.peerLock.Lock()
	   for peer := range peersToGossip {
	      log.Printf("synced with peer %v, moving to gossip", peer)
	      // TODO: Send to gossip
	      delete(m.peers, peer)
	      delete(peers, peer)
	   }
	   for peer := range peersToDisconnect {
	      log.Printf("disconnecting from peer %v", peer)
	      // TODO: Trigger disconnect?
	      delete(m.peers, peer)
	      delete(peers, peer)
	   }
	   m.peerLock.Unlock()

	   if len(peers) == 0 {
	      continue
	   }

	   blockDelta := (smallestHeadBlock - uint64(headBlock.Height)) / 2
	   if blockDelta > maxBlockDelta {
	      blockDelta = maxBlockDelta
	   }
	   targetSyncBlock := uint64(headBlock.Height) + blockDelta

	   // Get the fork each peer is on
	   peerForks := m.getPeerForks(targetSyncBlock, peers, peerHeads)

	   syncChans := make([]chan int, 0, len(peerForks))

	   for _, peers := range peerForks {
	      doneChan := make(chan int)
	      go m.syncFork(targetSyncBlock, headBlock, peers, peerHeads, doneChan)
	      syncChans = append(syncChans, doneChan)
	   }

	   for _, done := range syncChans {
	      <-done
	   }
	*/
}

// Start syncing blocks from peers
func (m *SyncManager) Start(ctx context.Context) {
	go m.run(ctx)
}
