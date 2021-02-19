package protocol

import (
	"fmt"
	"log"
	"sync"

	"github.com/koinos/koinos-p2p/internal/rpc"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
)

// SyncID Identifies the koinos sync protocol
const SyncID = "/koinos/sync/1.0.0"

// SyncManager syncs blocks using multiple peers
type SyncManager struct {
	server *gorpc.Server
	client *gorpc.Client
	rpc    rpc.RPC

	peers    []peer.ID
	peerLock sync.Locker
}

// NewSyncManager factory
func NewSyncManager(h host.Host, rpc rpc.RPC) *SyncManager {

	manager := SyncManager{
		server:   gorpc.NewServer(h, SyncID),
		client:   gorpc.NewClient(h, SyncID),
		rpc:      rpc,
		peerLock: &sync.Mutex{},
	}

	err := manager.server.Register(NewSyncService(&rpc))
	if err != nil {
		panic(err)
	}

	return &manager
}

// AddPeer to manager to begin requesting blocks from
func (m *SyncManager) AddPeer(peer peer.ID) error {
	log.Printf("connecting to peer for sync: %v", peer)

	peerChainID := GetChainIDResponse{}
	err := m.client.Call(peer, "SyncService", "GetChainID", GetChainIDRequest{}, &peerChainID)
	if err != nil {
		return err
	}

	chainID, err := m.rpc.GetChainID()
	if !chainID.ChainID.Equals(&peerChainID.ChainID) {
		log.Printf("%v: peer's chain id does not match", peer)
		return fmt.Errorf("%v: peer's chain id does not match", peer)
	}

	headBlock, err := m.rpc.GetHeadBlock()
	peerForkStatus := GetForkStatusResponse{}
	err = m.client.Call(peer, "SyncService", "GetForkStatus", GetForkStatusRequest{HeadID: headBlock.ID, HeadHeight: headBlock.Height}, &peerForkStatus)
	if err != nil {
		return err
	}

	if peerForkStatus.Status == DifferentFork {
		log.Printf("%v: peer is on a different fork", peer)
		return fmt.Errorf("%v: peer is on a different fork", peer)
	}

	m.peerLock.Lock()
	defer m.peerLock.Unlock()

	m.peers = append(m.peers, peer)

	log.Printf("%v: connected!", peer)

	return nil
}

func (m *SyncManager) run() {
	// TODO: Implement sync manager algorithm here
}

// Start syncing blocks from peers
func (m *SyncManager) Start() {
	go m.run()
}
