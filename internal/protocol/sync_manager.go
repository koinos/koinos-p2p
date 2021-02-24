package protocol

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
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
	batchSize      = uint64(20)
	syncDelta      = uint64(1)
	maxBlockDelta  = uint64(10000)
	timeoutSeconds = uint64(30)
)

// BatchBlockRequest a batch block request
type BatchBlockRequest struct {
	StartBlockHeight types.BlockHeightType
	BatchSize        types.UInt64
}

// SyncManager syncs blocks using multiple peers
type SyncManager struct {
	server *gorpc.Server
	client *gorpc.Client
	rpc    rpc.RPC

	peers      map[peer.ID]void
	peerLock   sync.Locker
	peerSignal *sync.Cond
}

type void struct{}

// NewSyncManager factory
func NewSyncManager(h host.Host, rpc rpc.RPC) *SyncManager {

	manager := SyncManager{
		server:     gorpc.NewServer(h, SyncID),
		client:     gorpc.NewClient(h, SyncID),
		rpc:        rpc,
		peers:      make(map[peer.ID]void),
		peerLock:   &sync.Mutex{},
		peerSignal: sync.NewCond(&sync.Mutex{}),
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
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
		err := m.client.CallContext(ctx, peer, "SyncService", "GetChainID", GetChainIDRequest{}, &peerChainID)
		if err != nil {
			log.Printf("%v: error getting peer chain id, %v", peer, err)
			return err
		}
	}

	chainID, err := m.rpc.GetChainID()
	if err != nil {
		log.Printf("%v: error getting chain id, %v", peer, err)
	}

	if !chainID.ChainID.Equals(&peerChainID.ChainID) {
		log.Printf("%v: peer's chain id does not match", peer)
		return fmt.Errorf("%v: peer's chain id does not match", peer)
	}

	headBlock, err := m.rpc.GetHeadBlock()
	if err != nil {
		log.Printf("%v: error getting head block, %v", peer, err)
		return err
	}

	peerForkStatus := GetForkStatusResponse{}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
		err = m.client.CallContext(ctx, peer, "SyncService", "GetForkStatus", GetForkStatusRequest{HeadID: headBlock.ID, HeadHeight: headBlock.Height}, &peerForkStatus)
		if err != nil {
			log.Printf("%v: error getting peer fork status, %v", peer, err)
			return err
		}
	}

	if peerForkStatus.Status == DifferentFork {
		log.Printf("%v: peer is on a different fork", peer)
		return fmt.Errorf("%v: peer is on a different fork", peer)
	}

	m.peerLock.Lock()
	defer m.peerLock.Unlock()

	m.peers[peer] = void{}
	m.peerSignal.Signal()

	log.Printf("%v: connected!", peer)

	return nil
}

func createBlockRequests(headBlock uint64, targetSyncBlock uint64, requestQueue chan BatchBlockRequest) {
	currentBlock := uint64(headBlock)
	currentBatchSize := batchSize

	if currentBlock+batchSize > targetSyncBlock {
		currentBatchSize = targetSyncBlock - currentBlock
	}

	for currentBlock <= targetSyncBlock {
		requestQueue <- BatchBlockRequest{
			StartBlockHeight: types.BlockHeightType(currentBlock) + 1,
			BatchSize:        types.UInt64(currentBatchSize),
		}

		currentBlock += batchSize
		if currentBlock+batchSize > targetSyncBlock {
			currentBatchSize = targetSyncBlock - currentBlock
		}
	}
}

func applyBlocks(headBlock uint64, targetSyncBlock uint64, rpc rpc.RPC, requestQueue chan BatchBlockRequest, responseQueue chan BlockBatch, doneChan chan int) {
	blockHeap := &BlockBatchHeap{}
	heap.Init(blockHeap)

	for resp := range responseQueue {
		// Add the response to the heap
		heap.Push(blockHeap, resp)

		// While the heap's head is <= head block + 1, pop and apply.
		for len(*blockHeap) > 0 && uint64((*blockHeap)[0].StartBlockHeight) <= headBlock+1 {
			batch := heap.Pop(blockHeap).(BlockBatch)
			_, blocks, err := types.DeserializeVectorBlockItem(batch.VectorBlockItems)
			if err != nil {
				requestQueue <- BatchBlockRequest{
					StartBlockHeight: batch.StartBlockHeight,
					BatchSize:        batch.BatchSize,
				}
			}

			for _, blockItem := range *blocks {
				blockItem.Block.Unbox()
				topology := types.NewBlockTopology()
				topology.Height = blockItem.BlockHeight
				topology.ID = blockItem.BlockID

				block, _ := blockItem.Block.GetNative()
				block.ActiveData.Unbox()
				active, _ := block.ActiveData.GetNative()
				topology.Previous = active.PreviousBlock

				ok, err := rpc.ApplyBlock(block, topology)

				// If application fails, send a new request and break
				if err != nil || !ok {
					requestQueue <- BatchBlockRequest{
						StartBlockHeight: topology.Height - 1,
						BatchSize:        batch.BatchSize - types.UInt64(topology.Height-batch.StartBlockHeight),
					}
					break
				}

				if uint64(topology.Height) > headBlock {
					headBlock = uint64(topology.Height)
				}
			}
		}

		if headBlock >= targetSyncBlock {
			doneChan <- 0
			return
		}
	}
}

func fetchBlocksFromPeer(peer peer.ID, headBlockID types.Multihash, client *gorpc.Client, requestQueue chan BatchBlockRequest, responseQueue chan BlockBatch) {
	request := GetBlocksRequest{
		HeadBlockID: headBlockID,
	}

	for batch := range requestQueue {
		log.Printf("request blocks %v-%v from peer %v", batch.StartBlockHeight, batch.StartBlockHeight+types.BlockHeightType(batch.BatchSize-1), peer)
		request.StartBlockHeight = batch.StartBlockHeight
		request.BatchSize = batch.BatchSize
		response := GetBlocksResponse{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
		err := client.CallContext(ctx, peer, "SyncService", "GetBlocks", request, &response)
		if err != nil {
			requestQueue <- batch
			time.Sleep(time.Duration(1000000000 + rand.Int31()%1000000000))
		} else {
			responseQueue <- BlockBatch{
				StartBlockHeight: request.StartBlockHeight,
				BatchSize:        request.BatchSize,
				VectorBlockItems: &response.VectorBlockItems,
			}
		}
	}
}

func (m *SyncManager) syncFork(targetSyncBlock uint64, headBlock *types.HeadInfo, peers map[peer.ID]void, peerHeads map[peer.ID]types.Multihash, doneChan chan int) {
	requestQueue := make(chan BatchBlockRequest, len(peers))
	responseQueue := make(chan BlockBatch, len(peers))
	syncDone := make(chan int)
	defer close(requestQueue)
	defer close(responseQueue)
	defer close(syncDone)

	log.Printf("target sync block is %v", targetSyncBlock)

	go createBlockRequests(uint64(headBlock.Height), targetSyncBlock, requestQueue)
	go applyBlocks(uint64(headBlock.Height), targetSyncBlock, m.rpc, requestQueue, responseQueue, doneChan)

	for peer := range peers {
		go fetchBlocksFromPeer(peer, peerHeads[peer], m.client, requestQueue, responseQueue)
	}

	<-syncDone
	doneChan <- 0
}

func (m *SyncManager) getPeerForks(targetSyncBlock uint64, peers map[peer.ID]void, peerHeads map[peer.ID]types.Multihash) map[string]map[peer.ID]void {
	peerForks := make(map[string]map[peer.ID]void)
	for p := range peers {
		peerForkBlockRequest := GetBlocksRequest{
			HeadBlockID:      peerHeads[p],
			StartBlockHeight: types.BlockHeightType(targetSyncBlock),
			BatchSize:        1,
		}
		peerForkBlock := GetBlocksResponse{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
		err := m.client.CallContext(ctx, p, "SyncService", "GetBlocks", peerForkBlockRequest, &peerForkBlock)
		if err != nil {
			log.Printf("%v: error getting peer fork block, %v", p, err)
			continue
		}

		_, blocks, err := types.DeserializeVectorBlockItem(&peerForkBlock.VectorBlockItems)
		if err != nil {
			log.Printf("%v: error deserializing peer fork block, %v", p, err)
			continue
		}
		if len(*blocks) != 1 {
			log.Printf("%v: peer did not send only one fork block", p)
			continue
		}

		forkPeers, ok := peerForks[string((*blocks)[0].BlockID.Digest)]
		if !ok {
			forkPeers = make(map[peer.ID]void, 0)
		}

		forkPeers[p] = void{}
		peerForks[string((*blocks)[0].BlockID.Digest)] = forkPeers
	}

	return peerForks
}

func (m *SyncManager) run() {
	for {
		// Wait for peers
		m.peerSignal.L.Lock()
		for len(m.peers) == 0 {
			m.peerSignal.Wait()
		}
		peers := m.peers
		m.peerSignal.L.Unlock()

		headBlock, err := m.rpc.GetHeadBlock()
		if err != nil {
			log.Printf("error getting head block, %v", err)
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
	}
}

// Start syncing blocks from peers
func (m *SyncManager) Start() {
	go m.run()
}
