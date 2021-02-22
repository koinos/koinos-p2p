package protocol

import (
	"container/heap"
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
	batchSize = uint64(20)
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

	peers      []peer.ID
	peerLock   sync.Locker
	peerSignal *sync.Cond
}

// NewSyncManager factory
func NewSyncManager(h host.Host, rpc rpc.RPC) *SyncManager {

	manager := SyncManager{
		server:     gorpc.NewServer(h, SyncID),
		client:     gorpc.NewClient(h, SyncID),
		rpc:        rpc,
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

	// TODO: Add timeout via CallContext
	peerChainID := GetChainIDResponse{}
	err := m.client.Call(peer, "SyncService", "GetChainID", GetChainIDRequest{}, &peerChainID)
	if err != nil {
		log.Printf("%v: error getting peer chain id, %v", peer, err)
		return err
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
	err = m.client.Call(peer, "SyncService", "GetForkStatus", GetForkStatusRequest{HeadID: headBlock.ID, HeadHeight: headBlock.Height}, &peerForkStatus)
	if err != nil {
		log.Printf("%v: error getting peer fork status, %v", peer, err)
		return err
	}

	if peerForkStatus.Status == DifferentFork {
		log.Printf("%v: peer is on a different fork", peer)
		return fmt.Errorf("%v: peer is on a different fork", peer)
	}

	m.peerLock.Lock()
	defer m.peerLock.Unlock()

	m.peers = append(m.peers, peer)
	m.peerSignal.Signal()

	log.Printf("%v: connected!", peer)

	return nil
}

func remove(s []int, i int) []int {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func createBlockRequests(headBlock uint64, targetSyncBlock uint64, requestQueue chan BatchBlockRequest) {
	currentBlock := uint64(headBlock) + 1
	currentBatchSize := batchSize

	for currentBlock < targetSyncBlock {
		requestQueue <- BatchBlockRequest{
			StartBlockHeight: types.BlockHeightType(currentBlock),
			BatchSize:        types.UInt64(currentBatchSize),
		}

		currentBlock += batchSize
		if currentBlock+batchSize > targetSyncBlock {
			currentBatchSize = targetSyncBlock - currentBlock
		}
	}
}

func applyBlocks(headBlock types.BlockHeightType, rpc rpc.RPC, requestQueue chan BatchBlockRequest, responseQueue chan BlockBatch, doneChan chan int) {
	defer close(doneChan)
	defer close(requestQueue)
	blockHeap := &BlockBatchHeap{}
	heap.Init(blockHeap)

	for resp := range responseQueue {
		// Add the response to the heap
		heap.Push(blockHeap, resp)

		// While the heap's head is <= head block + 1, pop and apply.
		for len(*blockHeap) > 0 && (*blockHeap)[0].StartBlockHeight <= headBlock+1 {
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

				if topology.Height > headBlock {
					headBlock = topology.Height
				}
			}
		}
	}
}

func fetchBlocksFromPeer(peer peer.ID, headBlockID types.Multihash, client *gorpc.Client, requestQueue chan BatchBlockRequest, responseQueue chan BlockBatch) {
	//parent := context.Background()
	request := GetBlocksRequest{
		HeadBlockID: headBlockID,
	}

	for batch := range requestQueue {
		log.Printf("%v: Request blocks from peer %v-%v", peer, batch.StartBlockHeight, batch.StartBlockHeight+types.BlockHeightType(batch.BatchSize-1))
		request.StartBlockHeight = batch.StartBlockHeight
		request.BatchSize = batch.BatchSize
		response := GetBlocksResponse{}

		//ctx, cancel := context.WithTimeout(parent, 1*time.Second)
		//defer cancel()
		//err := client.CallContext(ctx, peer, "SyncService", "GetBlocks", request, &response)
		err := client.Call(peer, "SyncService", "GetBlocks", request, &response)
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

		for _, peer := range peers {
			peerHeadBlock := GetHeadBlockResponse{}
			err := m.client.Call(peer, "SyncService", "GetHeadBlock", GetHeadBlockRequest{}, &peerHeadBlock)
			if err != nil {
				log.Printf("%v: error getting peer head block, disconnecting from peer", peer)
				// TODO: Remove peer
			}

			if peerHeadBlock.Height <= headBlock.Height {
				// TODO: Remove peer / move them to gossip
			} else if uint64(peerHeadBlock.Height) < smallestHeadBlock {
				smallestHeadBlock = uint64(peerHeadBlock.Height)
			}

			peerHeads[peer] = peerHeadBlock.ID
		}

		//targetSyncBlock := (smallestHeadBlock - types.UInt64(headBlock.Height)) / 2
		targetSyncBlock := smallestHeadBlock
		requestQueue := make(chan BatchBlockRequest, len(peers))
		responseQueue := make(chan BlockBatch, len(peers))
		doneChan := make(chan int)

		go createBlockRequests(uint64(headBlock.Height), targetSyncBlock, requestQueue)
		go applyBlocks(headBlock.Height, m.rpc, requestQueue, responseQueue, doneChan)

		for _, peer := range peers {
			go fetchBlocksFromPeer(peer, peerHeads[peer], m.client, requestQueue, responseQueue)
		}

		<-doneChan
		close(responseQueue)
	}
}

// Start syncing blocks from peers
func (m *SyncManager) Start() {
	go m.run()
}
