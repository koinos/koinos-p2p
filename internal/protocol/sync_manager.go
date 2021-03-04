package protocol

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"math/rand"
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
	batchSize        = uint64(20)
	syncDelta        = uint64(1)
	maxBlockDelta    = uint64(10000)
	timeoutSeconds   = uint64(30)
	blacklistSeconds = uint64(60)
	syncCycleSeconds = uint64(10)
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

type InFlightBlockRequest struct {
	Request         PeerBlockRequest
	DownloadingPeer peer.ID
	WaitingPeers    map[peer.ID]void
}

// SyncManager syncs blocks using multiple peers
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

	// Channel for block requests
	blockRequests chan PeerBlockRequest

	// Channel for completed block downloads
	blockRequestDone chan PeerBlockResponse

	// Channel for block downloads that had error
	blockRequestErr chan PeerBlockResponseError

	// Channel for block downloads that didn't apply
	blockRequestApplyErr chan PeerBlockResponseError

	// Peer ID map.  Only updated in the internal SyncManager thread
	peers map[peer.ID]void

	// Blacklisted peers.
	blacklist map[peer.ID]void

	// Currently downloading blocks
	blockDownloads map[string]InFlightBlockRequest
}

type void struct{}

// NewSyncManager factory
func NewSyncManager(h host.Host, rpc rpc.RPC) *SyncManager {

	manager := SyncManager{
		server: gorpc.NewServer(h, SyncID),
		client: gorpc.NewClient(h, SyncID),
		rpc:    rpc,

		newPeers:           make(chan peer.ID),
		handshakeDonePeers: make(chan peer.ID),
		errPeers:           make(chan peerError),
		unblacklistPeers:   make(chan peer.ID),
		blockRequests:      make(chan PeerBlockRequest),

		peers:     make(map[peer.ID]void),
		blacklist: make(map[peer.ID]void),
	}

	err := manager.server.Register(NewSyncService(&rpc))
	if err != nil {
		panic(err)
	}

	// TODO: What is context?
	h.Network().Notify(NewSyncManagerPeerAddr(ctx, h, &manager))

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

func (m *SyncManager) doPeerHandshake(ctx context.Context, pid peer.ID) error {

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
	}()

	if err != nil {
		select {
		case m.errPeers <- pid:
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
		case m.unblacklistPeers <- PeerError{pid, err}:
		case <-ctx.Done():
		}
	}()
}

func (m *SyncManager) syncLoop(ctx context.Context, pid peer.ID) {
	for {
		err := syncCycle(ctx, pid)

		if err != nil {
			select {
			case m.errPeers <- pid:
			case <-ctx.Done():
			}
			return
		}

		select {
		case <-time.After(time.Duration(syncCycleSeconds) * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

// Get the heights that we want to query.
func GetInitialQueryHeights(myHeight uint64, yourHeight uint64, myIrrHeight uint64, numResults uint64, batchDelta uint64, batchSink uint64) []uint64 {
	// TODO:  Refactor to prefer getting critical heights to help cache hits
	lib := myIrrHeight
	result := make([]uint64, 0)

	h := myHeight
	if yourHeight < h {
		h = yourHeight
	}
	if h <= lib {
		return result
	}

	delta := yourHeight - lib
	if delta <= numResults {
		for i = 0; i < delta; i++ {
			result = append(result, lib+i)
		}
		return result
	}

	start := yourHeight - 1
	if start > myHeight {
		start = myHeight + 1
		hi2 = myHeight + batchDelta - batchSink
		hi1 = lib + batchDelta
		if hi2 > yourHeight {
			hi2 = yourHeight
		}
		if hi1 > hi2 {
			hi1 = (start + hi2) / 2
		}

		if hi2 > start {
			result = append(result, hi2)
		}

		if hi1 > start {
			result = append(result, hi1)
		}
	}

	x := start
	for i = 0; i < numResults-4; i++ {
		if x <= lib {
			return result
		}
		result = append(result, x)
		x -= 1
	}

	m := (x + lib) / 2
	result = append(result, m)
	result = append(result, lib)
	return result
}

func (m *SyncManager) syncCycle(ctx context.Context, pid peer.ID) error {
	// Do a single cycle of sync mode

	// What fork heads do you have available?  What's your LIB?
	yourForkHeads := GetForkHeadsResponse{}
	subctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	err := m.client.CallContext(subctx, pid, "SyncService", "GetForkHeads", GetForkHeadsRequest{}, &forkHeadsResponse)
	if err != nil {
		log.Printf("%v: error getting peer head block, error was %v\n", pid, err)
		return err
	}

	// What fork heads do I have available?  What's my LIB?
	myForkHeads, err := m.rpc.GetForkHeads()
	if err != nil {
		// TODO:  We lose all peers when there's an error return due to this purely local condition
		// Perhaps we should have some failure-retry for local errors like this?
		log.Printf("error getting head block, %v", err)
		return err
	}

	rand.Shuffle(len(yourForkHeads), func(i, j int) {
		yourForkHeads[i], yourForkHeads[j] = yourForkHeads[j], yourForkHeads[i]
	})

	// TODO:  Put myForkHeads in a dictionary to make this comparison not O(n^2)
	// TODO:  Check cases in order of expense.  E.g. we can do extension checks for everybody really fast.
	// TODO:  Cache your topological assertions instead of asking you every time.
	for _, yourHead := range yourForkHeads.ForkHeads {
		for _, myHead := range myForkHeads.ForkHeads {

			// Are we both at the same head on this fork?
			if yourHead.ID.Equals(myHead.ID) {
				continue
			}

			// Are you 1 block ahead of me on this fork?
			if yourHead.Previous.Equals(myHead.ID) {
				select {
				case m.blockRequests <- PeerBlockRequest{yourHead, pid}:
				case <-ctx.Done():
					return
				}
				continue
			}

			// Are you behind my LIB height on this fork?
			lih := uint64(myForkHeads.LastIrr.Height)
			if uint64(yourHead.Height) <= lih {
				continue
			}

			// You're multiple blocks ahead of me on this fork.
			// I need to find the MRCA of your fork and my fork.
			// Query your topology for some heights on your fork.

			// Query heights
			// GetInitialQueryHeights(myHeight, yourHeight, myIrrHeight, numResults, batchDelta, batchSink)
			heights := GetInitialQueryHeights(uint64(myHead.Height), uint64(yourHead.Height), lib, 8, 20, 6)
			log.Printf("%v: Querying heights %v (my=%d, your=%d, lib=%d)", pid, heights, myHead.Height, yourHead.Height, lib)

			ancestorTopology := GetAncestorTopologyAtHeights(yourHead.ID, heights)

			for _, yourTopo := range ancestorTopology {
				if (yourTopo.Height == myHead.Height+1) && (yourTopo.Previous.Equals(myHead.ID)) {
					doneChan := make(chan PeerBlockResponse, 1)
					errChan := make(chan PeerBlockResponseError, 1)
					select {
					case m.blockRequests <- PeerBlockRequest{yourTopo, pid, doneChan, errChan}:
					case <-ctx.Done():
						return
					}

					select {
					case resp := <-doneChan:
						// TODO does the peer need to do anything with the response?
					case errResp := <-errChan:
						log.Printf("error getting head block, %v\n", errResp.Error)
						return err
					case <-ctx.Done():
					}
					break
				}
				// TODO:  Handle pushing fork blocks if we're on a fork
				// TODO:  Batch requests
			}
		}
	}
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

func doBlockRequest(req PeerBlockRequest) {
	// Download the block from req.Requester
	// Attempt to apply the block
	// Send to blockRequestDone, blockRequestErr, or blockRequestApplyErr as appropriate

	// Note:  We don't want to send to the peer's DoneChan or ErrChan in this method,
	// since SyncManager needs to update its state in the main thread.
}

func (m *SyncManager) run(ctx context.Context) {
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

		case req := <-m.blockRequests:
			k := req.Topology.Serialize(NewVariableBlob())
			currentReq, hasReq := m.blockDownloads[k]
			if hasReq {
				// TODO check that this doesn't overwrite an existing entry and also isn't the current downloader
				currentReq.WaitingReq[req.Requester] = req
			} else {
				m.blockDownloads[k] = InFlightBlockRequest{
					DownloadingReq: req,
					WaitingReq:     make(map[peer.ID]void),
				}
				go doBlockRequest(req)
			}

		case resp := <-m.blockRequestDone:
			k := resp.Topology.Serialize(NewVariableBlob())
			currentReq, hasReq := m.blockDownloads[k]
			if !hasReq {
				panic("Illegal state, blockDownloads entry was not found")
			}
			currentReq.DownloadingReq.DoneChan <- resp
			for _, waiter := range currentReq.WaitingReq {
				waiter.DoneChan <- resp
			}
			delete(m.BlockDownloads, k)

		case peerDone := <-m.blockRequestErr:
			// We only send the error code to the peer that couldn't download it
			k := resp.Topology.Serialize(NewVariableBlob())
			currentReq, hasReq := m.blockDownloads[k]
			if !hasReq {
				panic("Illegal state, blockDownloads entry was not found")
			}
			currentReq.DownloadingReq.ErrChan <- resp

			if len(currentReq.WaitingReq) == 0 {
				delete(m.BlockDownloads, k)
			} else {
				// TODO we just pick the first key in the Golang map key traversal order, do we want to pick randomly instead?
				for newDownloaderID, newDownloader := range currentReq.WaitingReq {
					break
				}
				delete(currentReq.WaitingReq, newDownloaderID)
				currentReq.DownloadingReq = newDownloader
				go doBlockRequest(newDownloader)
			}

		case applyErr := <-m.blockRequestApplyErr:
			// TODO write this code

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
}

// Start syncing blocks from peers
func (m *SyncManager) Start() {
	go m.run()
}
