package protocol

const (
	defaultMaxDownloadsInFlight int = 8
	defaultMaxDownloadDepth     int = 3
)

// BlockDownloadRequest represents a block download request that has been issued to a peer.
type BlockDownloadRequest struct {
	SerTopology string
	Topology    types.BlockTopology
	PeerID      peer.ID
}

// BlockDownloadResponse represents a peer's response to a BlockDownloadRequest.
//
// The response can be an Err.
// The response is not yet applied.
//
type BlockDownloadResponse struct {
	SerTopology string
	Topology    types.BlockTopology
	PeerID      peer.ID

	Block OpaqueBlock
	Err   error
}

type BlockDownloadApplyResult struct {
	SerTopology string
	Topology    types.BlockTopology
	PeerID      peer.ID

	Err error
}

type BlockDownloadManagerInterface interface {
	// RequestDownload is called by the BlockDownloadManager to request a download to begin.
	//
	// The implementation should use a goroutine to handle any blocking operations.
	// When the download result is available, the implementation should send it to DownloadResponseChan().
	RequestDownload(BlockDownloadRequest)

	// ApplyBlock is called by the BlockDownloadManager to request a block to be applied.
	//
	// The implementation should use a goroutine to handle any blocking operations.
	// When the block succeeds or fails to apply, the implementation should send it to ApplyBlockResultChan().
	ApplyBlock(BlockDownloadResponse)

	// MyBlockTopologyChan values are supplied by the user to inform the BlockDownloadManager of
	// blocks that have been successfully applied to the local node.
	//
	// - RequestDownload / ApplyBlock operations issued by the BlockDownloadManager for the new block are not cancelled.
	// - If the new block extends the height of the longest fork, the height range requested from peers is expanded.
	MyBlockTopologyChan() <-chan types.BlockTopology

	// MyLastIrrChan values are supplied by the user to inform the BlockDownloadManager of
	// changes to the local node's last irreversible block (LIB).
	//
	// - RequestDownload / ApplyBlock operations issued by the BlockDownloadManager
	//   for blocks orphaned by the LIB advancement are not cancelled.
	// - The height range requested from peers is shrunken accordingly.
	MyLastIrrChan() <-chan types.BlockTopology

	// PeerHasBlockChan values are supplied by the user to inform the BlockDownloadManager of blocks that are
	// available from a peer.
	//
	// Normally the user would give this channel to each peer's PeerHandler.
	PeerHasBlockChan() <-chan PeerHasBlock

	// DownloadResponseChan values are supplied by the user to inform the BlockDownloadManager when
	// a download is complete.
	//
	// - If the download is already present in MyBlockTopology, it is discarded.
	//
	// - If the download connects directly to a block in my topology,
	//   the BlockDownloadManager will immediately call ApplyBlock().
	//
	// - If the download connects indirectly to a block in my topology,
	//   the BlockDownloadManager will call ApplyBlock() when / if the dependency is resolved
	//   by a successful ApplyBlock() for the dependency.
	//
	// - If the download doesn't connect directly or indirectly to my topology, it is discarded.
	DownloadResponseChan() <-chan BlockDownloadResponse

	// ApplyBlockResultChan values are supplied by the user to inform the BlockDownloadManager when
	// an attempt to apply a block is complete (either successfully or an error occurred).
	//
	// - If the block failed with a CorruptBlockError, the peer gave us an incorrect byte sequence for the
	//   claimed block ID.  The BlockDownloadManager will attempt to download the block from other peers.
	// - If the block failed with any other error, the block was valid but failed for semantic reasons.
	//   The BlockDownloadManager will add the block to its blacklist (TODO).
	ApplyBlockResultChan() <-chan BlockDownloadApply

	// RescanChan values are supplied to cause a rescan to initiate new download requests.
	//
	// If the supplied bool is true, the rescan is forced.
	// Otherwise the rescan is only performed if "dirty".
	//
	// The caller should occasionally perform a forced rescan (once every 5-30 seconds) and
	// frequently perform a dirty-only rescan (once every 100-1000 milliseconds).
	RescanChan() <-chan bool
}

type BlockDownloadManager struct {
	MyTopoCache    MyTopologyCache
	TopoCache      TopologyCache
	Downloading    map[string]BlockDownloadRequest
	Applying       map[string]BlockDownloadResponse
	WaitingToApply map[string]BlockDownloadResponse

	MaxDownloadsInFlight int
	MaxDownloadDepth     int

	needRescan bool
	rng        rand.Rand
	iface      BlockDownloadManagerInterface
}

func (*BlockDownloadManager) NewBlockDownloadManager(rng *rand.Rand, iface BlockDownloadManagerInterface) {
	man := BlockDownloadManager{
		MyTopoCache:    NewMyTopologyCache(),
		TopoCache:      NewTopologyCache(),
		Downloading:    make(map[string]BlockDownloadRequest),
		Applying:       make(map[string]BlockDownloadResponse),
		WaitingToApply: make(map[string]BlockDownloadResponse),

		MaxDownloadsInFlight: defaultMaxDownloadsInFlight,
		MaxDownloadDepth:     defaultMaxDownloadDepth,
		needRescan:           false,
		rng:                  rand.Rand,
		iface:                iface,
	}
	return &man
}

func (*BlockDownloadManager) Start(ctx context.Context) {
	go man.downloadManagerLoop(ctx)
}

func (m *BlockDownloadManager) handleDownloadResponse(resp BlockDownloadResponse) {
	_, hasDownloading := m.Downloading[resp.SerTopology]
	if !hasDownloading {
		log.Printf("Got BlockDownloadResponse for block %v from peer %v, but it was unexpectedly not tracked in the Downloading map\n",
			resp.Topology.ID, resp.PeerID)
	} else {
		delete(m.Downloading, resp.SerTopology)
	}

	alreadyApplying, hasAlreadyApplying := m.Applying[resp.SerTopology]
	if hasAlreadyApplying {
		log.Printf("Discarded block response for block %v from peer %v:  Already applying from peer %v\n",
			resp.Topology.ID, resp.PeerID, alreadyApplying.PeerID)
		return
	}
	alreadyWaiting, hasAlreadyWaiting := m.WaitingToApply
	if hasAlreadyWaiting {
		log.Printf("Discarded block response for block %v from peer %v:  Already waiting to apply from peer %v\n",
			resp.Topology.ID, resp.PeerID, alreadyWaiting.PeerID)
		return
	}

	_, hasPrev := m.MyTopoCache.ByID[resp.Topology.Previous]
	if hasPrev {
		m.Applying[resp.SerTopology] = resp
		m.iface.ApplyBlock(resp)
	} else {
		m.WaitingToApply[resp.SerTopology] = resp
	}
}

func (m *BlockDownloadManager) handleApplyBlockResult(applyResult ApplyBlockResult) {
	if applyResult.Err == nil {
		// Success.
		// Do nothing, subsequent waiting blocks will be activated by MyBlockTopologyChan message.
		return
	}

	// Failure.
	// TODO:  Handle block that fails to apply.
}

func (m *BlockDownloadManager) startDownload(download string) {
	// If the download's already gotten in, no-op
	_, isDownloading := m.Downloading[download]
	if isDownloading {
		return
	}
	_, isApplying := m.Applying[download]
	if isApplying {
		return
	}
	_, isWaiting := m.WaitingToApply[download]
	if isWaiting {
		return
	}

	topo, hasTopo := m.TopoCache.DeserCache[download]
	if !hasTopo {
		log.Printf("Was not able to get topology for download %v\n", download)
		return
	}

	// Pick a peer that has the download
	peer, err := m.TopoCache.PickPeer(download, m.rng)
	if err {
		log.Printf("Got an error trying to pick a peer to download block %v\n", topo.ID)
		return
	}

	req := BlockDownloadRequest{
		SerTopology: download,
		Topology:    topo,
		PeerID:      peer,
	}

	m.Downloading[download] = req
	iface.RequestDownload(req)
}

func (m *BlockDownloadManager) rescan() {
	// Figure out the blocks we'd ideally be downloading
	downloadList := GetDownloads(&m.MyTopoCache, &m.TopoCache, m.MaxDownloadsInFlight, m.MaxDownloadDepth)

	for _, download := range downloadList {
		// If we can't support additional downloads, bail
		if len(m.Downloading)+len(m.Applying)+len(m.WaitingToApply) >= m.MaxDownloadsInFlight {
			break
		}

		m.startDownload(download)
	}

	// TODO:  Expire obsolete entries (behind LIB)
}

func (m *BlockDownloadManager) downloadManagerLoop(ctx context.Context) {
	m.needRescan = false
	for {
		select {
		case forcedRescan := <-m.iface.RescanChan():
			if forcedRescan || m.needRescan {
				m.rescan()
				m.needRescan = false
			}
		case newMyTopo := <-m.iface.MyBlockTopologyChan():
			m.MyTopoCache.Add(newMyTopo)
			m.needRescan = true
		case newMyLastIrr := <-m.iface.MyLastIrrChan():
			m.MyTopoCache.SetLastIrr(newMyLastIrr)
			m.TopoCache.SetLastIrr(newMyLastIrr)
			m.needRescan = true
		case peerHasBlock := <-m.iface.PeerHasBlockChan():
			m.TopoCache.Add(peerHasBlock.PeerID)
			m.needRescan = true
		case downloadResponse := <-m.iface.DownloadResponseChan():
			m.handleDownloadResponse(downloadResponse)
		case applyBlockResult := <-m.iface.ApplyBlockResultChan():
			m.handleApplyBlockResult(applyBlockResult)
		case <-ctx.Done():
			return
		}
	}
}
