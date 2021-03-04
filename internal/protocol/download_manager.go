package protocol

import (
	"context"
	"log"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"

	types "github.com/koinos/koinos-types-golang"
)

const (
	defaultMaxDownloadsInFlight int = 8
	defaultMaxDownloadDepth     int = 3
)

// BlockDownloadRequest represents a block download request that has been issued to a peer.
type BlockDownloadRequest struct {
	Topology BlockTopologyCmp
	PeerID   peer.ID
}

// BlockDownloadResponse represents a peer's response to a BlockDownloadRequest.
//
// The response can be an Err.
// The response is not yet applied.
//
type BlockDownloadResponse struct {
	Topology BlockTopologyCmp
	PeerID   peer.ID

	Block types.OpaqueBlock
	Err   error
}

type BlockDownloadApplyResult struct {
	Topology BlockTopologyCmp
	PeerID   peer.ID

	Err error
}

type BlockDownloadManagerInterface interface {
	// RequestDownload is called by the BlockDownloadManager to request a download to begin.
	//
	// The implementation should use a goroutine to handle any blocking operations.
	// Any goroutines launched should respect the passed-in context.
	// When the download result is available, the implementation should send it to DownloadResponseChan().
	RequestDownload(context.Context, BlockDownloadRequest)

	// ApplyBlock is called by the BlockDownloadManager to request a block to be applied.
	//
	// The implementation should use a goroutine to handle any blocking operations.
	// Any goroutines launched should respect the passed-in context.
	// When the block succeeds or fails to apply, the implementation should send it to ApplyBlockResultChan().
	ApplyBlock(context.Context, BlockDownloadResponse)

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
	ApplyBlockResultChan() <-chan BlockDownloadApplyResult

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
	Downloading    map[BlockTopologyCmp]BlockDownloadRequest
	Applying       map[BlockTopologyCmp]BlockDownloadResponse
	WaitingToApply map[BlockTopologyCmp]BlockDownloadResponse

	MaxDownloadsInFlight int
	MaxDownloadDepth     int

	needRescan bool
	rng        *rand.Rand
	iface      BlockDownloadManagerInterface
}

func NewBlockDownloadManager(rng *rand.Rand, iface BlockDownloadManagerInterface) *BlockDownloadManager {
	man := BlockDownloadManager{
		MyTopoCache:    *NewMyTopologyCache(),
		TopoCache:      *NewTopologyCache(),
		Downloading:    make(map[BlockTopologyCmp]BlockDownloadRequest),
		Applying:       make(map[BlockTopologyCmp]BlockDownloadResponse),
		WaitingToApply: make(map[BlockTopologyCmp]BlockDownloadResponse),

		MaxDownloadsInFlight: defaultMaxDownloadsInFlight,
		MaxDownloadDepth:     defaultMaxDownloadDepth,
		needRescan:           false,
		rng:                  rng,
		iface:                iface,
	}
	return &man
}

func (m *BlockDownloadManager) Start(ctx context.Context) {
	go m.downloadManagerLoop(ctx)
}

func (m *BlockDownloadManager) handleDownloadResponse(ctx context.Context, resp BlockDownloadResponse) {
	_, hasDownloading := m.Downloading[resp.Topology]
	if !hasDownloading {
		log.Printf("Got BlockDownloadResponse for block %v from peer %v, but it was unexpectedly not tracked in the Downloading map\n",
			resp.Topology.ID, resp.PeerID)
	} else {
		delete(m.Downloading, resp.Topology)
	}

	alreadyApplying, hasAlreadyApplying := m.Applying[resp.Topology]
	if hasAlreadyApplying {
		log.Printf("Discarded block response for block %v from peer %v:  Already applying from peer %v\n",
			resp.Topology.ID, resp.PeerID, alreadyApplying.PeerID)
		return
	}
	alreadyWaiting, hasAlreadyWaiting := m.WaitingToApply[resp.Topology]
	if hasAlreadyWaiting {
		log.Printf("Discarded block response for block %v from peer %v:  Already waiting to apply from peer %v\n",
			resp.Topology.ID, resp.PeerID, alreadyWaiting.PeerID)
		return
	}

	_, hasPrev := m.MyTopoCache.ByID[resp.Topology.Previous]
	if hasPrev {
		m.Applying[resp.Topology] = resp
		m.iface.ApplyBlock(ctx, resp)
	} else {
		m.WaitingToApply[resp.Topology] = resp
	}
}

func (m *BlockDownloadManager) handleApplyBlockResult(applyResult BlockDownloadApplyResult) {
	if applyResult.Err == nil {
		// Success.
		// Do nothing, subsequent waiting blocks will be activated by MyBlockTopologyChan message.
		return
	}

	// Failure.
	// TODO:  Handle block that fails to apply.
}

func (m *BlockDownloadManager) startDownload(ctx context.Context, download BlockTopologyCmp) {
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

	_, hasTopo := m.TopoCache.ByTopology[download]
	if !hasTopo {
		log.Printf("Could not find download %v in TopoCache\n", download)
		return
	}

	// Pick a peer that has the download
	// TODO:  Add constraint to bound the number of in-flight downloads sent to a single peer
	peer, err := m.TopoCache.PickPeer(download, m.rng)
	if err != nil {
		log.Printf("Got an error trying to pick a peer to download block %v\n", download.ID)
		return
	}

	req := BlockDownloadRequest{
		Topology: download,
		PeerID:   peer,
	}

	m.Downloading[download] = req
	m.iface.RequestDownload(ctx, req)
}

func (m *BlockDownloadManager) rescan(ctx context.Context) {
	// Figure out the blocks we'd ideally be downloading
	downloadList := GetDownloads(&m.MyTopoCache, &m.TopoCache, m.MaxDownloadsInFlight, m.MaxDownloadDepth)

	for _, download := range downloadList {
		// If we can't support additional downloads, bail
		if len(m.Downloading)+len(m.Applying)+len(m.WaitingToApply) >= m.MaxDownloadsInFlight {
			break
		}

		m.startDownload(ctx, download)
	}

	// TODO:  Expire obsolete entries (behind LIB)
}

func (m *BlockDownloadManager) downloadManagerLoop(ctx context.Context) {
	m.needRescan = false
	for {
		select {
		case forcedRescan := <-m.iface.RescanChan():
			if forcedRescan || m.needRescan {
				m.rescan(ctx)
				m.needRescan = false
			}
		case newMyTopo := <-m.iface.MyBlockTopologyChan():
			m.MyTopoCache.Add(BlockTopologyToCmp(newMyTopo))
			m.needRescan = true
		case newMyLastIrr := <-m.iface.MyLastIrrChan():
			c := BlockTopologyToCmp(newMyLastIrr)
			m.MyTopoCache.SetLastIrr(c)
			m.TopoCache.SetLastIrr(c)
			m.needRescan = true
		case peerHasBlock := <-m.iface.PeerHasBlockChan():
			m.TopoCache.Add(peerHasBlock)
			m.needRescan = true
		case downloadResponse := <-m.iface.DownloadResponseChan():
			m.handleDownloadResponse(ctx, downloadResponse)
		case applyBlockResult := <-m.iface.ApplyBlockResultChan():
			m.handleApplyBlockResult(applyBlockResult)
		case <-ctx.Done():
			return
		}
	}
}
