package protocol

import (
	"context"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/options"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
)

// BlockDownloadRequest represents a block download request.
//
// It contains a list of peers that are known to have the request, and a single peer to download from.
type BlockDownloadRequest struct {
	Topology util.BlockTopologyCmp
	PeerID   peer.ID
}

// BlockDownloadResponse represents a peer's response to a BlockDownloadRequest.
//
// The response can be an Err.
// The response is not yet applied.
//
type BlockDownloadResponse struct {
	Topology util.BlockTopologyCmp
	PeerID   peer.ID

	Block types.OptionalBlock
	Err   error
}

// BlockDownloadApplyResult represents the result of an attempt to apply a block
type BlockDownloadApplyResult struct {
	Topology util.BlockTopologyCmp
	PeerID   peer.ID

	Ok  bool
	Err error
}

// PeerIsContemporary is a message that specifies a peer is / is not contemporary.
type PeerIsContemporary struct {
	PeerID         peer.ID
	IsContemporary bool
}

// PeerIsClosed is a message that specifies a peer connection has been closed
type PeerIsClosed struct {
	PeerID peer.ID
}

// BlockDownloadManagerInterface is an abstraction of the methods a BlockDownloadManager should contain
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

	// EnableGossip is called by the BlockDownloadManager to request gossip mode to be enabled / disabled.
	//
	// The implementation should use a goroutine to handle any blocking operations.
	// Any goroutines launched should respect the passed-in context.
	EnableGossip(context.Context, bool)

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

	// PeerIsContemporaryChan values are supplied by the user to inform the BlockDownloadManager if the peer is contemporary.
	// Contemporary peers are peers that have nearby heads and can participate in gossip.
	//
	// Normally the user would give this channel to each peer's PeerHandler.
	PeerIsContemporaryChan() <-chan PeerIsContemporary

	// PeerIsClosed values are supplied by the user to inform the BlockDownloadManager when a
	// peer connection has been closed.
	//
	// Normally, the user would give this channel to each peer's PeerHandler.
	PeerIsClosedChan() <-chan PeerIsClosed

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

// BlockDownloadManager handles downloads
type BlockDownloadManager struct {
	MyTopoCache    LocalTopologyCache
	TopoCache      NetTopologyCache
	Downloading    map[util.BlockTopologyCmp]BlockDownloadRequest
	Applying       map[util.BlockTopologyCmp]BlockDownloadResponse
	WaitingToApply map[util.BlockTopologyCmp]BlockDownloadResponse
	Options        options.DownloadManagerOptions
	GossipVoter    GossipVoter

	needRescan bool
	rng        *rand.Rand
	iface      BlockDownloadManagerInterface
}

// NewBlockDownloadResponse creates a new instance of BlockDownloadResponse
func NewBlockDownloadResponse() *BlockDownloadResponse {
	// It is okay to default-initialize all fields except Block
	block := types.NewOptionalBlock()
	resp := BlockDownloadResponse{
		Block: *block,
	}
	return &resp
}

// NewBlockDownloadManager creates a new instance of BlockDownloadManager
func NewBlockDownloadManager(rng *rand.Rand, iface BlockDownloadManagerInterface, opt options.DownloadManagerOptions) *BlockDownloadManager {
	man := BlockDownloadManager{
		MyTopoCache:    *NewLocalTopologyCache(),
		TopoCache:      *NewNetTopologyCache(),
		Downloading:    make(map[util.BlockTopologyCmp]BlockDownloadRequest),
		Applying:       make(map[util.BlockTopologyCmp]BlockDownloadResponse),
		WaitingToApply: make(map[util.BlockTopologyCmp]BlockDownloadResponse),
		Options:        opt,
		GossipVoter:    *NewGossipVoter(opt.GossipDisableBp, opt.GossipEnableBp, opt.GossipAlwaysDisable, opt.GossipAlwaysEnable),

		needRescan: false,
		rng:        rng,
		iface:      iface,
	}
	return &man
}

// Start starts the download manager
func (m *BlockDownloadManager) Start(ctx context.Context) {
	go m.downloadManagerLoop(ctx)
}

func (m *BlockDownloadManager) maybeApplyBlock(ctx context.Context, resp BlockDownloadResponse) {
	_, isAlreadyApplying := m.Applying[resp.Topology]
	if isAlreadyApplying {
		log.Debugf("maybeApplyBlock() could not apply block %s from peer %s", util.BlockTopologyCmpString(&resp.Topology), resp.PeerID)
		return
	}

	var hasPrev bool
	if resp.Topology.Height == 1 {
		hasPrev = true
	} else {
		_, hasPrev = m.MyTopoCache.ByID(resp.Topology.Previous)
	}

	if hasPrev {
		log.Debugf("maybeApplyBlock() entering hasPrev case for block %s from peer %s", util.BlockTopologyCmpString(&resp.Topology), resp.PeerID)
		delete(m.Downloading, resp.Topology)
		delete(m.WaitingToApply, resp.Topology)
		m.Applying[resp.Topology] = resp
		m.iface.ApplyBlock(ctx, resp)
	} else {
		log.Debugf("maybeApplyBlock() entering !hasPrev case for block %s from peer %s", util.BlockTopologyCmpString(&resp.Topology), resp.PeerID)

		delete(m.Downloading, resp.Topology)
		m.WaitingToApply[resp.Topology] = resp
	}
}

func (m *BlockDownloadManager) handleDownloadResponse(ctx context.Context, resp BlockDownloadResponse) {
	log.Debugf("Got BlockDownloadResponse for block %s from peer %s", util.BlockTopologyCmpString(&resp.Topology), resp.PeerID)
	_, hasDownloading := m.Downloading[resp.Topology]
	if !hasDownloading {
		log.Warnf("Got BlockDownloadResponse for block %s from peer %s, but it was unexpectedly not tracked in the Downloading map",
			util.BlockTopologyCmpString(&resp.Topology), resp.PeerID)
	} else {
		delete(m.Downloading, resp.Topology)
	}

	if resp.Err != nil {
		log.Infof("Error downloading block %s from peer %s: %s", util.BlockTopologyCmpString(&resp.Topology), resp.PeerID, resp.Err)
		return
	}

	alreadyApplying, hasAlreadyApplying := m.Applying[resp.Topology]
	if hasAlreadyApplying {
		log.Warnf("Discarded block response for block %s from peer %s:  Already applying from peer %s",
			util.BlockTopologyCmpString(&resp.Topology), resp.PeerID, alreadyApplying.PeerID)
		return
	}
	alreadyWaiting, hasAlreadyWaiting := m.WaitingToApply[resp.Topology]
	if hasAlreadyWaiting {
		log.Warnf("Discarded block response for block %s from peer %s:  Already waiting to apply from peer %s",
			util.BlockTopologyCmpString(&resp.Topology), resp.PeerID, alreadyWaiting.PeerID)
		return
	}

	m.maybeApplyBlock(ctx, resp)
}

func (m *BlockDownloadManager) handleApplyBlockResult(applyResult BlockDownloadApplyResult) {

	delete(m.Applying, applyResult.Topology)

	if applyResult.Err == nil {
		//
		// Success.
		//
		// Advance the fork head as in MyBlockTopologyChan case, but don't trigger a rescan.
		// Even if we do nothing, subsequent waiting blocks will be activated by MyBlockTopologyChan message,
		// but this may occur at a limited rate (especially considering the polling implementation).
		//

		// Since we're not triggering a rescan based on whether it was added or not, we ignore the result of Add()
		m.MyTopoCache.Add(applyResult.Topology)

		return
	}

	// Failure.
	// TODO:  Handle block that fails to apply.
	log.Infof("Error applying block %s", applyResult.Err)
}

// ConvertPeerSetToSlice converts a set (a map from PeerCmp to void) to a slice.
//
func ConvertPeerSetToSlice(m map[peer.ID]util.Void) []peer.ID {
	result := make([]peer.ID, len(m))

	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	return result
}

func (m *BlockDownloadManager) startDownload(ctx context.Context, download util.BlockTopologyCmp) {
	log.Debugf("startDownload() on block %s", util.BlockTopologyCmpString(&download))

	// If the download's already gotten in, no-op
	_, isDownloading := m.Downloading[download]
	if isDownloading {
		log.Debugf("  - Bail, already downloading %s", util.BlockTopologyCmpString(&download))
		return
	}
	_, isApplying := m.Applying[download]
	if isApplying {
		log.Debugf("  - Bail, already applying %s", util.BlockTopologyCmpString(&download))
		return
	}
	waitingResp, isWaiting := m.WaitingToApply[download]
	if isWaiting {
		m.maybeApplyBlock(ctx, waitingResp)
		log.Debugf("  - Bail, already waiting to apply %s", util.BlockTopologyCmpString(&download))
		return
	}

	peers, hasPeers := m.TopoCache.ByTopology(download)
	if (!hasPeers) || (len(peers) < 1) {
		log.Warnf("Could not find download %s in TopoCache", util.BlockTopologyCmpString(&download))
		return
	}

	// Pick a peer that has the download
	// TODO:  Add constraint to bound the number of in-flight downloads sent to a single peer
	peer, err := m.TopoCache.PickPeer(download, m.rng)
	if err != nil {
		log.Warnf("Got an error trying to pick a peer to download block %s", util.BlockTopologyCmpString(&download))
		return
	}

	req := BlockDownloadRequest{
		Topology: download,
		PeerID:   peer,
	}

	log.Debugf("  - Downloading block %s from peer %s", util.BlockTopologyCmpString(&download), req.PeerID)
	m.Downloading[download] = req
	m.iface.RequestDownload(ctx, req)
}

func (m *BlockDownloadManager) rescan(ctx context.Context) {
	//log.Debug("Rescanning downloads")

	for _, resp := range m.WaitingToApply {
		m.maybeApplyBlock(ctx, resp)
	}

	// Figure out the blocks we'd ideally be downloading
	downloadList := GetDownloads(&m.MyTopoCache, &m.TopoCache, m.Options.MaxDownloadsInFlight, m.Options.MaxDownloadDepth)
	//log.Debugf("GetDownloads() suggests %d eligible downloads", len(downloadList))

	for _, download := range downloadList {
		// If we can't support additional downloads, bail
		if len(m.Downloading)+len(m.Applying)+len(m.WaitingToApply) >= m.Options.MaxDownloadsInFlight {
			log.Debugf("No more downloads will be initiated, as this would exceed %d in-flight downloads", m.Options.MaxDownloadsInFlight)
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
			added := m.MyTopoCache.Add(util.BlockTopologyToCmp(newMyTopo))
			m.needRescan = m.needRescan || added
		case newMyLastIrr := <-m.iface.MyLastIrrChan():
			c := util.BlockTopologyToCmp(newMyLastIrr)
			m.MyTopoCache.SetLastIrr(c)
			m.TopoCache.SetLastIrr(c)
			m.needRescan = true
		case peerHasBlock := <-m.iface.PeerHasBlockChan():
			log.Debugf("%v: Service PeerHasBlock message %s", peerHasBlock.PeerID, util.BlockTopologyCmpString(&peerHasBlock.Block))
			added := m.TopoCache.Add(peerHasBlock)
			m.needRescan = m.needRescan || added
		case peerIsContemporary := <-m.iface.PeerIsContemporaryChan():
			log.Debugf("%v: Service PeerIsContemporary message %s", peerIsContemporary.PeerID, peerIsContemporary.IsContemporary)
			// TODO:  Remove disconnected peers from GossipVoter, topoCache
			oldFlag := m.GossipVoter.EnableGossip
			m.GossipVoter.Vote(peerIsContemporary)
			if m.GossipVoter.EnableGossip != oldFlag {
				log.Infof("EnableGossip set to %s", m.GossipVoter.EnableGossip)
				m.iface.EnableGossip(ctx, m.GossipVoter.EnableGossip)
			}
		case peerIsClosed := <-m.iface.PeerIsClosedChan():
			m.TopoCache.RemovePeer(peerIsClosed.PeerID)
		case downloadResponse := <-m.iface.DownloadResponseChan():
			m.handleDownloadResponse(ctx, downloadResponse)
		case applyBlockResult := <-m.iface.ApplyBlockResultChan():
			m.handleApplyBlockResult(applyBlockResult)
		case <-ctx.Done():
			return
		}
	}
}
