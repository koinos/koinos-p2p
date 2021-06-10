package protocol

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"

	log "github.com/koinos/koinos-log-golang"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
)

// PeerHasBlock is a message that specifies a peer has a block with the given topology.
//
type PeerHasBlock struct {
	PeerID peer.ID
	Block  util.BlockTopologyCmp
}

// MyTopologyCache holds my topology (i.e. the topology of a single node).
type MyTopologyCache struct {
	ByTopology  map[util.BlockTopologyCmp]util.Void
	ByID        map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	ByPrevious  map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	ByHeight    map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void
	OldestBlock types.BlockHeightType
}

// NewMyTopologyCache instantiates a new MyTopologyCache
func NewMyTopologyCache() *MyTopologyCache {
	return &MyTopologyCache{
		ByTopology:  make(map[util.BlockTopologyCmp]util.Void),
		ByID:        make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		ByPrevious:  make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		ByHeight:    make(map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void),
		OldestBlock: ^types.BlockHeightType(0),
	}
}

// Add adds the given BlockTopology to the cache
func (c *MyTopologyCache) Add(block util.BlockTopologyCmp) bool {
	_, hasBlock := c.ByTopology[block]
	if hasBlock {
		return false
	}
	c.ByTopology[block] = util.Void{}

	{
		m, hasM := c.ByID[block.ID]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.ByID[block.ID] = m
		}
		m[block] = util.Void{}
	}

	{
		m, hasM := c.ByPrevious[block.Previous]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.ByPrevious[block.Previous] = m
		}
		m[block] = util.Void{}
	}

	{
		m, hasM := c.ByHeight[block.Height]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.ByHeight[block.Height] = m
		}
		m[block] = util.Void{}
		if block.Height < c.OldestBlock {
			c.OldestBlock = block.Height
		}
	}

	return true
}

// SetLastIrr sets the last irreversible block
func (c *MyTopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	for c.OldestBlock < newMyLastIrr.Height {
		if topos, ok := c.ByHeight[c.OldestBlock]; ok {
			for topo := range topos {
				// Remove from ByTopology
				delete(c.ByTopology, topo)

				// Remove from ByID
				if blocksByID, ok := c.ByID[topo.ID]; ok {
					delete(blocksByID, topo)

					if len(blocksByID) == 0 {
						delete(c.ByID, topo.ID)
					}
				}

				// Remove from ByPrevious
				if blocksByPrevious, ok := c.ByPrevious[topo.Previous]; ok {
					delete(blocksByPrevious, topo)

					if len(blocksByPrevious) == 0 {
						delete(c.ByPrevious, topo.Previous)
					}
				}
			}

			// Remove from ByHeight
			delete(c.ByHeight, c.OldestBlock)
		}

		c.OldestBlock++
	}
}

// TopologyCache holds the topology of all peers.
// TODO rename to NetTopologyCache?
type TopologyCache struct {
	Set         map[PeerHasBlock]util.Void
	ByTopology  map[util.BlockTopologyCmp]map[peer.ID]util.Void
	ByPrevious  map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void
	ByHeight    map[types.BlockHeightType]map[PeerHasBlock]util.Void
	ByPeer      map[peer.ID]map[PeerHasBlock]util.Void
	PeerHeight  map[peer.ID]types.BlockHeightType
	OldestBlock types.BlockHeightType
}

// NewTopologyCache instantiates a new TopologyCache
func NewTopologyCache() *TopologyCache {
	return &TopologyCache{
		Set:         make(map[PeerHasBlock]util.Void),
		ByTopology:  make(map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		ByPrevious:  make(map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		ByHeight:    make(map[types.BlockHeightType]map[PeerHasBlock]util.Void),
		ByPeer:      make(map[peer.ID]map[PeerHasBlock]util.Void),
		PeerHeight:  make(map[peer.ID]types.BlockHeightType),
		OldestBlock: ^types.BlockHeightType(0),
	}
}

// Add adds a known block held by a peer
func (c *TopologyCache) Add(peerHasBlock PeerHasBlock) bool {
	_, hasBlock := c.Set[peerHasBlock]
	if hasBlock {
		return false
	}
	c.Set[peerHasBlock] = util.Void{}

	{
		m, hasM := c.ByTopology[peerHasBlock.Block]
		if !hasM {
			m = make(map[peer.ID]util.Void)
			c.ByTopology[peerHasBlock.Block] = m
		}
		m[peerHasBlock.PeerID] = util.Void{}
	}

	{
		m, hasM := c.ByPrevious[peerHasBlock.Block.Previous]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]map[peer.ID]util.Void)
			c.ByPrevious[peerHasBlock.Block.Previous] = m
		}
		m2, hasM2 := m[peerHasBlock.Block]
		if !hasM2 {
			m2 = make(map[peer.ID]util.Void)
			m[peerHasBlock.Block] = m2
		}
		m2[peerHasBlock.PeerID] = util.Void{}
	}

	{
		m, hasM := c.ByHeight[peerHasBlock.Block.Height]
		if !hasM {
			m = make(map[PeerHasBlock]util.Void)
			c.ByHeight[peerHasBlock.Block.Height] = m
		}
		m[peerHasBlock] = util.Void{}
		if peerHasBlock.Block.Height < c.OldestBlock {
			c.OldestBlock = peerHasBlock.Block.Height
		}
	}

	{
		m, hasM := c.ByPeer[peerHasBlock.PeerID]
		if !hasM {
			m = make(map[PeerHasBlock]util.Void)
			c.ByPeer[peerHasBlock.PeerID] = m
		}
		m[peerHasBlock] = util.Void{}
	}

	{
		height, hasHeight := c.PeerHeight[peerHasBlock.PeerID]
		if (!hasHeight) || (height < peerHasBlock.Block.Height) {
			c.PeerHeight[peerHasBlock.PeerID] = peerHasBlock.Block.Height
		}
	}

	return true
}

// PickPeer chooses a random peer
func (c *TopologyCache) PickPeer(topo util.BlockTopologyCmp, rng *rand.Rand) (peer.ID, error) {
	var emptyPeerID peer.ID

	peers, hasPeers := c.ByTopology[topo]
	if !hasPeers {
		return emptyPeerID, errors.New("Attempt to log with no peers")
	}
	if len(peers) < 1 {
		return emptyPeerID, errors.New("Cannot pick from empty peer list")
	}
	pickIndex := rng.Intn(len(peers))

	// O(n) scan to pick peer, is there a way to speed this up?
	i := 0
	for peerID := range peers {
		if i == pickIndex {
			return peerID, nil
		}
		i++
	}
	return emptyPeerID, fmt.Errorf("Could not pick the %dth element of map of length %d", pickIndex, len(peers))
}

// SetLastIrr sets the last irreversible block
func (c *TopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	for c.OldestBlock < newMyLastIrr.Height {
		// Each block needs to be checked incrementally because removal of peers can
		// create gaps in the topology cache
		if peerBlocks, ok := c.ByHeight[c.OldestBlock]; ok {

			for peerBlock := range peerBlocks {
				// Remove from Set
				delete(c.Set, peerBlock)

				// Remove from ByTopology
				delete(c.ByTopology, peerBlock.Block)

				// Remove from ByPrevious
				if blocksByPrevious, ok := c.ByPrevious[peerBlock.Block.Previous]; ok {
					delete(blocksByPrevious, peerBlock.Block)

					if len(blocksByPrevious) == 0 {
						delete(c.ByPrevious, peerBlock.Block.Previous)
					}
				}

				// Remove from ByPeer
				if blockByPeer, ok := c.ByPeer[peerBlock.PeerID]; ok {
					delete(blockByPeer, peerBlock)

					if len(blockByPeer) == 0 {
						delete(c.ByPeer, peerBlock.PeerID)
					}
				}
			}

			// Remove from ByHeight
			delete(c.ByHeight, c.OldestBlock)
		}

		c.OldestBlock++
	}
}

// RemovePeer removes peer's blocks from the cache
func (c *TopologyCache) RemovePeer(pid peer.ID) {
	if peerBlocks, ok := c.ByPeer[pid]; ok {
		for peerBlock := range peerBlocks {
			// Remove from Set
			delete(c.Set, peerBlock)

			// Remove from ByTopology
			if topologyPeers, ok := c.ByTopology[peerBlock.Block]; ok {
				delete(topologyPeers, pid)

				if len(topologyPeers) == 0 {
					delete(c.ByTopology, peerBlock.Block)
				}
			}

			// Remove from ByPrevious
			if blocksByPrevious, ok := c.ByPrevious[peerBlock.Block.Previous]; ok {
				if topologyPeers, ok := blocksByPrevious[peerBlock.Block]; ok {
					delete(topologyPeers, pid)

					if len(topologyPeers) == 0 {
						delete(blocksByPrevious, peerBlock.Block)
					}
				}

				if len(blocksByPrevious) == 0 {
					delete(c.ByPrevious, peerBlock.Block.Previous)
				}
			}

			// Remove from ByHeight
			if heightPeers, ok := c.ByHeight[peerBlock.Block.Height]; ok {
				delete(heightPeers, peerBlock)

				if len(heightPeers) == 0 {
					delete(c.ByHeight, peerBlock.Block.Height)
				}
			}
		}

		// Remove from ByPeer
		delete(c.ByPeer, pid)

		// Remove from PeerHeight
		delete(c.PeerHeight, pid)
	}
}

// GetInitialDownload returns the initial download from a topology.
//
// The initial download is the set of blocks in netTopo that directly connect to myTopo but are
// not themselves in myTopo.
func GetInitialDownload(myTopo *MyTopologyCache, netTopo *TopologyCache) map[util.BlockTopologyCmp]util.Void {
	result := make(map[util.BlockTopologyCmp]util.Void)

	// Special case:  A node that has no blocks is interested in blocks of height 1
	if len(myTopo.ByTopology) == 0 {
		log.Debug("No blocks, so GetInitialDownload is looking for blocks of height 1")
		netNextBlocks, ok := netTopo.ByHeight[1]
		if ok {
			for nextBlock := range netNextBlocks {
				result[nextBlock.Block] = util.Void{}
			}
		}

		log.Debugf("GetInitialDownload() returned %d blocks", len(result))
		return result
	}

	for block := range myTopo.ByTopology {
		netNextBlocks, ok := netTopo.ByPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock := range netNextBlocks {
			// Skip blocks we already have
			_, myHasNextBlock := myTopo.ByTopology[nextBlock]
			if myHasNextBlock {
				continue
			}
			result[nextBlock] = util.Void{}
		}
	}

	//log.Debugf("GetInitialDownload() returned %d blocks", len(result))
	return result
}

// GetNextDownload returns the next download from a topology.
//
// The next download is the set of blocks in netTopo that directly connect to currentDownload.
func GetNextDownload(myTopo *MyTopologyCache, netTopo *TopologyCache, currentDownload map[util.BlockTopologyCmp]util.Void) map[util.BlockTopologyCmp]util.Void {
	result := make(map[util.BlockTopologyCmp]util.Void)
	for block := range currentDownload {
		netNextBlocks, ok := netTopo.ByPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock := range netNextBlocks {
			result[nextBlock] = util.Void{}
		}
	}
	return result
}

// ConvertBlockTopologySetToSlice converts a set (a map from BlockTopologyCmp to util.Void) to a slice.
//
// Only the first n elements are converted.
func ConvertBlockTopologySetToSlice(m map[util.BlockTopologyCmp]util.Void) []util.BlockTopologyCmp {
	result := make([]util.BlockTopologyCmp, len(m))

	i := 0
	for k := range m {
		result[i] = k
		i++
	}
	return result
}

// GetDownloads scans for a set of downloads that makes progress from the current topology.
//
// This function could likely be optimized by adding additional indexing.
func GetDownloads(myTopo *MyTopologyCache, netTopo *TopologyCache, maxCount int, maxDepth int) []util.BlockTopologyCmp {
	nextSet := GetInitialDownload(myTopo, netTopo)
	resultSet := make(map[util.BlockTopologyCmp]util.Void)

	// Set resultSet to the union of resultSet and nextSet
	for k := range nextSet {
		if len(resultSet) >= maxCount {
			return ConvertBlockTopologySetToSlice(resultSet)
		}
		resultSet[k] = util.Void{}
	}

	for depth := 1; depth <= maxDepth; depth++ {
		if len(resultSet) >= maxCount {
			return ConvertBlockTopologySetToSlice(resultSet)
		}

		// Set resultSet to the union of resultSet and nextSet
		for k := range nextSet {
			if len(resultSet) >= maxCount {
				return ConvertBlockTopologySetToSlice(resultSet)
			}
			resultSet[k] = util.Void{}
		}

		nextSet = GetNextDownload(myTopo, netTopo, resultSet)
		if len(nextSet) == 0 {
			return ConvertBlockTopologySetToSlice(resultSet)
		}
	}

	return ConvertBlockTopologySetToSlice(resultSet)
}
