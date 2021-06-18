package protocol

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

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

// LocalTopologyCache holds my topology (i.e. the topology of a single node).
type LocalTopologyCache struct {
	byTopology  map[util.BlockTopologyCmp]util.Void
	byID        map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	byPrevious  map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	byHeight    map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void
	oldestBlock types.BlockHeightType
	mutex       sync.Mutex
}

// ByTopology allows for thread safe access to the topology map
func (c *LocalTopologyCache) ByTopology(key util.BlockTopologyCmp) (util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byTopology[key]
	return a, b
}

// ByID allows for thread safe access to the ID map
func (c *LocalTopologyCache) ByID(key util.MultihashCmp) (map[util.BlockTopologyCmp]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byID[key]
	return a, b
}

// ByPrevious allows for thread safe access to the previous map
func (c *LocalTopologyCache) ByPrevious(key util.MultihashCmp) (map[util.BlockTopologyCmp]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byPrevious[key]
	return a, b
}

// ByHeight allows for thread safe access to the height map
func (c *LocalTopologyCache) ByHeight(key types.BlockHeightType) (map[util.BlockTopologyCmp]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byHeight[key]
	return a, b
}

// NewLocalTopologyCache instantiates a new LocalTopologyCache
func NewLocalTopologyCache() *LocalTopologyCache {
	return &LocalTopologyCache{
		byTopology:  make(map[util.BlockTopologyCmp]util.Void),
		byID:        make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		byPrevious:  make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		byHeight:    make(map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void),
		oldestBlock: ^types.BlockHeightType(0),
	}
}

// Add adds the given BlockTopology to the cache
func (c *LocalTopologyCache) Add(block util.BlockTopologyCmp) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, hasBlock := c.byTopology[block]
	if hasBlock {
		return false
	}
	c.byTopology[block] = util.Void{}

	{
		m, hasM := c.byID[block.ID]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.byID[block.ID] = m
		}
		m[block] = util.Void{}
	}

	{
		m, hasM := c.byPrevious[block.Previous]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.byPrevious[block.Previous] = m
		}
		m[block] = util.Void{}
	}

	{
		m, hasM := c.byHeight[block.Height]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]util.Void)
			c.byHeight[block.Height] = m
		}
		m[block] = util.Void{}
		if block.Height < c.oldestBlock {
			c.oldestBlock = block.Height
		}
	}

	return true
}

// SetLastIrr sets the last irreversible block
func (c *LocalTopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for c.oldestBlock < newMyLastIrr.Height {
		if topos, ok := c.byHeight[c.oldestBlock]; ok {
			for topo := range topos {
				// Remove from ByTopology
				delete(c.byTopology, topo)

				// Remove from ByID
				if blocksByID, ok := c.byID[topo.ID]; ok {
					delete(blocksByID, topo)

					if len(blocksByID) == 0 {
						delete(c.byID, topo.ID)
					}
				}

				// Remove from ByPrevious
				if blocksByPrevious, ok := c.byPrevious[topo.Previous]; ok {
					delete(blocksByPrevious, topo)

					if len(blocksByPrevious) == 0 {
						delete(c.byPrevious, topo.Previous)
					}
				}
			}

			// Remove from ByHeight
			delete(c.byHeight, c.oldestBlock)
		}

		c.oldestBlock++
	}
}

// NetTopologyCache holds the topology of all peers.
type NetTopologyCache struct {
	set         map[PeerHasBlock]util.Void
	byTopology  map[util.BlockTopologyCmp]map[peer.ID]util.Void
	byPrevious  map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void
	byHeight    map[types.BlockHeightType]map[PeerHasBlock]util.Void
	byPeer      map[peer.ID]map[PeerHasBlock]util.Void
	peerHeight  map[peer.ID]types.BlockHeightType
	oldestBlock types.BlockHeightType
	mutex       sync.Mutex
}

// NewNetTopologyCache instantiates a new TopologyCache
func NewNetTopologyCache() *NetTopologyCache {
	return &NetTopologyCache{
		set:         make(map[PeerHasBlock]util.Void),
		byTopology:  make(map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		byPrevious:  make(map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		byHeight:    make(map[types.BlockHeightType]map[PeerHasBlock]util.Void),
		byPeer:      make(map[peer.ID]map[PeerHasBlock]util.Void),
		peerHeight:  make(map[peer.ID]types.BlockHeightType),
		oldestBlock: ^types.BlockHeightType(0),
	}
}

// ByTopology allows for thread safe access to the topology map
func (c *NetTopologyCache) ByTopology(key util.BlockTopologyCmp) (map[peer.ID]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byTopology[key]
	return a, b
}

// ByPeer allows for thread safe access to the peer map
func (c *NetTopologyCache) ByPeer(key peer.ID) (map[PeerHasBlock]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byPeer[key]
	return a, b
}

// ByPrevious allows for thread safe access to the previous map
func (c *NetTopologyCache) ByPrevious(key util.MultihashCmp) (map[util.BlockTopologyCmp]map[peer.ID]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byPrevious[key]
	return a, b
}

// ByHeight allows for thread safe access to the height map
func (c *NetTopologyCache) ByHeight(key types.BlockHeightType) (map[PeerHasBlock]util.Void, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	a, b := c.byHeight[key]
	return a, b
}

// Add adds a known block held by a peer
func (c *NetTopologyCache) Add(peerHasBlock PeerHasBlock) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, hasBlock := c.set[peerHasBlock]
	if hasBlock {
		return false
	}
	c.set[peerHasBlock] = util.Void{}

	{
		m, hasM := c.byTopology[peerHasBlock.Block]
		if !hasM {
			m = make(map[peer.ID]util.Void)
			c.byTopology[peerHasBlock.Block] = m
		}
		m[peerHasBlock.PeerID] = util.Void{}
	}

	{
		m, hasM := c.byPrevious[peerHasBlock.Block.Previous]
		if !hasM {
			m = make(map[util.BlockTopologyCmp]map[peer.ID]util.Void)
			c.byPrevious[peerHasBlock.Block.Previous] = m
		}
		m2, hasM2 := m[peerHasBlock.Block]
		if !hasM2 {
			m2 = make(map[peer.ID]util.Void)
			m[peerHasBlock.Block] = m2
		}
		m2[peerHasBlock.PeerID] = util.Void{}
	}

	{
		m, hasM := c.byHeight[peerHasBlock.Block.Height]
		if !hasM {
			m = make(map[PeerHasBlock]util.Void)
			c.byHeight[peerHasBlock.Block.Height] = m
		}
		m[peerHasBlock] = util.Void{}
		if peerHasBlock.Block.Height < c.oldestBlock {
			c.oldestBlock = peerHasBlock.Block.Height
		}
	}

	{
		m, hasM := c.byPeer[peerHasBlock.PeerID]
		if !hasM {
			m = make(map[PeerHasBlock]util.Void)
			c.byPeer[peerHasBlock.PeerID] = m
		}
		m[peerHasBlock] = util.Void{}
	}

	{
		height, hasHeight := c.peerHeight[peerHasBlock.PeerID]
		if (!hasHeight) || (height < peerHasBlock.Block.Height) {
			c.peerHeight[peerHasBlock.PeerID] = peerHasBlock.Block.Height
		}
	}

	return true
}

// PickPeer chooses a random peer
func (c *NetTopologyCache) PickPeer(topo util.BlockTopologyCmp, rng *rand.Rand) (peer.ID, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var emptyPeerID peer.ID

	peers, hasPeers := c.byTopology[topo]
	if !hasPeers {
		return emptyPeerID, errors.New("attempt to log with no peers")
	}
	if len(peers) < 1 {
		return emptyPeerID, errors.New("cannot pick from empty peer list")
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
	return emptyPeerID, fmt.Errorf("could not pick the %dth element of map of length %d", pickIndex, len(peers))
}

// SetLastIrr sets the last irreversible block
func (c *NetTopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for c.oldestBlock < newMyLastIrr.Height {
		// Each block needs to be checked incrementally because removal of peers can
		// create gaps in the topology cache
		if peerBlocks, ok := c.byHeight[c.oldestBlock]; ok {

			for peerBlock := range peerBlocks {
				// Remove from Set
				delete(c.set, peerBlock)

				// Remove from ByTopology
				delete(c.byTopology, peerBlock.Block)

				// Remove from ByPrevious
				if blocksByPrevious, ok := c.byPrevious[peerBlock.Block.Previous]; ok {
					delete(blocksByPrevious, peerBlock.Block)

					if len(blocksByPrevious) == 0 {
						delete(c.byPrevious, peerBlock.Block.Previous)
					}
				}

				// Remove from ByPeer
				if blockByPeer, ok := c.byPeer[peerBlock.PeerID]; ok {
					delete(blockByPeer, peerBlock)

					if len(blockByPeer) == 0 {
						delete(c.byPeer, peerBlock.PeerID)
					}
				}
			}

			// Remove from ByHeight
			delete(c.byHeight, c.oldestBlock)
		}

		c.oldestBlock++
	}
}

// RemovePeer removes peer's blocks from the cache
func (c *NetTopologyCache) RemovePeer(pid peer.ID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if peerBlocks, ok := c.byPeer[pid]; ok {
		for peerBlock := range peerBlocks {
			// Remove from Set
			delete(c.set, peerBlock)

			// Remove from ByTopology
			if topologyPeers, ok := c.byTopology[peerBlock.Block]; ok {
				delete(topologyPeers, pid)

				if len(topologyPeers) == 0 {
					delete(c.byTopology, peerBlock.Block)
				}
			}

			// Remove from ByPrevious
			if blocksByPrevious, ok := c.byPrevious[peerBlock.Block.Previous]; ok {
				if topologyPeers, ok := blocksByPrevious[peerBlock.Block]; ok {
					delete(topologyPeers, pid)

					if len(topologyPeers) == 0 {
						delete(blocksByPrevious, peerBlock.Block)
					}
				}

				if len(blocksByPrevious) == 0 {
					delete(c.byPrevious, peerBlock.Block.Previous)
				}
			}

			// Remove from ByHeight
			if heightPeers, ok := c.byHeight[peerBlock.Block.Height]; ok {
				delete(heightPeers, peerBlock)

				if len(heightPeers) == 0 {
					delete(c.byHeight, peerBlock.Block.Height)
				}
			}
		}

		// Remove from ByPeer
		delete(c.byPeer, pid)

		// Remove from PeerHeight
		delete(c.peerHeight, pid)
	}
}

// getInitialDownload returns the initial download from a topology.
//
// The initial download is the set of blocks in netTopo that directly connect to myTopo but are
// not themselves in myTopo.
func getInitialDownload(localTopo *LocalTopologyCache, netTopo *NetTopologyCache) map[util.BlockTopologyCmp]util.Void {
	localTopo.mutex.Lock()
	netTopo.mutex.Lock()
	defer localTopo.mutex.Unlock()
	defer netTopo.mutex.Unlock()

	result := make(map[util.BlockTopologyCmp]util.Void)

	// Special case:  A node that has no blocks is interested in blocks of height 1
	if len(localTopo.byTopology) == 0 {
		log.Debugm("No blocks, so getInitialDownload is looking for blocks of height 1")
		netNextBlocks, ok := netTopo.byHeight[1]
		if ok {
			for nextBlock := range netNextBlocks {
				result[nextBlock.Block] = util.Void{}
			}
		}

		log.Debugm("getInitialDownload() returned blocks",
			"numBlocks", len(result))
		return result
	}

	for block := range localTopo.byTopology {
		netNextBlocks, ok := netTopo.byPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock := range netNextBlocks {
			// Skip blocks we already have
			_, myHasNextBlock := localTopo.byTopology[nextBlock]
			if myHasNextBlock {
				continue
			}
			result[nextBlock] = util.Void{}
		}
	}

	//log.Debugf("GetInitialDownload() returned %d blocks", len(result))
	return result
}

// getNextDownload returns the next download from a topology.
//
// The next download is the set of blocks in netTopo that directly connect to currentDownload.
func getNextDownload(netTopo *NetTopologyCache, currentDownload map[util.BlockTopologyCmp]util.Void) map[util.BlockTopologyCmp]util.Void {
	netTopo.mutex.Lock()
	defer netTopo.mutex.Unlock()

	result := make(map[util.BlockTopologyCmp]util.Void)
	for block := range currentDownload {
		netNextBlocks, ok := netTopo.byPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock := range netNextBlocks {
			result[nextBlock] = util.Void{}
		}
	}
	return result
}

// convertBlockTopologySetToSlice converts a set (a map from BlockTopologyCmp to util.Void) to a slice.
//
// Only the first n elements are converted.
func convertBlockTopologySetToSlice(m map[util.BlockTopologyCmp]util.Void) []util.BlockTopologyCmp {
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
func GetDownloads(localTopo *LocalTopologyCache, netTopo *NetTopologyCache, maxCount int, maxDepth int) []util.BlockTopologyCmp {
	nextSet := getInitialDownload(localTopo, netTopo)
	resultSet := make(map[util.BlockTopologyCmp]util.Void)

	// Set resultSet to the union of resultSet and nextSet
	for k := range nextSet {
		if len(resultSet) >= maxCount {
			return convertBlockTopologySetToSlice(resultSet)
		}
		resultSet[k] = util.Void{}
	}

	for depth := 1; depth <= maxDepth; depth++ {
		if len(resultSet) >= maxCount {
			return convertBlockTopologySetToSlice(resultSet)
		}

		// Set resultSet to the union of resultSet and nextSet
		for k := range nextSet {
			if len(resultSet) >= maxCount {
				return convertBlockTopologySetToSlice(resultSet)
			}
			resultSet[k] = util.Void{}
		}

		nextSet = getNextDownload(netTopo, resultSet)
		if len(nextSet) == 0 {
			return convertBlockTopologySetToSlice(resultSet)
		}
	}

	return convertBlockTopologySetToSlice(resultSet)
}
