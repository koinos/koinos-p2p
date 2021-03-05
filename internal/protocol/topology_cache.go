package protocol

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
)

// PeerHasBlock is a message that specifies a peer has a block with the given topology.
//
type PeerHasBlock struct {
	PeerID peer.ID
	Block  util.BlockTopologyCmp
}

// MyTopologyCache holds my topology (i.e. the topology of a single node).
type MyTopologyCache struct {
	ByTopology map[util.BlockTopologyCmp]util.Void
	ByID       map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	ByPrevious map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void
	ByHeight   map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void
}

func NewMyTopologyCache() *MyTopologyCache {
	return &MyTopologyCache{
		ByTopology: make(map[util.BlockTopologyCmp]util.Void),
		ByID:       make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		ByPrevious: make(map[util.MultihashCmp]map[util.BlockTopologyCmp]util.Void),
		ByHeight:   make(map[types.BlockHeightType]map[util.BlockTopologyCmp]util.Void),
	}
}

func (c *MyTopologyCache) Add(block util.BlockTopologyCmp) {
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
	}
}

func (c *MyTopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	// TODO: Implement this
}

// TopologyCache holds the topology of all peers.
// TODO rename to NetTopologyCache?
type TopologyCache struct {
	Set        map[PeerHasBlock]util.Void
	ByTopology map[util.BlockTopologyCmp]map[peer.ID]util.Void
	ByPrevious map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void
	ByHeight   map[types.BlockHeightType]map[PeerHasBlock]util.Void
	ByPeer     map[peer.ID]map[PeerHasBlock]util.Void
}

func NewTopologyCache() *TopologyCache {
	return &TopologyCache{
		Set:        make(map[PeerHasBlock]util.Void),
		ByTopology: make(map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		ByPrevious: make(map[util.MultihashCmp]map[util.BlockTopologyCmp]map[peer.ID]util.Void),
		ByHeight:   make(map[types.BlockHeightType]map[PeerHasBlock]util.Void),
		ByPeer:     make(map[peer.ID]map[PeerHasBlock]util.Void),
	}
}

func (c *TopologyCache) Add(peerHasBlock PeerHasBlock) {
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
	}

	{
		m, hasM := c.ByPeer[peerHasBlock.PeerID]
		if !hasM {
			m = make(map[PeerHasBlock]util.Void)
			c.ByPeer[peerHasBlock.PeerID] = m
		}
		m[peerHasBlock] = util.Void{}
	}
}

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
	for peerID, _ := range peers {
		if i == pickIndex {
			return peerID, nil
		}
		i += 1
	}
	return emptyPeerID, fmt.Errorf("Could not pick the %dth element of map of length %d", pickIndex, len(peers))
}

func (c *TopologyCache) SetLastIrr(newMyLastIrr util.BlockTopologyCmp) {
	// TODO: Implement this
}

// GetInitialDownload returns the initial download from a topology.
//
// The initial download is the set of blocks in netTopo that directly connect to myTopo but are
// not themselves in myTopo.
func GetInitialDownload(myTopo *MyTopologyCache, netTopo *TopologyCache) map[util.BlockTopologyCmp]util.Void {
	result := make(map[util.BlockTopologyCmp]util.Void)
	for block, _ := range myTopo.ByTopology {
		netNextBlocks, ok := netTopo.ByPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock, _ := range netNextBlocks {
			// Skip blocks we already have
			_, myHasNextBlock := myTopo.ByTopology[nextBlock]
			if myHasNextBlock {
				continue
			}
			result[nextBlock] = util.Void{}
		}
	}
	return result
}

// GetNextDownload returns the next download from a topology.
//
// The next download is the set of blocks in netTopo that directly connect to currentDownload.
func GetNextDownload(myTopo *MyTopologyCache, netTopo *TopologyCache, currentDownload map[util.BlockTopologyCmp]util.Void) map[util.BlockTopologyCmp]util.Void {
	result := make(map[util.BlockTopologyCmp]util.Void)
	for block, _ := range currentDownload {
		netNextBlocks, ok := netTopo.ByPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock, _ := range netNextBlocks {
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
	for k, _ := range m {
		result[i] = k
		i += 1
	}
	return result
}

// GetDownloads() scans for a set of downloads that makes progress from the current topology.
//
// This function could likely be optimized by adding additional indexing.
func GetDownloads(myTopo *MyTopologyCache, netTopo *TopologyCache, maxCount int, maxDepth int) []util.BlockTopologyCmp {
	nextSet := GetInitialDownload(myTopo, netTopo)
	resultSet := make(map[util.BlockTopologyCmp]util.Void)

	// Set resultSet to the union of resultSet and nextSet
	for k, _ := range nextSet {
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
		for k, _ := range nextSet {
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