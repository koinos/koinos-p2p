package protocol

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"

	types "github.com/koinos/koinos-types-golang"
)

// PeerHasBlock is a message that specifies a peer has a block with the given topology.
//
type PeerHasBlock struct {
	PeerID peer.ID
	Block  BlockTopologyCmp
}

// MyTopologyCache holds my topology (i.e. the topology of a single node).
type MyTopologyCache struct {
	ByTopology map[BlockTopologyCmp]void
	ByID       map[MultihashCmp]map[BlockTopologyCmp]void
	ByPrevious map[MultihashCmp]map[BlockTopologyCmp]void
	ByHeight   map[types.BlockHeightType]map[BlockTopologyCmp]void
}

func NewMyTopologyCache() *MyTopologyCache {
	return &MyTopologyCache{
		ByTopology: make(map[BlockTopologyCmp]void),
		ByID:       make(map[MultihashCmp]map[BlockTopologyCmp]void),
		ByPrevious: make(map[MultihashCmp]map[BlockTopologyCmp]void),
		ByHeight:   make(map[types.BlockHeightType]map[BlockTopologyCmp]void),
	}
}

func (c *MyTopologyCache) Add(block BlockTopologyCmp) {
	c.ByTopology[block] = void{}

	{
		m, hasM := c.ByID[block.ID]
		if !hasM {
			m = make(map[BlockTopologyCmp]void)
			c.ByID[block.ID] = m
		}
		m[block] = void{}
	}

	{
		m, hasM := c.ByPrevious[block.Previous]
		if !hasM {
			m = make(map[BlockTopologyCmp]void)
			c.ByPrevious[block.Previous] = m
		}
		m[block] = void{}
	}

	{
		m, hasM := c.ByHeight[block.Height]
		if !hasM {
			m = make(map[BlockTopologyCmp]void)
			c.ByHeight[block.Height] = m
		}
		m[block] = void{}
	}
}

func (c *MyTopologyCache) SetLastIrr(newMyLastIrr BlockTopologyCmp) {
	// TODO: Implement this
}

// TopologyCache holds the topology of all peers.
// TODO rename to NetTopologyCache?
type TopologyCache struct {
	Set        map[PeerHasBlock]void
	ByTopology map[BlockTopologyCmp]map[peer.ID]void
	ByPrevious map[MultihashCmp]map[BlockTopologyCmp]map[peer.ID]void
	ByHeight   map[types.BlockHeightType]map[PeerHasBlock]void
	ByPeer     map[peer.ID]map[PeerHasBlock]void
}

func NewTopologyCache() *TopologyCache {
	return &TopologyCache{
		Set:        make(map[PeerHasBlock]void),
		ByTopology: make(map[BlockTopologyCmp]map[peer.ID]void),
		ByPrevious: make(map[MultihashCmp]map[BlockTopologyCmp]map[peer.ID]void),
		ByHeight:   make(map[types.BlockHeightType]map[PeerHasBlock]void),
		ByPeer:     make(map[peer.ID]map[PeerHasBlock]void),
	}
}

func (c *TopologyCache) Add(peerHasBlock PeerHasBlock) {
	c.Set[peerHasBlock] = void{}

	{
		m, hasM := c.ByTopology[peerHasBlock.Block]
		if !hasM {
			m = make(map[peer.ID]void)
			c.ByTopology[peerHasBlock.Block] = m
		}
		m[peerHasBlock.PeerID] = void{}
	}

	{
		m, hasM := c.ByPrevious[peerHasBlock.Block.Previous]
		if !hasM {
			m = make(map[BlockTopologyCmp]map[peer.ID]void)
			c.ByPrevious[peerHasBlock.Block.Previous] = m
		}
		m2, hasM2 := m[peerHasBlock.Block]
		if !hasM2 {
			m2 = make(map[peer.ID]void)
			m[peerHasBlock.Block] = m2
		}
		m2[peerHasBlock.PeerID] = void{}
	}

	{
		m, hasM := c.ByHeight[peerHasBlock.Block.Height]
		if !hasM {
			m = make(map[PeerHasBlock]void)
			c.ByHeight[peerHasBlock.Block.Height] = m
		}
		m[peerHasBlock] = void{}
	}

	{
		m, hasM := c.ByPeer[peerHasBlock.PeerID]
		if !hasM {
			m = make(map[PeerHasBlock]void)
			c.ByPeer[peerHasBlock.PeerID] = m
		}
		m[peerHasBlock] = void{}
	}
}

func (c *TopologyCache) PickPeer(topo BlockTopologyCmp, rng *rand.Rand) (peer.ID, error) {
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

func (c *TopologyCache) SetLastIrr(newMyLastIrr BlockTopologyCmp) {
	// TODO: Implement this
}

// GetInitialDownload returns the initial download from a topology.
//
// The initial download is the set of blocks in netTopo that directly connect to myTopo but are
// not themselves in myTopo.
func GetInitialDownload(myTopo *MyTopologyCache, netTopo *TopologyCache) map[BlockTopologyCmp]void {
	result := make(map[BlockTopologyCmp]void)
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
			result[nextBlock] = void{}
		}
	}
	return result
}

// GetNextDownload returns the next download from a topology.
//
// The next download is the set of blocks in netTopo that directly connect to currentDownload.
func GetNextDownload(myTopo *MyTopologyCache, netTopo *TopologyCache, currentDownload map[BlockTopologyCmp]void) map[BlockTopologyCmp]void {
	result := make(map[BlockTopologyCmp]void)
	for block, _ := range currentDownload {
		netNextBlocks, ok := netTopo.ByPrevious[block.ID]
		if !ok {
			continue
		}
		for nextBlock, _ := range netNextBlocks {
			result[nextBlock] = void{}
		}
	}
	return result
}

// ConvertSetToSlice converts a set (a map from BlockTopologyCmp to void) to a slice.
//
// Only the first n elements are converted.
func ConvertSetToSlice(m map[BlockTopologyCmp]void) []BlockTopologyCmp {
	result := make([]BlockTopologyCmp, len(m))

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
func GetDownloads(myTopo *MyTopologyCache, netTopo *TopologyCache, maxCount int, maxDepth int) []BlockTopologyCmp {
	nextSet := GetInitialDownload(myTopo, netTopo)
	resultSet := make(map[BlockTopologyCmp]void)

	// Set resultSet to the union of resultSet and nextSet
	for k, _ := range nextSet {
		if len(resultSet) >= maxCount {
			return ConvertSetToSlice(resultSet)
		}
		resultSet[k] = void{}
	}

	for depth := 1; depth <= maxDepth; depth++ {
		if len(resultSet) >= maxCount {
			return ConvertSetToSlice(resultSet)
		}

		// Set resultSet to the union of resultSet and nextSet
		for k, _ := range nextSet {
			if len(resultSet) >= maxCount {
				return ConvertSetToSlice(resultSet)
			}
			resultSet[k] = void{}
		}

		nextSet = GetNextDownload(myTopo, netTopo, resultSet)
		if len(nextSet) == 0 {
			return ConvertSetToSlice(resultSet)
		}
	}

	return ConvertSetToSlice(resultSet)
}
