package protocol

import (
	"github.com/libp2p/go-libp2p-core/peer"

	types "github.com/koinos/koinos-types-golang"
)

// MyTopologyCache holds my topology (i.e. the topology of a single node).
type MyTopologyCache struct {
	DeserCache map[string]types.BlockTopology
	ByID       map[types.Multihash]map[string]void
	ByPrevious map[types.Multihash]map[string]void
	ByHeight   map[types.BlockHeightType]map[string]void
}

func NewMyTopologyCache() *MyTopologyCache {
	return MyTopologyCache{
		DeserCache: make(map[string]types.BlockTopology),
		ByID:       make(map[types.Multihash]map[string]void),
		ByPrevious: make(map[types.Multihash]map[string]void),
		ByHeight:   make(map[types.BlockHeightType]map[string]void),
	}
}

func (c *MyTopologyCache) Add(block types.BlockTopology) {
	k := block.Serialize(NewVariableBlob())
	c.DeserCache[k] = block

	{
		m, hasM := c.ByID[k]
		if !hasM {
			m = make(map[string]void)
			c.ByID[k] = m
		}
		m[k] = void{}
	}

	{
		m, hasM := c.ByPrevious[k]
		if !hasM {
			m = make(map[string]void)
			c.ByPrevious[k] = m
		}
		m[k] = void{}
	}

	{
		m, hasM := c.ByHeight[k]
		if !hasM {
			m = make(map[string]void)
			c.ByHeight[k] = m
		}
		m[k] = void{}
	}
}

func (c *MyTopologyCache) SetLastIrr(newMyLastIrr types.BlockTopology) {
	// TODO: Implement this
}

// TopologyCache holds the topology of all peers.
// TODO rename to NetTopologyCache?
type TopologyCache struct {
	DeserCache map[string]types.BlockTopology
	ByTopology map[string]map[peer.ID]void
	ByPrevious map[types.Multihash]map[string]void
	ByHeight   map[types.BlockHeightType]map[string]void
	ByPeer     map[peer.ID]map[string]void
}

func NewTopologyCache() *TopologyCache {
	return TopologyCache{
		DeserCache: make(map[string]types.BlockTopology),
		ByTopology: make(map[string]map[peer.ID]void),
		ByPrevious: make(map[types.Multihash]map[string]void),
		ByHeight:   make(map[types.BlockHeightType]map[string]void),
		ByPeer:     make(map[peer.ID]map[string]void),
	}
}

func (c *TopologyCache) Add(peerID peer.ID, block types.BlockTopology) {
	k := block.Serialize(NewVariableBlob())
	c.DeserCache[k] = block

	{
		m, hasM := c.ByTopology[k]
		if !hasM {
			m = make(map[peer.ID]void)
			c.ByTopology[k] = m
		}
		m[peerID] = void{}
	}

	{
		m, hasM := c.ByPrevious[k]
		if !hasM {
			m = make(map[string]void)
			c.ByPrevious[k] = m
		}
		m[k] = void{}
	}

	{
		m, hasM := c.ByHeight[k]
		if !hasM {
			m = make(map[string]void)
			c.ByHeight[k] = m
		}
		m[k] = void{}
	}

	{
		m, hasM := c.ByPeer[peerID]
		if !hasM {
			m = make(map[string]void)
			c.ByPeer[k] = m
		}
		m[k] = void{}
	}
}

func (c *TopologyCache) PickPeer(topo string, rng *rand.Rand) (peer.ID, error) {
	peers, hasPeers := c.ByTopology[topo]
	if !hasPeers {
		return nil, errors.New("Attempt to log with no peers")
	}
	if len(peers) < 1 {
		return nil, errors.New("Cannot pick from empty peer list")
	}
	pickIndex := rng.Intn(len(peers))

	// O(n) scan to pick peer, is there a way to speed this up?
	i := 0
	for peerID, _ := range peers {
		if i == pickIndex {
			return peerID
		}
		i += 1
	}
	return nil, errors.New("Could not pick the %dth element of map of length %d", pickIndex, len(peers))
}

func (c *TopologyCache) SetLastIrr(newMyLastIrr types.BlockTopology) {
	// TODO: Implement this
}

// GetInitialDownload returns the initial download from a topology.
//
// The initial download is the set of blocks in netTopo that directly connect to myTopo but are
// not themselves in myTopo.
func GetInitialDownload(myTopo *MyTopologyCache, netTopo *TopologyCache) map[string]void {
	result := make(map[string]void)
	for k, block := range myTopo.DeserCache {
		netNextBlocks, ok := netTopo.ByPrevious[k]
		if !ok {
			continue
		}
		for _, nextK := range netNextBlocks {
			// Skip blocks we already have
			_, myHasNextK := myTopo.DeserCache[k]
			if myHasNextK {
				continue
			}
			result[nextK] = void{}
		}
	}
	return result
}

// GetNextDownload returns the next download from a topology.
//
// The next download is the set of blocks in netTopo that directly connect to currentDownload.
func GetNextDownload(myTopo *MyTopologyCache, netTopo *TopologyCache, currentDownload map[string]void) map[string]void {
	result := make(map[string]void)
	for k, _ := range currentDownload {
		netNextBlocks, ok := netTopo.ByPrevious[k]
		if !ok {
			continue
		}
		for _, nextK := range netNextBlocks {
			result[nextK] = void{}
		}
	}
	return result
}

// ConvertSetToSlice converts a set (a map from string to void) to a slice.
//
// Only the first n elements are converted.
func ConvertSetToSlice(m map[string]void) []string {
	result := make([]string, len(m))

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
func GetDownloads(myTopo *MyTopologyCache, netTopo *TopologyCache, maxCount int, maxDepth int) []string {
	nextSet := GetInitialDownload(myTopo, netTopo)
	resultSet := make(map[string]void)

	// Set resultSet to the union of resultSet and nextSet
	for k, _ := range nextSet {
		if len(resultSet) >= maxCount {
			return resultSet
		}
		resultSet[k] = void{}
	}

	for depth := 1; depth <= maxDepth; depth++ {
		if len(resultSet) >= maxCount {
			return resultSet
		}

		// Set resultSet to the union of resultSet and nextSet
		for k, _ := range nextSet {
			if len(resultSet) >= maxCount {
				return resultSet
			}
			resultSet[k] = void{}
		}

		nextSet = GetNextDownload(myTopo, netTopo, resultSet)
		if len(nextSet) == 0 {
			return resultSet
		}
	}

	return resultSet
}
