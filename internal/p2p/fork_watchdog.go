package p2p

import (
	"sort"
	"sync"

	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
)

type pairKey struct {
	Signer string
	Parent string
}

type ForkWatchdog struct {
	forkTracker map[uint64]map[pairKey]map[string]void
	mutex       sync.Mutex
}

func NewForkWatchdog() *ForkWatchdog {
	return &ForkWatchdog{
		forkTracker: make(map[uint64]map[pairKey]map[string]void),
	}
}

// Add a block to the fork watchdog
func (f *ForkWatchdog) Add(block *protocol.Block) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	pair := pairKey{
		Signer: string(block.Header.Signer),
		Parent: string(block.Header.Previous),
	}

	if _, ok := f.forkTracker[block.Header.Height]; !ok {
		f.forkTracker[block.Header.Height] = make(map[pairKey]map[string]void)
	}

	if _, ok := f.forkTracker[block.Header.Height][pair]; !ok {
		f.forkTracker[block.Header.Height][pair] = make(map[string]void)
	}

	f.forkTracker[block.Header.Height][pair][string(block.Id)] = void{}

	if len(f.forkTracker[block.Header.Height][pair]) > 3 {
		return p2perrors.ErrForkBomb
	}

	return nil
}

// Purge a set of fork records from fork watchdog
func (f *ForkWatchdog) Purge(lib uint64) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	heights := make([]uint64, 0, len(f.forkTracker))

	for h := range f.forkTracker {
		heights = append(heights, h)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	for _, h := range heights {
		if h <= lib {
			delete(f.forkTracker, h)
		} else {
			break
		}
	}
}
