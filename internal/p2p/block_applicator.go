package p2p

import (
	"context"
	"errors"
	"sort"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
)

type blockEntry struct {
	block   *protocol.Block
	errChan chan<- error
}

// BlockApplicator manages block application to avoid duplicate application and premature application
type BlockApplicator struct {
	rpc  rpc.LocalRPC
	head *koinos.BlockTopology
	lib  uint64

	blocksToApply    map[string]void
	blocksById       map[string]*blockEntry
	blocksByPrevious map[string]map[string]void
	blocksByHeight   map[uint64]map[string]void

	newBlockChan       chan *blockEntry
	forkHeadsChan      chan *broadcast.ForkHeads
	blockBroadcastChan chan *broadcast.BlockAccepted

	opts options.BlockApplicatorOptions
}

func NewBlockApplicator(ctx context.Context, rpc rpc.LocalRPC, opts options.BlockApplicatorOptions) (*BlockApplicator, error) {
	headInfo, err := rpc.GetHeadBlock(ctx)

	if err != nil {
		return nil, err
	}

	return &BlockApplicator{
		rpc:                rpc,
		head:               headInfo.HeadTopology,
		lib:                headInfo.LastIrreversibleBlock,
		blocksToApply:      make(map[string]void),
		blocksById:         make(map[string]*blockEntry),
		blocksByPrevious:   make(map[string]map[string]void),
		blocksByHeight:     make(map[uint64]map[string]void),
		newBlockChan:       make(chan *blockEntry),
		forkHeadsChan:      make(chan *broadcast.ForkHeads),
		blockBroadcastChan: make(chan *broadcast.BlockAccepted),
		opts:               opts,
	}, nil
}

// ApplyBlock will apply the block to the chain at the appropriate time
func (b *BlockApplicator) ApplyBlock(ctx context.Context, block *protocol.Block) error {
	errChan := make(chan error, 1)

	b.newBlockChan <- &blockEntry{block: block, errChan: errChan}

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		return p2perrors.ErrBlockApplicationTimeout
	}
}

// HandleForkHeads handles a fork heads broadcast
func (b *BlockApplicator) HandleForkHeads(forkHeads *broadcast.ForkHeads) {
	b.forkHeadsChan <- forkHeads
}

// HandleBlockBroadcast handles a block broadcast
func (b *BlockApplicator) HandleBlockBroadcast(blockAccept *broadcast.BlockAccepted) {
	b.blockBroadcastChan <- blockAccept
}

func (b *BlockApplicator) addEntry(entry *blockEntry) error {
	id := string(entry.block.Id)
	previousId := string(entry.block.Header.Previous)
	height := entry.block.Header.Height

	if _, ok := b.blocksById[id]; ok {
		return errors.New("duplicate block added")
	}

	b.blocksById[id] = entry

	if _, ok := b.blocksByPrevious[previousId]; !ok {
		b.blocksByPrevious[previousId] = make(map[string]void)
	}
	b.blocksByPrevious[string(entry.block.Header.Previous)][id] = void{}

	if _, ok := b.blocksByHeight[height]; !ok {
		b.blocksByHeight[height] = make(map[string]void)
	}
	b.blocksByHeight[height][id] = void{}

	return nil
}

func (b *BlockApplicator) removeEntry(id string) {
	if entry, ok := b.blocksById[id]; ok {
		close(entry.errChan)
		delete(b.blocksById, id)

		previousId := string(entry.block.Header.Previous)
		height := entry.block.Header.Height

		if blocks, ok := b.blocksByPrevious[previousId]; ok {
			delete(blocks, id)

			if len(b.blocksByPrevious[previousId]) == 0 {
				delete(b.blocksByPrevious, previousId)
			}
		}

		if blocks, ok := b.blocksByHeight[height]; ok {
			delete(blocks, id)

			if len(b.blocksByHeight[height]) == 0 {
				delete(b.blocksByHeight, height)
			}
		}
	}
}

func (b *BlockApplicator) applyBlock(ctx context.Context, id string) {
	if entry, ok := b.blocksById[id]; ok {

		_, err := b.rpc.ApplyBlock(ctx, entry.block)
		entry.errChan <- err

		b.removeEntry(id)
	}
}

func (b *BlockApplicator) handleNewBlock(ctx context.Context, entry *blockEntry) {
	var err error = nil

	if len(b.blocksById) >= int(b.opts.MaxPendingBlocks) {
		err = p2perrors.ErrMaxPendingBlocks
	} else if entry.block.Header.Height > b.head.Height+1 {
		err = b.addEntry(entry)
	} else {
		_, err = b.rpc.ApplyBlock(ctx, entry.block)

		if errors.Is(err, p2perrors.ErrUnknownPreviousBlock) {
			b.addEntry(entry)
			return
		}
	}

	entry.errChan <- err
	close(entry.errChan)
}

func (b *BlockApplicator) handleForkHeads(ctx context.Context, forkHeads *broadcast.ForkHeads) {
	heights := make([]uint64, 0, len(b.blocksByHeight))

	for h := range b.blocksByHeight {
		heights = append(heights, h)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	for _, h := range heights {
		if h <= forkHeads.LastIrreversibleBlock.Height {
			for id := range b.blocksByHeight[h] {
				b.blocksById[id].errChan <- p2perrors.ErrUnknownPreviousBlock
				b.removeEntry(id)
			}
		} else {
			break
		}
	}
}

func (b *BlockApplicator) handleBlockBroadcast(ctx context.Context, blockAccept *broadcast.BlockAccepted) {
	if blockAccept.Head {
		b.head = &koinos.BlockTopology{Id: blockAccept.Block.Id, Height: blockAccept.Block.Header.Height, Previous: blockAccept.Block.Header.Previous}
	}

	if children, ok := b.blocksByPrevious[string(blockAccept.Block.Header.Previous)]; ok {
		for id := range children {
			b.blocksToApply[id] = void{}
		}
	}
}

func (b *BlockApplicator) Start(ctx context.Context) {
	go func() {
		for {
			for id := range b.blocksToApply {
				b.applyBlock(ctx, id)
			}

			if len(b.blocksToApply) > 0 {
				b.blocksToApply = make(map[string]void)
			}

			select {
			case entry := <-b.newBlockChan:
				b.handleNewBlock(ctx, entry)
			case forkHeads := <-b.forkHeadsChan:
				b.handleForkHeads(ctx, forkHeads)
			case blockBroadcast := <-b.blockBroadcastChan:
				b.handleBlockBroadcast(ctx, blockBroadcast)

			case <-ctx.Done():
				return
			}
		}
	}()
}
