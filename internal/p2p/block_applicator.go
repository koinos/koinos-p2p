package p2p

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
)

type blockEntry struct {
	block    *protocol.Block
	errChans []chan<- error
}

type blockApplicationRequest struct {
	block   *protocol.Block
	errChan chan<- error
}

type blockApplicationStatus struct {
	block *protocol.Block
	err   error
}

// BlockApplicator manages block application to avoid duplicate application and premature application
type BlockApplicator struct {
	rpc  rpc.LocalRPC
	head *koinos.BlockTopology
	lib  uint64

	forkWatchdog *ForkWatchdog

	blocksById       map[string]*blockEntry
	blocksByPrevious map[string]map[string]void
	blocksByHeight   map[uint64]map[string]void

	newBlockChan       chan *blockEntry
	forkHeadsChan      chan *broadcast.ForkHeads
	blockBroadcastChan chan *broadcast.BlockAccepted
	applyBlockChan     chan *blockApplicationRequest
	blockStatusChan    chan *blockApplicationStatus

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
		forkWatchdog:       NewForkWatchdog(),
		blocksById:         make(map[string]*blockEntry),
		blocksByPrevious:   make(map[string]map[string]void),
		blocksByHeight:     make(map[uint64]map[string]void),
		newBlockChan:       make(chan *blockEntry, 10),
		forkHeadsChan:      make(chan *broadcast.ForkHeads, 10),
		blockBroadcastChan: make(chan *broadcast.BlockAccepted, 10),
		applyBlockChan:     make(chan *blockApplicationRequest, 10),
		blockStatusChan:    make(chan *blockApplicationStatus, 10),
		opts:               opts,
	}, nil
}

// ApplyBlock will apply the block to the chain at the appropriate time
func (b *BlockApplicator) ApplyBlock(ctx context.Context, block *protocol.Block) error {
	err := b.forkWatchdog.Add(block)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)

	b.newBlockChan <- &blockEntry{block: block, errChans: []chan<- error{errChan}}

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

	if oldEntry, ok := b.blocksById[id]; ok {
		oldEntry.errChans = append(oldEntry.errChans, entry.errChans...)
	} else {
		b.blocksById[id] = entry
	}

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

func (b *BlockApplicator) removeEntry(ctx context.Context, id string, err error) {
	if entry, ok := b.blocksById[id]; ok {
		for _, ch := range entry.errChans {
			select {
			case ch <- err:
				close(ch)
			case <-ctx.Done():
			}
		}

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

func (b *BlockApplicator) requestApplication(ctx context.Context, block *protocol.Block) {
	go func() {
		errChan := make(chan error)

		// If block is more than 4 seconds in the future, do not apply it until
		// it is less than 4 seconds in the future.
		applicationThreshold := time.Now().Add(b.opts.DelayThreshold)
		blockTime := time.Unix(int64(block.Header.Timestamp/1000), int64(block.Header.Timestamp%1000))

		if blockTime.After(applicationThreshold) {
			delayCtx, delayCancel := context.WithTimeout(ctx, b.opts.DelayTimeout)
			defer delayCancel()
			timerCtx, timerCancel := context.WithTimeout(ctx, blockTime.Sub(applicationThreshold))
			defer timerCancel()

			select {
			case <-timerCtx.Done():
			case <-delayCtx.Done():
				b.blockStatusChan <- &blockApplicationStatus{
					block: block,
					err:   ctx.Err(),
				}
				return
			}
		}

		b.applyBlockChan <- &blockApplicationRequest{
			block:   block,
			errChan: errChan,
		}

		select {
		case err := <-errChan:
			b.blockStatusChan <- &blockApplicationStatus{
				block: block,
				err:   err,
			}

		case <-ctx.Done():
		}
	}()
}

func (b *BlockApplicator) handleNewBlock(ctx context.Context, entry *blockEntry) {
	var err error

	if entry.block.Header.Height > b.head.Height+b.opts.MaxHeightDelta {
		err = p2perrors.ErrBlockApplication
	} else if len(b.blocksById) >= int(b.opts.MaxPendingBlocks) {
		err = p2perrors.ErrMaxPendingBlocks
	} else if entry.block.Header.Height <= b.lib {
		err = p2perrors.ErrBlockIrreversibility
	} else {
		err = b.addEntry(entry)
		if err == nil && entry.block.Header.Height <= b.head.Height+1 && entry.block.Header.Height > b.lib {
			b.requestApplication(ctx, entry.block)
		}
	}

	if err != nil {
		for _, ch := range entry.errChans {
			select {
			case ch <- err:
				close(ch)
			case <-ctx.Done():
			}
		}
	}
}

func (b *BlockApplicator) handleForkHeads(ctx context.Context, forkHeads *broadcast.ForkHeads) {
	oldLib := b.lib
	atomic.StoreUint64(&b.lib, forkHeads.LastIrreversibleBlock.Height)

	b.forkWatchdog.Purge(forkHeads.LastIrreversibleBlock.Height)

	// Blocks at or before LIB are automatically rejected, so all entries have height
	// greater than previous LIB. We check every block height greater than old LIB,
	// up to and including the current LIB and remove their entry.
	// Some blocks may be on unreachable forks at this point. That's ok.
	// Because they are not reachable, we will never get a parent block that causes their
	// application and they'll be cleaned up using this logic once their height passes
	// beyond irreversibility
	for h := oldLib + 1; h <= forkHeads.LastIrreversibleBlock.Height; h++ {
		if ids, ok := b.blocksByHeight[h]; ok {
			for id := range ids {
				b.removeEntry(ctx, id, p2perrors.ErrBlockIrreversibility)
			}
		}
	}
}

func (b *BlockApplicator) handleBlockBroadcast(ctx context.Context, blockAccept *broadcast.BlockAccepted) {
	if blockAccept.Head {
		b.head = &koinos.BlockTopology{Id: blockAccept.Block.Id, Height: blockAccept.Block.Header.Height, Previous: blockAccept.Block.Header.Previous}
	}

	if children, ok := b.blocksByPrevious[string(blockAccept.Block.Id)]; ok {
		for id := range children {
			if entry, ok := b.blocksById[id]; ok {
				b.requestApplication(ctx, entry.block)
			}
		}
	}
}

func (b *BlockApplicator) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case status := <-b.blockStatusChan:
				if status.err == nil || !errors.Is(status.err, p2perrors.ErrUnknownPreviousBlock) {
					b.removeEntry(ctx, string(status.block.Id), status.err)
				}
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

	go func() {
		for {
			select {
			case request := <-b.applyBlockChan:
				var err error
				if request.block.Header.Height <= atomic.LoadUint64(&b.lib) {
					err = p2perrors.ErrBlockIrreversibility
				} else {
					_, err = b.rpc.ApplyBlock(ctx, request.block)
				}

				request.errChan <- err
				close(request.errChan)

			case <-ctx.Done():
				return
			}
		}
	}()
}
