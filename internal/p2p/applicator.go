package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
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
	ctx     context.Context
}

type blockApplicationStatus struct {
	block *protocol.Block
	err   error
}

type transactionApplicatorRequest struct {
	trx     *protocol.Transaction
	errChan chan<- error
	ctx     context.Context
}

// Applicator manages block application to avoid duplicate application and premature application
type Applicator struct {
	rpc          rpc.LocalRPC
	highestBlock uint64
	lib          uint64

	forkWatchdog *ForkWatchdog

	blocksById       map[string]*blockEntry
	blocksByPrevious map[string]map[string]void
	blocksByHeight   map[uint64]map[string]void

	newBlockChan         chan *blockEntry
	forkHeadsChan        chan *broadcast.ForkHeads
	blockBroadcastChan   chan *broadcast.BlockAccepted
	applyBlockChan       chan *blockApplicationRequest
	blockStatusChan      chan *blockApplicationStatus
	applyTransactionChan chan *transactionApplicatorRequest

	opts options.ApplicatorOptions
}

func NewApplicator(ctx context.Context, rpc rpc.LocalRPC, opts options.ApplicatorOptions) (*Applicator, error) {
	headInfo, err := rpc.GetHeadBlock(ctx)

	if err != nil {
		return nil, err
	}

	return &Applicator{
		rpc:                  rpc,
		highestBlock:         headInfo.HeadTopology.Height,
		lib:                  headInfo.LastIrreversibleBlock,
		forkWatchdog:         NewForkWatchdog(),
		blocksById:           make(map[string]*blockEntry),
		blocksByPrevious:     make(map[string]map[string]void),
		blocksByHeight:       make(map[uint64]map[string]void),
		newBlockChan:         make(chan *blockEntry, 10),
		forkHeadsChan:        make(chan *broadcast.ForkHeads, 10),
		blockBroadcastChan:   make(chan *broadcast.BlockAccepted, 10),
		applyBlockChan:       make(chan *blockApplicationRequest, 10),
		blockStatusChan:      make(chan *blockApplicationStatus, 10),
		applyTransactionChan: make(chan *transactionApplicatorRequest, 10),
		opts:                 opts,
	}, nil
}

// ApplyBlock will apply the block to the chain at the appropriate time
func (b *Applicator) ApplyBlock(ctx context.Context, block *protocol.Block) error {
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

func (b *Applicator) ApplyTransaction(ctx context.Context, trx *protocol.Transaction) error {
	errChan := make(chan error, 1)

	b.applyTransactionChan <- &transactionApplicatorRequest{trx, errChan, ctx}

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleForkHeads handles a fork heads broadcast
func (b *Applicator) HandleForkHeads(forkHeads *broadcast.ForkHeads) {
	b.forkHeadsChan <- forkHeads
}

// HandleBlockBroadcast handles a block broadcast
func (b *Applicator) HandleBlockBroadcast(blockAccept *broadcast.BlockAccepted) {
	b.blockBroadcastChan <- blockAccept
}

func (b *Applicator) addEntry(ctx context.Context, entry *blockEntry) {
	id := string(entry.block.Id)
	previousId := string(entry.block.Header.Previous)
	height := entry.block.Header.Height

	if oldEntry, ok := b.blocksById[id]; ok {
		oldEntry.errChans = append(oldEntry.errChans, entry.errChans...)
		return
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

	if entry.block.Header.Height <= b.highestBlock+1 {
		b.requestApplication(ctx, entry.block)
	}
}

func (b *Applicator) removeEntry(ctx context.Context, id string, err error) {
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

func (b *Applicator) requestApplication(ctx context.Context, block *protocol.Block) {
	go func() {
		errChan := make(chan error, 1)

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

		b.applyBlockChan <- &blockApplicationRequest{block, errChan, ctx}

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

func (b *Applicator) handleNewBlock(ctx context.Context, entry *blockEntry) {
	var err error

	if entry.block.Header.Height > b.highestBlock+b.opts.MaxHeightDelta {
		err = fmt.Errorf("%w, block height exeeds applicator height delta", p2perrors.ErrBlockApplication)
	} else if len(b.blocksById) >= int(b.opts.MaxPendingBlocks) {
		err = p2perrors.ErrMaxPendingBlocks
	} else if entry.block.Header.Height <= b.lib {
		err = p2perrors.ErrBlockIrreversibility
	} else {
		b.addEntry(ctx, entry)
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

func (b *Applicator) handleForkHeads(ctx context.Context, forkHeads *broadcast.ForkHeads) {
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

func (b *Applicator) handleBlockBroadcast(ctx context.Context, blockAccept *broadcast.BlockAccepted) {
	// It is not possible for a block with a new highest height to not be head, so this check is sufficient
	if blockAccept.Block.Header.Height > b.highestBlock {
		b.highestBlock = blockAccept.Block.Header.Height
	}

	if children, ok := b.blocksByPrevious[string(blockAccept.Block.Id)]; ok {
		for id := range children {
			if entry, ok := b.blocksById[id]; ok {
				b.requestApplication(ctx, entry.block)
			}
		}
	}
}

func (b *Applicator) handleApplyBlock(request *blockApplicationRequest) {
	var err error
	if request.block.Header.Height <= atomic.LoadUint64(&b.lib) {
		err = p2perrors.ErrBlockIrreversibility
	} else {
		_, err = b.rpc.ApplyBlock(request.ctx, request.block)
	}

	request.errChan <- err
	close(request.errChan)
}

func (b *Applicator) handleApplyTransaction(request *transactionApplicatorRequest) {
	_, err := b.rpc.ApplyTransaction(request.ctx, request.trx)

	request.errChan <- err
	close(request.errChan)
}

func (b *Applicator) Start(ctx context.Context) {
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

	for i := 0; i < b.opts.ApplicationJobs; i++ {
		go func() {
			for {
				select {
				case request := <-b.applyBlockChan:
					b.handleApplyBlock(request)
				default:
					select {
					case request := <-b.applyBlockChan:
						b.handleApplyBlock(request)
					case request := <-b.applyTransactionChan:
						b.handleApplyTransaction(request)
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
}
