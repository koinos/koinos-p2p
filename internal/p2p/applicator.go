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
	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
)

type blockEntry struct {
	block    *protocol.Block
	errChans []chan<- error
}

type tryBlockApplicationRequest struct {
	block *protocol.Block
	force bool
}

type applyBlockRequest struct {
	block   *protocol.Block
	errChan chan<- error
	ctx     context.Context
}

type blockApplicationStatus struct {
	block *protocol.Block
	err   error
}

type applyTransactionRequest struct {
	trx     *protocol.Transaction
	errChan chan<- error
	ctx     context.Context
}

// Applicator manages block application to avoid duplicate application and premature application
type Applicator struct {
	rpc          rpc.LocalRPC
	highestBlock uint64
	lib          uint64

	transactionCache *TransactionCache

	forkWatchdog *ForkWatchdog

	blocksById       map[string]*blockEntry
	blocksByPrevious map[string]map[string]void
	blocksByHeight   map[uint64]map[string]void
	pendingBlocks    map[string]void

	newBlockChan       chan *blockEntry
	forkHeadsChan      chan *broadcast.ForkHeads
	blockBroadcastChan chan *broadcast.BlockAccepted
	blockStatusChan    chan *blockApplicationStatus
	tryBlockChan       chan *tryBlockApplicationRequest

	applyBlockChan       chan *applyBlockRequest
	applyTransactionChan chan *applyTransactionRequest

	opts options.ApplicatorOptions
}

func NewApplicator(ctx context.Context, rpc rpc.LocalRPC, cache *TransactionCache, opts options.ApplicatorOptions) (*Applicator, error) {
	headInfo, err := rpc.GetHeadBlock(ctx)

	if err != nil {
		return nil, err
	}

	return &Applicator{
		rpc:                  rpc,
		highestBlock:         headInfo.HeadTopology.Height,
		lib:                  headInfo.LastIrreversibleBlock,
		transactionCache:     cache,
		forkWatchdog:         NewForkWatchdog(),
		blocksById:           make(map[string]*blockEntry),
		blocksByPrevious:     make(map[string]map[string]void),
		blocksByHeight:       make(map[uint64]map[string]void),
		pendingBlocks:        make(map[string]void),
		newBlockChan:         make(chan *blockEntry, 10),
		forkHeadsChan:        make(chan *broadcast.ForkHeads, 10),
		blockBroadcastChan:   make(chan *broadcast.BlockAccepted, 10),
		blockStatusChan:      make(chan *blockApplicationStatus, 10),
		tryBlockChan:         make(chan *tryBlockApplicationRequest, 10),
		applyBlockChan:       make(chan *applyBlockRequest, 10),
		applyTransactionChan: make(chan *applyTransactionRequest, 10),
		opts:                 opts,
	}, nil
}

func (a *Applicator) validateBlock(block *protocol.Block) error {
	if block.Id == nil {
		return fmt.Errorf("%w, block id was nil", p2perrors.ErrInvalidBlock)
	}

	if block.Header == nil {
		return fmt.Errorf("%w, block header was nil", p2perrors.ErrInvalidBlock)
	}

	if block.Header.Previous == nil {
		return fmt.Errorf("%w, previous block was nil", p2perrors.ErrInvalidBlock)
	}

	return nil
}

// ApplyBlock will apply the block to the chain at the appropriate time
func (a *Applicator) ApplyBlock(ctx context.Context, block *protocol.Block) error {
	if err := a.validateBlock(block); err != nil {
		return err
	}

	if err := a.forkWatchdog.Add(block); err != nil {
		return err
	}

	errChan := make(chan error, 1)

	a.newBlockChan <- &blockEntry{block: block, errChans: []chan<- error{errChan}}

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		return p2perrors.ErrBlockApplicationTimeout
	}
}

func (a *Applicator) validateTransaction(trx *protocol.Transaction) error {
	if trx.Id == nil {
		return fmt.Errorf("%w, transaction id was nil", p2perrors.ErrInvalidTransaction)
	}

	if trx.Header == nil {
		return fmt.Errorf("%w, transaction header was nil", p2perrors.ErrInvalidTransaction)
	}

	if trx.Header.Payer == nil {
		return fmt.Errorf("%w, transaction payer was nil", p2perrors.ErrInvalidTransaction)
	}

	if trx.Header.Nonce == nil {
		return fmt.Errorf("%w, transaction nonce was nil", p2perrors.ErrInvalidTransaction)
	}

	return nil
}

func (a *Applicator) ApplyTransaction(ctx context.Context, trx *protocol.Transaction) error {
	if err := a.validateTransaction(trx); err != nil {
		return err
	}

	errChan := make(chan error, 1)

	a.applyTransactionChan <- &applyTransactionRequest{trx, errChan, ctx}

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleForkHeads handles a fork heads broadcast
func (a *Applicator) HandleForkHeads(forkHeads *broadcast.ForkHeads) {
	a.forkHeadsChan <- forkHeads
}

// HandleBlockBroadcast handles a block broadcast
func (a *Applicator) HandleBlockBroadcast(blockAccept *broadcast.BlockAccepted) {
	a.blockBroadcastChan <- blockAccept
}

func (a *Applicator) addBlockEntry(ctx context.Context, entry *blockEntry) {
	id := string(entry.block.Id)
	previousId := string(entry.block.Header.Previous)
	height := entry.block.Header.Height

	if oldEntry, ok := a.blocksById[id]; ok {
		oldEntry.errChans = append(oldEntry.errChans, entry.errChans...)
	} else {
		a.blocksById[id] = entry

		if _, ok := a.blocksByPrevious[previousId]; !ok {
			a.blocksByPrevious[previousId] = make(map[string]void)
		}
		a.blocksByPrevious[string(entry.block.Header.Previous)][id] = void{}

		if _, ok := a.blocksByHeight[height]; !ok {
			a.blocksByHeight[height] = make(map[string]void)
		}
		a.blocksByHeight[height][id] = void{}
	}

	// If the block height is greater than the highest block we have seen plus one,
	// we know we cannot apply it yet. Wait
	if entry.block.Header.Height > a.highestBlock+1 {
		return
	}

	// If the parent block is currently being applied, we cannot apply it yet. Wait
	if _, ok := a.pendingBlocks[string(entry.block.Header.Previous)]; ok {
		return
	}

	a.tryBlockApplication(ctx, entry.block, false)
}

func (a *Applicator) removeBlockEntry(ctx context.Context, id string, err error) {
	if entry, ok := a.blocksById[id]; ok {
		for _, ch := range entry.errChans {
			select {
			case ch <- err:
				close(ch)
			case <-ctx.Done():
			}
		}

		delete(a.blocksById, id)

		previousId := string(entry.block.Header.Previous)
		height := entry.block.Header.Height

		if blocks, ok := a.blocksByPrevious[previousId]; ok {
			delete(blocks, id)

			if len(a.blocksByPrevious[previousId]) == 0 {
				delete(a.blocksByPrevious, previousId)
			}
		}

		if blocks, ok := a.blocksByHeight[height]; ok {
			delete(blocks, id)

			if len(a.blocksByHeight[height]) == 0 {
				delete(a.blocksByHeight, height)
			}
		}
	}
}

func (a *Applicator) tryBlockApplication(ctx context.Context, block *protocol.Block, force bool) {
	go func() {
		select {
		case a.tryBlockChan <- &tryBlockApplicationRequest{
			block: block,
			force: force,
		}:
		case <-ctx.Done():
		}
	}()
}

func (a *Applicator) handleTryBlockApplication(ctx context.Context, request *tryBlockApplicationRequest) {
	// If there is already a pending application of the block, return
	if _, ok := a.pendingBlocks[string(request.block.Id)]; ok {
		if request.force {
			go func() {
				select {
				case <-time.After(a.opts.ForceApplicationRetryDelay):
					a.tryBlockApplication(ctx, request.block, request.force)
				case <-ctx.Done():
				}
			}()
		}

		return
	}

	a.pendingBlocks[string(request.block.Id)] = void{}

	go func() {
		errChan := make(chan error, 1)

		// If block is more than 4 seconds in the future, do not apply it until
		// it is less than 4 seconds in the future.
		applicationThreshold := time.Now().Add(a.opts.DelayThreshold)
		blockTime := time.Unix(int64(request.block.Header.Timestamp/1000), int64(request.block.Header.Timestamp%1000))

		if blockTime.After(applicationThreshold) {
			delayCtx, delayCancel := context.WithTimeout(ctx, a.opts.DelayTimeout)
			defer delayCancel()
			timerCtx, timerCancel := context.WithTimeout(ctx, blockTime.Sub(applicationThreshold))
			defer timerCancel()

			select {
			case <-timerCtx.Done():
			case <-delayCtx.Done():
				a.blockStatusChan <- &blockApplicationStatus{
					block: request.block,
					err:   ctx.Err(),
				}
				return
			case <-ctx.Done():
				return
			}
		}

		a.applyBlockChan <- &applyBlockRequest{request.block, errChan, ctx}

		select {
		case err := <-errChan:
			select {
			case a.blockStatusChan <- &blockApplicationStatus{
				block: request.block,
				err:   err,
			}:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
}

func (a *Applicator) handleBlockStatus(ctx context.Context, status *blockApplicationStatus) {
	delete(a.pendingBlocks, string(status.block.Id))

	if status.err != nil && (errors.Is(status.err, p2perrors.ErrBlockState)) {
		a.tryBlockApplication(ctx, status.block, false)
	} else if status.err == nil || !errors.Is(status.err, p2perrors.ErrUnknownPreviousBlock) {

		if status.err == nil {
			a.checkBlockChildren(ctx, string(status.block.Id))
		}

		a.removeBlockEntry(ctx, string(status.block.Id), status.err)
	}
}

func (a *Applicator) handleNewBlock(ctx context.Context, entry *blockEntry) {
	var err error

	if entry.block.Header.Height > a.highestBlock+a.opts.MaxHeightDelta {
		err = p2perrors.ErrMaxHeight
	} else if len(a.blocksById) >= int(a.opts.MaxPendingBlocks) {
		err = p2perrors.ErrMaxPendingBlocks
	} else if entry.block.Header.Height <= a.lib {
		err = p2perrors.ErrBlockIrreversibility
	}

	if err != nil {
		for _, ch := range entry.errChans {
			select {
			case ch <- err:
				close(ch)
			case <-ctx.Done():
			}
		}
	} else {
		a.addBlockEntry(ctx, entry)
	}
}

func (a *Applicator) checkBlockChildren(ctx context.Context, blockID string) {
	if children, ok := a.blocksByPrevious[string(blockID)]; ok {
		for id := range children {
			if entry, ok := a.blocksById[id]; ok {
				force := time.Since(time.UnixMilli(int64(entry.block.Header.Timestamp))) < a.opts.ForceChildRequestThreshold
				a.tryBlockApplication(ctx, entry.block, force)
			}
		}
	}
}

func (a *Applicator) handleForkHeads(ctx context.Context, forkHeads *broadcast.ForkHeads) {
	oldLib := a.lib
	atomic.StoreUint64(&a.lib, forkHeads.LastIrreversibleBlock.Height)

	a.forkWatchdog.Purge(forkHeads.LastIrreversibleBlock.Height)

	for _, head := range forkHeads.Heads {
		a.checkBlockChildren(ctx, string(head.Id))
	}

	// Blocks at or before LIB are automatically rejected, so all entries have height
	// greater than previous LIA. We check every block height greater than old LIB,
	// up to and including the current LIB and remove their entry.
	// Some blocks may be on unreachable forks at this point. That's ok.
	// Because they are not reachable, we will never get a parent block that causes their
	// application and they'll be cleaned up using this logic once their height passes
	// beyond irreversibility
	for h := oldLib + 1; h <= forkHeads.LastIrreversibleBlock.Height; h++ {
		if ids, ok := a.blocksByHeight[h]; ok {
			for id := range ids {
				a.removeBlockEntry(ctx, id, p2perrors.ErrBlockIrreversibility)
			}
		}
	}
}

func (a *Applicator) handleBlockBroadcast(ctx context.Context, blockAccept *broadcast.BlockAccepted) {
	a.transactionCache.CheckBlock(blockAccept.Block)

	// It is not possible for a block with a new highest height to not be head, so this check is sufficient
	if blockAccept.Block.Header.Height > a.highestBlock {
		a.highestBlock = blockAccept.Block.Header.Height
	}

	a.checkBlockChildren(ctx, string(blockAccept.Block.Id))
}

func (a *Applicator) handleApplyBlock(request *applyBlockRequest) {
	var err error
	if request.block.Header.Height <= atomic.LoadUint64(&a.lib) {
		err = p2perrors.ErrBlockIrreversibility
	} else {
		_, err = a.rpc.ApplyBlock(request.ctx, request.block)
	}

	request.errChan <- err
	close(request.errChan)
}

func (a *Applicator) handleApplyTransaction(request *applyTransactionRequest) {
	var err error
	if a.transactionCache.CheckTransactions(request.trx) == 0 {
		_, err = a.rpc.ApplyTransaction(request.ctx, request.trx)
	}

	request.errChan <- err
	close(request.errChan)
}

func (a *Applicator) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case status := <-a.blockStatusChan:
				a.handleBlockStatus(ctx, status)
			case entry := <-a.newBlockChan:
				a.handleNewBlock(ctx, entry)
			case forkHeads := <-a.forkHeadsChan:
				a.handleForkHeads(ctx, forkHeads)
			case blockBroadcast := <-a.blockBroadcastChan:
				a.handleBlockBroadcast(ctx, blockBroadcast)
			case tryApplyBlock := <-a.tryBlockChan:
				a.handleTryBlockApplication(ctx, tryApplyBlock)

			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < a.opts.ApplicationJobs; i++ {
		go func() {
			for {
				select {
				case request := <-a.applyBlockChan:
					a.handleApplyBlock(request)
				default:
					select {
					case request := <-a.applyBlockChan:
						a.handleApplyBlock(request)
					case request := <-a.applyTransactionChan:
						a.handleApplyTransaction(request)
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
}
