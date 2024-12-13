package p2p

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/v2/koinos/chain"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"google.golang.org/protobuf/proto"
)

type blockEntry struct {
	block    *protocol.Block
	errChans []chan<- error
}

type transactionEntry struct {
	transaction *protocol.Transaction
	errChans    []chan<- error
}

type tryBlockApplicationRequest struct {
	block *protocol.Block
	force bool
}

type tryTransactionApplicationRequest struct {
	transaction *protocol.Transaction
	force       bool
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
	transaction *protocol.Transaction
	errChan     chan<- error
	ctx         context.Context
}

type transactionApplicationStatus struct {
	transaction *protocol.Transaction
	err         error
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

	transactionsById         map[string]*transactionEntry
	transactionsByPayeeNonce map[string]map[string]void
	pendingTransactions      map[string]void

	newBlockChan             chan *blockEntry
	newTransactionChan       chan *transactionEntry
	forkHeadsChan            chan *broadcast.ForkHeads
	blockBroadcastChan       chan *broadcast.BlockAccepted
	transactionBroadcastChan chan *broadcast.TransactionAccepted
	blockStatusChan          chan *blockApplicationStatus
	transactionStatusChan    chan *transactionApplicationStatus
	tryBlockChan             chan *tryBlockApplicationRequest
	tryTransactionChan       chan *tryTransactionApplicationRequest
	removeTransactionChan    chan string

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
		rpc:                      rpc,
		highestBlock:             headInfo.HeadTopology.Height,
		lib:                      headInfo.LastIrreversibleBlock,
		transactionCache:         cache,
		forkWatchdog:             NewForkWatchdog(),
		blocksById:               make(map[string]*blockEntry),
		blocksByPrevious:         make(map[string]map[string]void),
		blocksByHeight:           make(map[uint64]map[string]void),
		pendingBlocks:            make(map[string]void),
		transactionsById:         make(map[string]*transactionEntry),
		transactionsByPayeeNonce: make(map[string]map[string]void),
		pendingTransactions:      make(map[string]void),
		newBlockChan:             make(chan *blockEntry, 10),
		newTransactionChan:       make(chan *transactionEntry, 10),
		forkHeadsChan:            make(chan *broadcast.ForkHeads, 10),
		blockBroadcastChan:       make(chan *broadcast.BlockAccepted, 10),
		transactionBroadcastChan: make(chan *broadcast.TransactionAccepted, 10),
		blockStatusChan:          make(chan *blockApplicationStatus, 10),
		transactionStatusChan:    make(chan *transactionApplicationStatus, 10),
		tryBlockChan:             make(chan *tryBlockApplicationRequest, 10),
		tryTransactionChan:       make(chan *tryTransactionApplicationRequest, 10),
		removeTransactionChan:    make(chan string),
		applyBlockChan:           make(chan *applyBlockRequest, 10),
		applyTransactionChan:     make(chan *applyTransactionRequest, 10),
		opts:                     opts,
	}, nil
}

// ApplyBlock will apply the block to the chain at the appropriate time
func (a *Applicator) ApplyBlock(ctx context.Context, block *protocol.Block) error {
	err := a.forkWatchdog.Add(block)
	if err != nil {
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

func (a *Applicator) ApplyTransaction(ctx context.Context, transaction *protocol.Transaction) error {
	errChan := make(chan error, 1)

	a.newTransactionChan <- &transactionEntry{transaction: transaction, errChans: []chan<- error{errChan}}

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		return p2perrors.ErrTransactionApplicationTimeout
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

// HandleTransactionBroadcast handles a transaction broadcast
func (a *Applicator) HandleTransactionBroadcast(transactionAccept *broadcast.TransactionAccepted) {
	a.transactionBroadcastChan <- transactionAccept
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

func (a *Applicator) addTransactionEntry(ctx context.Context, entry *transactionEntry) {
	id := string(entry.transaction.Id)

	// If the transaction is already known, add the error channels and return
	if oldEntry, ok := a.transactionsById[id]; ok {
		oldEntry.errChans = append(oldEntry.errChans, entry.errChans...)
		return
	} else {
		a.transactionsById[id] = entry
	}

	// Get the payee from the transaction and append the nonce
	payee := string(entry.transaction.Header.Payer)
	if entry.transaction.Header.Payee != nil {
		payee = string(entry.transaction.Header.Payee)
	} else {
		a.transactionsById[id] = entry
	}

	payeeNonce := payee + string(entry.transaction.Header.Nonce)

	// Record the transaction by the payee and the nonce
	if _, ok := a.transactionsByPayeeNonce[payeeNonce]; !ok {
		a.transactionsByPayeeNonce[payeeNonce] = make(map[string]void)
	}
	a.transactionsByPayeeNonce[payeeNonce][id] = void{}

	// Automatically expire the transaction after 30 seconds
	go func() {
		select {
		case <-time.After(a.opts.TransactionExpiration):
			select {
			case a.transactionStatusChan <- &transactionApplicationStatus{entry.transaction, p2perrors.ErrTransactionApplicationTimeout}:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()

	// Decrement the nonce by one to get the previous nonce
	nonce := &chain.ValueType{}
	err := proto.Unmarshal(entry.transaction.Header.Nonce, nonce)
	if err != nil {
		return
	}

	nonceValue := nonce.GetUint64Value() - 1
	nonce.Kind = &chain.ValueType_Uint64Value{Uint64Value: nonceValue}
	prevNonce, err := proto.Marshal(nonce)
	if err != nil {
		return
	}

	prevPayeeNonce := payee + string(prevNonce)

	// If we know the previous nonce transaction exists, do not try application right now
	if _, ok := a.transactionsByPayeeNonce[prevPayeeNonce]; ok {
		return
	}

	a.tryTransactionApplication(ctx, entry.transaction, false)
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

func (a *Applicator) removeTransactionEntry(ctx context.Context, id string, err error) {
	if entry, ok := a.transactionsById[id]; ok {
		for _, ch := range entry.errChans {
			defer close(ch)
			select {
			case ch <- err:
			case <-ctx.Done():
			}
		}

		delete(a.transactionsById, id)

		payee := string(entry.transaction.Header.Payer)
		if entry.transaction.Header.Payee != nil {
			payee = string(entry.transaction.Header.Payee)
		}

		payeeNonce := payee + string(entry.transaction.Header.Nonce)

		if transactions, ok := a.transactionsByPayeeNonce[payeeNonce]; ok {
			delete(transactions, id)

			if len(transactions) == 0 {
				delete(a.transactionsByPayeeNonce, payeeNonce)
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

func (a *Applicator) tryTransactionApplication(ctx context.Context, transaction *protocol.Transaction, force bool) {
	go func() {
		select {
		case a.tryTransactionChan <- &tryTransactionApplicationRequest{
			transaction: transaction,
			force:       force,
		}:
		case <-ctx.Done():
		}
	}()
}

func (a *Applicator) handleTryTransactionApplication(ctx context.Context, request *tryTransactionApplicationRequest) {
	if _, ok := a.pendingTransactions[string(request.transaction.Id)]; ok {
		if request.force {
			go func() {
				select {
				case <-time.After(a.opts.ForceApplicationRetryDelay):
					a.tryTransactionApplication(ctx, request.transaction, request.force)
				case <-ctx.Done():
				}
			}()
		}

		return
	}

	a.pendingTransactions[string(request.transaction.Id)] = void{}

	go func() {
		errChan := make(chan error, 1)

		select {
		case a.applyTransactionChan <- &applyTransactionRequest{request.transaction, errChan, ctx}:
		case <-ctx.Done():
			return
		}

		select {
		case err := <-errChan:
			select {
			case a.transactionStatusChan <- &transactionApplicationStatus{request.transaction, err}:
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

func (a *Applicator) handleTransactionStatus(ctx context.Context, status *transactionApplicationStatus) {
	delete(a.pendingTransactions, string(status.transaction.Id))

	if status.err == nil {
		a.transactionCache.AddTransactions(status.transaction)
		a.checkTransactionChildren(ctx, status.transaction)
	} else if errors.Is(status.err, p2perrors.ErrInvalidNonce) {
		return
	}

	a.removeTransactionEntry(ctx, string(status.transaction.Id), status.err)
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

func (a *Applicator) handleNewTransaction(ctx context.Context, entry *transactionEntry) {
	var err error

	if len(a.transactionsById) >= int(a.opts.MaxPendingTransactions) {
		err = p2perrors.ErrMaxPendingTransactions
	} else {
		a.addTransactionEntry(ctx, entry)
	}

	if err != nil {
		for _, ch := range entry.errChans {
			defer close(ch)
			select {
			case ch <- err:
			case <-ctx.Done():
			}
		}
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

func (a *Applicator) checkTransactionChildren(ctx context.Context, transaction *protocol.Transaction) {
	payee := string(transaction.Header.Payer)
	if transaction.Header.Payee != nil {
		payee = string(transaction.Header.Payee)
	}

	// Increment the nonce by one to get the next nonce
	nonce := &chain.ValueType{}
	err := proto.Unmarshal(transaction.Header.Nonce, nonce)
	if err != nil {
		return
	}

	nonceValue := nonce.GetUint64Value() + 1
	nonce.Kind = &chain.ValueType_Uint64Value{Uint64Value: nonceValue}
	nextNonce, err := proto.Marshal(nonce)
	if err != nil {
		return
	}

	nextPayeeNonce := payee + string(nextNonce)

	if children, ok := a.transactionsByPayeeNonce[nextPayeeNonce]; ok {
		for id := range children {
			if entry, ok := a.transactionsById[id]; ok {
				a.tryTransactionApplication(ctx, entry.transaction, true)
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
	// greater than previous LIB. We check every block height greater than old LIB,
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

	for _, transaction := range blockAccept.Block.Transactions {
		a.checkTransactionChildren(ctx, transaction)
	}
}

func (a *Applicator) handleTransactionBroadcast(ctx context.Context, transactionAccept *broadcast.TransactionAccepted) {
	a.transactionCache.AddTransactions(transactionAccept.Transaction)
	a.checkTransactionChildren(ctx, transactionAccept.Transaction)
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
	if a.transactionCache.CheckTransactions(request.transaction) == 0 {
		_, err = a.rpc.ApplyTransaction(request.ctx, request.transaction)
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
			case status := <-a.transactionStatusChan:
				a.handleTransactionStatus(ctx, status)
			case entry := <-a.newBlockChan:
				a.handleNewBlock(ctx, entry)
			case entry := <-a.newTransactionChan:
				a.handleNewTransaction(ctx, entry)
			case forkHeads := <-a.forkHeadsChan:
				a.handleForkHeads(ctx, forkHeads)
			case blockBroadcast := <-a.blockBroadcastChan:
				a.handleBlockBroadcast(ctx, blockBroadcast)
			case transactionBroadcast := <-a.transactionBroadcastChan:
				a.handleTransactionBroadcast(ctx, transactionBroadcast)
			case tryApplyBlock := <-a.tryBlockChan:
				a.handleTryBlockApplication(ctx, tryApplyBlock)
			case tryApplyTransaction := <-a.tryTransactionChan:
				a.handleTryTransactionApplication(ctx, tryApplyTransaction)

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
