package p2p

import (
	"encoding/hex"
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
)

type void struct{}

// TransactionCacheItem is a an item in the transaction cache
type TransactionCacheItem struct {
	transactionID string
	timeAdded     time.Time
}

// TransactionCache is a cache of recently received transactions
type TransactionCache struct {
	transactionMap   map[string]void
	transactionItems []*TransactionCacheItem
	cacheDuration    time.Duration
	mu               sync.Mutex
}

// NewTransactionCache creates a new transaction cache
func NewTransactionCache(cacheDuration time.Duration) *TransactionCache {
	txc := &TransactionCache{
		transactionMap:   make(map[string]void),
		transactionItems: make([]*TransactionCacheItem, 0),
		cacheDuration:    cacheDuration,
	}

	return txc
}

func (txc *TransactionCache) addTransactionItem(item *TransactionCacheItem) {
	// Maintain the constraint that the transaction cache is sorted by time added
	// Since this is not user facing code, we issue a panic, as this should never happen
	numItems := len(txc.transactionItems)
	older := numItems != 0 && item.timeAdded.Before(txc.transactionItems[numItems-1].timeAdded)
	if older {
		panic("TransactionCache.addTransactionItem: transaction is older than the last transaction")
	}

	txc.transactionMap[string(item.transactionID)] = void{}
	txc.transactionItems = append(txc.transactionItems, item)

	log.Debugf("TransactionCache.addTransactionItem: added transaction to cache: %s", hex.EncodeToString([]byte(item.transactionID)))
	log.Debugf("Items currently in transaction cache: %d", len(txc.transactionItems))
}

// CheckTransactions returns the number of transactions that are in the cache
func (txc *TransactionCache) CheckTransactions(transactions ...*protocol.Transaction) int {
	// Lock this entire function
	txc.mu.Lock()
	defer txc.mu.Unlock()

	now := time.Now()

	// First prune the transactions
	txc.pruneTransactions(now)

	count := 0
	for _, tx := range transactions {
		id := string(tx.Id)
		// Check if the transaction is in the cache
		if _, ok := txc.transactionMap[id]; ok {
			// Increase count if present
			count++
		} else {
			// Insert into the cache if not present
			txc.addTransactionItem(&TransactionCacheItem{
				transactionID: string(id),
				timeAdded:     now,
			})
		}
	}

	return count
}

// CheckBlock is a helper function to check transactions in a block
func (txc *TransactionCache) CheckBlock(block *protocol.Block) int {
	return txc.CheckTransactions(block.Transactions...)
}

func (txc *TransactionCache) pruneTransactions(pruneTime time.Time) {
	pruneCount := 0
	for i := 0; i < len(txc.transactionItems); i++ {
		if pruneTime.Sub(txc.transactionItems[i].timeAdded) > txc.cacheDuration {
			delete(txc.transactionMap, txc.transactionItems[i].transactionID)
			pruneCount++
		}
	}

	// Prune the transaction items
	if pruneCount > 0 {
		txc.transactionItems = txc.transactionItems[pruneCount:]
		log.Debugf("TransactionCache.pruneTransactions: pruned %d transactions", pruneCount)
		log.Debugf("Items currently in transaction cache: %d", len(txc.transactionItems))
	}
}
