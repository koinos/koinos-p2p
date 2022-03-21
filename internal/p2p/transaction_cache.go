package p2p

import (
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	util "github.com/koinos/koinos-util-golang"
)

// TransactionCacheItem is a an item in the transaction cache
type TransactionCacheItem struct {
	transaction *protocol.Transaction
	timeAdded   time.Time
}

// TransactionCache is a cache of recently received transactions
type TransactionCache struct {
	transactionMap   map[string]*protocol.Transaction
	transactionItems []*TransactionCacheItem
	cacheDuration    time.Duration
	mu               sync.Mutex
}

// NewTransactionCache creates a new transaction cache
func NewTransactionCache(cacheDuration time.Duration) *TransactionCache {
	txc := &TransactionCache{
		transactionMap:   make(map[string]*protocol.Transaction),
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

	txc.transactionMap[string(item.transaction.Id)] = item.transaction
	txc.transactionItems = append(txc.transactionItems, item)

	log.Debugf("TransactionCache.addTransactionItem: added transaction to cache: %s", util.TransactionString(item.transaction))
	log.Debugf("Items currently in transaction cache: %d", len(txc.transactionItems))
}

// CheckTransaction returns true if the transaction is in the cache
func (txc *TransactionCache) CheckTransaction(transaction *protocol.Transaction) bool {
	// Lock this entire function
	txc.mu.Lock()
	defer txc.mu.Unlock()

	now := time.Now()

	// First prune the transactions
	txc.pruneTransactions(now)

	// Check if the transaction is in the cache
	if _, ok := txc.transactionMap[string(transaction.Id)]; ok {
		return true
	}

	// Insert the transaction into the cache
	txc.addTransactionItem(&TransactionCacheItem{transaction: transaction, timeAdded: now})

	return false
}

func (txc *TransactionCache) pruneTransactions(pruneTime time.Time) {
	pruneCount := 0
	for i := 0; i < len(txc.transactionItems); i++ {
		if pruneTime.Sub(txc.transactionItems[i].timeAdded) > txc.cacheDuration {
			delete(txc.transactionMap, string(txc.transactionItems[i].transaction.Id))
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
