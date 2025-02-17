package p2p

import (
	"testing"
	"time"

	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"github.com/stretchr/testify/assert"
)

func makeTestTxn(id string) *protocol.Transaction {
	return &protocol.Transaction{Id: []byte(id)}
}

func TestAddTransaction(t *testing.T) {
	tc := NewTransactionCache(time.Minute)
	// Add some transactions in time order
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"1", time.Unix(1000, 1000)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"2", time.Unix(2000, 1000)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"3", time.Unix(3000, 1000)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"4", time.Unix(4000, 1001)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"5", time.Unix(5000, 1002)}) })

	assert.Equal(t, 5, len(tc.transactionItems))
	assert.Equal(t, 5, len(tc.transactionMap))

	// Add a transaction that is the same age at the latest
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"6", time.Unix(5000, 1002)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"7", time.Unix(5000, 1002)}) })
	assert.NotPanics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"8", time.Unix(5000, 1002)}) })

	assert.Equal(t, 8, len(tc.transactionItems))
	assert.Equal(t, 8, len(tc.transactionMap))

	// Add a transaction that is older than the oldest transaction and make sure it panics
	assert.Panics(t, func() { tc.addTransactionItem(&TransactionCacheItem{"9", time.Unix(1000, 1000)}) })
}

func TestCheckTransaction(t *testing.T) {
	tc := NewTransactionCache(time.Minute)

	// Check some transaction that are not in the cache
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("1")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("2")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("3")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("4")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("5")))

	assert.Equal(t, 0, len(tc.transactionItems))
	assert.Equal(t, 0, len(tc.transactionMap))

	// Add some transactions that are in the cache
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("1")))
	assert.Equal(t, 1, tc.CheckTransactions(makeTestTxn("1")))
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("2")))
	assert.Equal(t, 1, tc.CheckTransactions(makeTestTxn("2")))
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("3")))
	assert.Equal(t, 1, tc.CheckTransactions(makeTestTxn("3")))
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("4")))
	assert.Equal(t, 1, tc.CheckTransactions(makeTestTxn("4")))
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("5")))
	assert.Equal(t, 1, tc.CheckTransactions(makeTestTxn("5")))

	assert.Equal(t, 5, len(tc.transactionItems))
	assert.Equal(t, 5, len(tc.transactionMap))

	// Add a few more that are not in the cache
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("6")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("7")))

	assert.Equal(t, 5, len(tc.transactionItems))
	assert.Equal(t, 5, len(tc.transactionMap))

	// Test that multiple transactions are checked properly
	assert.Equal(t, 3, tc.CheckTransactions(makeTestTxn("1"), makeTestTxn("2"), makeTestTxn("3")))
	assert.Equal(t, 2, tc.CheckTransactions(makeTestTxn("4"), makeTestTxn("5"), makeTestTxn("6")))
	assert.Equal(t, 2, tc.AddTransactions(makeTestTxn("4"), makeTestTxn("5"), makeTestTxn("6")))
	assert.Equal(t, 3, tc.CheckTransactions(makeTestTxn("4"), makeTestTxn("5"), makeTestTxn("6")))
	assert.Equal(t, 0, tc.CheckTransactions(makeTestTxn("7"), makeTestTxn("8"), makeTestTxn("9")))
	assert.Equal(t, 0, tc.AddTransactions(makeTestTxn("7"), makeTestTxn("8"), makeTestTxn("9")))
	assert.Equal(t, 3, tc.CheckTransactions(makeTestTxn("7"), makeTestTxn("8"), makeTestTxn("9")))
}

func TestCachePrune(t *testing.T) {
	tc := NewTransactionCache(time.Minute)

	// Add some transactions in time order
	tc.addTransactionItem(&TransactionCacheItem{"1", time.Unix(1000, 1000)})
	tc.addTransactionItem(&TransactionCacheItem{"2", time.Unix(2000, 1000)})
	tc.addTransactionItem(&TransactionCacheItem{"3", time.Unix(3000, 1000)})
	tc.addTransactionItem(&TransactionCacheItem{"4", time.Unix(4000, 1000)})
	tc.addTransactionItem(&TransactionCacheItem{"5", time.Unix(5000, 1000)})

	assert.Equal(t, 5, len(tc.transactionItems))

	// Prune the cache right after txn 2
	tc.pruneTransactions(time.Unix(2000, 1001))

	// Transaction 1 should have been pruned out, while 2 stays since it is well within the 1 minute window specified
	assert.Equal(t, 4, len(tc.transactionItems))
	assert.Equal(t, 4, len(tc.transactionMap))
	assert.NotContains(t, tc.transactionMap, "1")
	assert.Contains(t, tc.transactionMap, "2")
	assert.Contains(t, tc.transactionMap, "3")
	assert.Contains(t, tc.transactionMap, "4")
	assert.Contains(t, tc.transactionMap, "5")

	// Prune the cache right after txn 2
	tc.pruneTransactions(time.Unix(2000, 1001))

	// Ensure nothing has been pruned out
	assert.Equal(t, 4, len(tc.transactionItems))
	assert.Equal(t, 4, len(tc.transactionMap))

	// Prune the cache a bit more than a minute after txn 3
	tc.pruneTransactions(time.Unix(3000, 1000).Add(time.Minute).Add(time.Second))

	assert.Equal(t, 2, len(tc.transactionItems))
	assert.Equal(t, 2, len(tc.transactionMap))

	// Prune the cache far into the future of any transaction
	tc.pruneTransactions(time.Unix(5000, 1000).Add(time.Hour))

	// Ensure it is now empty
	assert.Equal(t, 0, len(tc.transactionItems))
	assert.Equal(t, 0, len(tc.transactionMap))
}
