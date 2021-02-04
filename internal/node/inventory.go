package node

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	types "github.com/koinos/koinos-types-golang"
)

// ExpirationTime is the amount of time an inventory item will stay in the
const ExpirationTime = time.Minute * time.Duration(1)

// InventoryKey is a comparable version of multihash, to be used as a map key
type InventoryKey struct {
	ID     uint64
	Digest string
}

func newInventoryKey(mh *types.Multihash) *InventoryKey {
	ik := InventoryKey{ID: uint64(mh.ID)}
	ik.Digest = hex.EncodeToString(mh.Digest)
	return &ik
}

// InventoryItem contains the inventory item and the set of peers known to already have this item
type InventoryItem struct {
	ID        types.Multihash
	Item      interface{} // The actual item
	TimeStamp time.Time
}

// NewInventoryItem creates a new inventory item object with the timestamp set to the current time
func NewInventoryItem(mh types.Multihash, item interface{}) *InventoryItem {
	ii := InventoryItem{Item: item, TimeStamp: time.Now()}
	return &ii
}

// InventoryStore contains the storage logic for a single type of object
type InventoryStore struct {
	items      map[InventoryKey]InventoryItem
	inventory  *Inventory
	mu         sync.Mutex
	expiration time.Duration
}

func newInventoryStore(expiration time.Duration) *InventoryStore {
	store := InventoryStore{expiration: expiration}
	store.items = make(map[InventoryKey]InventoryItem)

	return &store
}

// Add adds an item to the store, keyed to its ID
func (store *InventoryStore) Add(item *InventoryItem) error {
	ik := newInventoryKey(&item.ID)

	store.mu.Lock()
	store.items[*ik] = *item
	store.mu.Unlock()

	store.pruneAsync()

	return nil
}

// Checks for the existence of the given key
// Assumes the store is locked by the invoker
func (store *InventoryStore) contains(ik *InventoryKey) bool {
	_, ok := store.items[*ik]
	return ok
}

// Contains check whether or not the store contains an item with the given ID
func (store *InventoryStore) Contains(id *types.Multihash) bool {
	ik := newInventoryKey(id)

	store.mu.Lock()
	c := store.contains(ik)
	store.mu.Unlock()

	return c
}

// Fetch attempts to fetch an item with the given ID. Returns an error if it cannot
func (store *InventoryStore) Fetch(id *types.Multihash) (*InventoryItem, error) {
	store.mu.Lock()
	ik := newInventoryKey(id)
	if !store.contains(ik) {
		store.mu.Unlock()
		return nil, errors.New("Requested item not found in inventory")
	}

	item := store.items[*ik]
	store.mu.Unlock()

	store.pruneAsync()

	return &item, nil
}

// pruneAsync asynchronously prunes the store
func (store *InventoryStore) pruneAsync() {
	go func() {
		store.Prune()
	}()
}

// Prunes the store based on the expiration time
// Assumes the store is locked by the invoker
func (store *InventoryStore) prune() {
	now := time.Now().Add(-store.expiration) // Time to test against timestamp
	remove := make([]InventoryKey, 0)        // Slice of keys to be removed

	for k, v := range store.items {
		if now.After(v.TimeStamp) {
			remove = append(remove, k)
		}
	}

	for _, k := range remove {
		delete(store.items, k)
	}
}

// Prune synchronously prunes the store
func (store *InventoryStore) Prune() {
	store.mu.Lock()
	store.prune()
	store.mu.Unlock()
}

// Inventory represents the current inventory of a node
type Inventory struct {
	Transactions InventoryStore
	Blocks       InventoryStore
}

// NewInventory creates a new Inventory object with the given expiration
func NewInventory(expiration time.Duration) *Inventory {
	inv := Inventory{}
	inv.Transactions = *newInventoryStore(expiration)
	inv.Blocks = *newInventoryStore(expiration)

	return &inv
}
