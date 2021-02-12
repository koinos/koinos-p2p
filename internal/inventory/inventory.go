package inventory

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	types "github.com/koinos/koinos-types-golang"
)

// ExpirationTime is the amount of time an inventory item will stay in the
const ExpirationTime = time.Minute * time.Duration(1)

// Key is a comparable version of multihash, to be used as a map key
type Key struct {
	ID     uint64
	Digest string
}

func newInventoryKey(mh *types.Multihash) *Key {
	ik := Key{ID: uint64(mh.ID)}
	ik.Digest = hex.EncodeToString(mh.Digest)
	return &ik
}

// Item contains the inventory item and the set of peers known to already have this item
type Item struct {
	ID        types.Multihash
	Item      interface{} // The actual item
	TimeStamp time.Time
}

// NewInventoryItem creates a new inventory item object with the timestamp set to the current time
func NewInventoryItem(mh types.Multihash, item interface{}) *Item {
	ii := Item{ID: mh, Item: item, TimeStamp: time.Now()}
	return &ii
}

// Store contains the storage logic for a single type of object
type Store struct {
	items      map[Key]Item
	inventory  *Inventory
	mu         sync.Mutex
	expiration time.Duration
}

func newInventoryStore(inventory *Inventory, expiration time.Duration) *Store {
	store := Store{expiration: expiration, inventory: inventory}
	store.items = make(map[Key]Item)

	return &store
}

// Add adds an item to the store, keyed to its ID
func (store *Store) Add(item *Item) error {
	ik := newInventoryKey(&item.ID)

	store.mu.Lock()
	store.items[*ik] = *item
	store.mu.Unlock()

	store.inventory.onAdd(item)
	store.pruneAsync()

	return nil
}

// Checks for the existence of the given key
// Assumes the store is locked by the invoker
func (store *Store) contains(ik *Key) bool {
	_, ok := store.items[*ik]
	return ok
}

// Contains check whether or not the store contains an item with the given ID
func (store *Store) Contains(id *types.Multihash) bool {
	ik := newInventoryKey(id)

	store.mu.Lock()
	c := store.contains(ik)
	store.mu.Unlock()

	return c
}

// Fetch attempts to fetch an item with the given ID. Returns an error if it cannot
func (store *Store) Fetch(id *types.Multihash) (*Item, error) {
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
func (store *Store) pruneAsync() {
	go func() {
		store.Prune()
	}()
}

// Prunes the store based on the expiration time
// Assumes the store is locked by the invoker
func (store *Store) prune() {
	now := time.Now().Add(-store.expiration) // Time to test against timestamp
	remove := make([]Key, 0)                 // Slice of keys to be removed

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
func (store *Store) Prune() {
	store.mu.Lock()
	store.prune()
	store.mu.Unlock()
}

// Inventory represents the current inventory of a node
type Inventory struct {
	Transactions   Store
	Blocks         Store
	mu             sync.Mutex
	gossipChannels map[chan<- *Item]bool
}

// NewInventory creates a new Inventory object with the given expiration
func NewInventory(expiration time.Duration) *Inventory {
	inv := Inventory{}
	inv.Transactions = *newInventoryStore(&inv, expiration)
	inv.Blocks = *newInventoryStore(&inv, expiration)
	inv.gossipChannels = make(map[chan<- *Item]bool)

	return &inv
}

// EnableGossipChannel enables the gossip channel, which is a channel that will be written to when a new item enters the inventory
func (inv *Inventory) EnableGossipChannel(ch chan<- *Item) {
	if ch == nil {
		panic("nil channel not allowed")
	}

	inv.gossipChannels[ch] = true
}

// DisableGossipChannel disables the gossip channel
func (inv *Inventory) DisableGossipChannel(ch chan<- *Item) {
	inv.mu.Lock()
	if _, ok := inv.gossipChannels[ch]; !ok {
		inv.mu.Unlock()
		return
	}

	delete(inv.gossipChannels, ch)
	inv.mu.Unlock()
}

func (inv *Inventory) onAdd(item *Item) {
	inv.mu.Lock()

	for ch := range inv.gossipChannels {
		ch <- item
	}

	inv.mu.Unlock()
}
