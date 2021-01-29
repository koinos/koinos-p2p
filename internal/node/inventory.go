package node

// InventoryItem contains the inventory item and the set of peers known to already have this item
type InventoryItem struct {
	Item  interface{}       // The actual item
	Peers map[string]string // List of peers to whom this item has been broadcast
}

// Inventory represents the current inventory of a node
type Inventory struct {
	Transactions map[string]InventoryItem
	Blocks       map[string]InventoryItem
}
