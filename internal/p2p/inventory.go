package p2p

type InventoryItem struct {
	Item  interface{}       // The actual item
	Peers map[string]string // List of peers to whom this item has been broadcast
}

type NodeInventory struct {
	Transactions map[string]InventoryItem
	Blocks       map[string]InventoryItem
}
