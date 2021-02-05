package node

import (
	"crypto/md5"
	"testing"
	"time"

	types "github.com/koinos/koinos-types-golang"
)

// Create a test item, using a serializeable payload
func makeTestItem(payload types.Serializeable) *InventoryItem {
	mh := types.NewMultihash()
	vb := types.NewVariableBlob()
	sum := md5.Sum(*payload.Serialize(vb))
	mh.Digest = types.VariableBlob(sum[:])

	ii := NewInventoryItem(*mh, payload)

	return ii
}

func checkItemNumbers(t *testing.T, inv *Inventory, blocks int, transactions int) {
	if len(inv.Blocks.items) != blocks {
		t.Errorf("There should be a %d blocks in the inventory, but found %d", blocks, len(inv.Blocks.items))
	}

	if len(inv.Transactions.items) != transactions {
		t.Errorf("There should be a %d transactions in the inventory, but found %d", transactions, len(inv.Transactions.items))
	}
}

func TestInventoryExpiration(t *testing.T) {
	inv := NewInventory(time.Duration(20) * time.Millisecond)

	// Create and add a test transaction
	it := makeTestItem(*types.NewTransaction())
	inv.Transactions.Add(it)

	// Create and add a test Block
	ib := makeTestItem(*types.NewBlock())
	inv.Blocks.Add(ib)

	// Fetch one of the transactions, make sure it is correct
	txn, err := inv.Transactions.Fetch(&it.ID)
	if err != nil {
		t.Error(err)
	}
	if !txn.ID.Equals(&it.ID) {
		t.Errorf("Fetched transaction ID mismatch")
	}

	// Ensure Contains returns true
	if !(inv.Transactions.Contains(&it.ID)) {
		t.Errorf("Inventory.Contains returning false")
	}

	// Fetch one of the blocks, make sure it is correct
	block, err := inv.Blocks.Fetch(&ib.ID)
	if err != nil {
		t.Error(err)
	}
	if !block.ID.Equals(&ib.ID) {
		t.Errorf("Fetched block ID mismatch")
	}

	// Ensure Contains returns true
	if !(inv.Blocks.Contains(&ib.ID)) {
		t.Errorf("Inventory.Contains returning false")
	}

	// Make sure the entries counts are correct
	checkItemNumbers(t, inv, 1, 1)

	// Sleep for less than the expiration time, prune, then verify
	time.Sleep(time.Millisecond * time.Duration(2))
	inv.Blocks.Prune()
	inv.Transactions.Prune()
	checkItemNumbers(t, inv, 1, 1)

	time.Sleep(time.Millisecond * time.Duration(21))
	inv.Transactions.Prune()
	checkItemNumbers(t, inv, 1, 0)
	inv.Blocks.Prune()
	checkItemNumbers(t, inv, 0, 0)

	// Make sure attempting to fetch the transaction gives an error
	txn, err = inv.Transactions.Fetch(&it.ID)
	if err == nil {
		t.Error("Fetching an expired transaction did not return an error")
	}

	// Make sure attempting to fetch the transaction gives an error
	block, err = inv.Blocks.Fetch(&ib.ID)
	if err == nil {
		t.Error("Fetching an expired block did not return an error")
	}

	// Ensure Contains returns false
	if inv.Transactions.Contains(&it.ID) {
		t.Errorf("Inventory.Contains wrongly returning true")
	}

	// Ensure Contains returns false
	if inv.Blocks.Contains(&ib.ID) {
		t.Errorf("Inventory.Contains wrongly returning true")
	}
}
