package util

import (
	"encoding/json"
	"fmt"

	types "github.com/koinos/koinos-types-golang"
)

// BlockString returns a string containing the given block's height and ID
func BlockString(block *types.Block) string {
	id, _ := json.Marshal(block.ID)
	return fmt.Sprintf("Height: %d, ID: %s", block.Header.Height, string(id))
}

// TransactionString returns a string containing the given transaction's ID
func TransactionString(transaction *types.Transaction) string {
	id, _ := json.Marshal(transaction.ID)
	return fmt.Sprintf("ID: %s", string(id))
}
