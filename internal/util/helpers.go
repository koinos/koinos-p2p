package util

import (
	"encoding/json"
	"fmt"

	types "github.com/koinos/koinos-types-golang"
)

// BlockString returns a string containing the given block's height and ID
func BlockString(block *types.Block) string {
	digest, _ := json.Marshal(block.ID.Digest)
	return fmt.Sprintf("Height: %d, ID: %s", block.Header.Height, string(digest))
}

// TransactionString returns a string containing the given transaction's ID
func TransactionString(transaction *types.Transaction) string {
	digest, _ := json.Marshal(transaction.ID.Digest)
	return fmt.Sprintf("ID: %s", string(digest))
}
