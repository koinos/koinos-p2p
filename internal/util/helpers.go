package util

import (
	"encoding/json"
	"fmt"

	types "github.com/koinos/koinos-types-golang"
)

// BlockString returns a string containing the given block's height and ID
func BlockString(block *types.Block) string {
	id, err := json.Marshal(block.ID)
	if err != nil {
		id = []byte("ERR")
	} else {
		id = id[1 : len(id)-1]
	}
	prevID, err := json.Marshal(block.Header.Previous)
	if err != nil {
		prevID = []byte("ERR")
	} else {
		prevID = prevID[1 : len(prevID)-1]
	}
	return fmt.Sprintf("Height:%d ID:%s Prev:%s", block.Header.Height, string(id), string(prevID))
}

// TransactionString returns a string containing the given transaction's ID
func TransactionString(transaction *types.Transaction) string {
	id, _ := json.Marshal(transaction.ID)
	return fmt.Sprintf("ID: %s", string(id))
}

// BlockTopologyCmpString returns a string representation of the BlockTopologyCmp
func BlockTopologyCmpString(topo *BlockTopologyCmp) string {
	id, err := json.Marshal(MultihashFromCmp(topo.ID))
	if err != nil {
		id = []byte("ERR")
	} else {
		id = id[1 : len(id)-1]
	}
	prevID, err := json.Marshal(MultihashFromCmp(topo.Previous))
	if err != nil {
		prevID = []byte("ERR")
	} else {
		prevID = prevID[1 : len(prevID)-1]
	}
	return fmt.Sprintf("Height:%d ID:%s Prev:%s", topo.Height, string(id), string(prevID))
}
