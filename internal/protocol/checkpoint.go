package protocol

import (
	"errors"
	"regexp"
	"strconv"

	types "github.com/koinos/koinos-types-golang"
)

// Checkpoint represents a particular fork we want to limit ourselves to
type Checkpoint struct {
	Height types.BlockHeightType
	ID     types.Multihash
}

// ParseCheckpoint parses a Checkpoint struct from a string of the form height:blockid
func ParseCheckpoint(checkpointStr string) (Checkpoint, error) {
	result := Checkpoint{}
	reCheckpoint := regexp.MustCompile(`^\s*([0-9]+):(\S+)\s*$`)

	groups := reCheckpoint.FindStringSubmatch(checkpointStr)
	if groups == nil {
		return result, errors.New("checkpoint couldn't be parsed")
	}
	height, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return result, err
	}
	if height < 0 {
		return result, errors.New("checkpoint attempted to specify negative height")
	}

	// Code to turn an ID into a multihash is copy-pasted from UnmarshalJSON() for Multihash
	db, err := types.DecodeBytes(groups[2])
	if err != nil {
		return result, err
	}
	vdb := types.VariableBlob(db)
	size, m1, err := types.DeserializeMultihash(&vdb)
	if err != nil {
		return result, err
	}
	if size != uint64(len(db)) {
		return result, errors.New("checkpoint multihash had extra bytes")
	}
	result.Height = types.BlockHeightType(height)
	result.ID = *m1
	return result, nil
}
