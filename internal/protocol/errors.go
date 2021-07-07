package protocol

import (
	"errors"
)

var (
	DeserializationError      = errors.New("error during deserialization")
	BlockIrreversibilityError = errors.New("block is earlier than irreversibility block")
	BlockApplicationError     = errors.New("block application failed")
)
