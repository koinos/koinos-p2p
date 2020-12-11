package p2p

import (
    types "github.com/koinos/koinos-types-golang"
)

// GetInfo returns a test string
func GetInfo() string {
    return "test"
}

// GetNumber returns a test number
func GetNumber() types.UInt64 {
    return types.UInt64(10)
}
