package p2p

import (
    "testing"
    types "github.com/koinos/koinos-types-golang"
)

func TestGetInfo(t *testing.T) {
    s := GetInfo()
    if s != "test" {
        t.Error("String does not match")
    }
}

func TestGetNumber(t *testing.T) {
    s := GetNumber()
    if s != types.UInt64(10) {
        t.Error("Number does not match")
    }
}