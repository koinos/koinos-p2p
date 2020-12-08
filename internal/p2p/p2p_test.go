package p2p

import (
    "testing"
)

func TestGetInfo(t *testing.T) {
    s := GetInfo()
    if s != "test" {
        t.Error("String does not match")
    }
}
