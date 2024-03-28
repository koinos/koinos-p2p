package p2p

import "github.com/koinos/koinos-proto-golang/v2/koinos"

// LastIrreversibleBlockProvider is an interface for providing the last irreversible block to PeerConnection
type LastIrreversibleBlockProvider interface {
	GetLastIrreversibleBlock() *koinos.BlockTopology
}
