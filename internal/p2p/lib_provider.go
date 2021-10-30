package p2p

// LastIrreversibleBlockProvider is an interface for providing the last irreversible block to PeerConnection
type LastIrreversibleBlockProvider interface {
	GetLastIrreversibleBlock() (uint64, []byte)
}
