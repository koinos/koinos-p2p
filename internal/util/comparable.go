package util

import (
	types "github.com/koinos/koinos-types-golang"
)

//
// We want to use Multihash and Topology as keys in maps.
// The []byte type is not comparable, however string is.
// As of this writing, VariableBlob is typedef'd to []byte,
// so anything containing a VariableBlob cannot be used as
// a map key.
//
// To get around this, we define MultihashCmp and
// BlockTopologyCmp using string in place of VariableBlob [1].
// These types can be used as map keys.
//
// This source file is a workaround for this issue, and it should
// be deleted if the koinos-types issue is resolved [2].
//
// [1] On the Golang blog, Rob Pike specifically stated that
// "a string holds arbitrary bytes. It is not required to
// hold Unicode text, UTF-8 text, or any other predefined
// format. As far as the content of a string is concerned,
// it is exactly equivalent to a slice of bytes."
// So we shouldn't have any encoding issues.
//
// [2] https://github.com/koinos/koinos-types/issues/142
//

// MultihashCmp is a comparable version of Koinos Types Multihash
type MultihashCmp struct {
	ID     types.UInt64
	Digest string
}

// BlockTopologyCmp is a comparable version of Koinos Types Block Topology
type BlockTopologyCmp struct {
	ID       MultihashCmp
	Height   types.BlockHeightType
	Previous MultihashCmp
}

// MultihashToCmp returns a MultihashCmp object for the given Multihash
func MultihashToCmp(h types.Multihash) MultihashCmp {
	return MultihashCmp{
		ID:     h.ID,
		Digest: string(h.Digest),
	}
}

// MultihashFromCmp returns a Multihash object for the given MultihashCmp
func MultihashFromCmp(h MultihashCmp) types.Multihash {
	return types.Multihash{
		ID:     h.ID,
		Digest: []byte(h.Digest),
	}
}

// BlockTopologyToCmp returns a BlockTopologyCmp object for the given BlockTopology
func BlockTopologyToCmp(topo types.BlockTopology) BlockTopologyCmp {
	return BlockTopologyCmp{
		ID:       MultihashToCmp(topo.ID),
		Height:   topo.Height,
		Previous: MultihashToCmp(topo.Previous),
	}
}

// BlockTopologyFromCmp returns a BlockTopology object for the given BlockTopologyCmp
func BlockTopologyFromCmp(topo BlockTopologyCmp) types.BlockTopology {
	return types.BlockTopology{
		ID:       MultihashFromCmp(topo.ID),
		Height:   topo.Height,
		Previous: MultihashFromCmp(topo.Previous),
	}
}
