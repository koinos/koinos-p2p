package p2p

import (
	"context"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	types "github.com/koinos/koinos-types-golang"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

const syncID = "/koinos/sync/1.0.0"

// SyncProtocol handles broadcasting inventory to peers
type SyncProtocol struct {
	Node *KoinosP2PNode
}

// NewSyncProtocol constructs a new broadcast protocol object
func NewSyncProtocol(host *KoinosP2PNode) *SyncProtocol {
	p := &SyncProtocol{Node: host}
	host.Host.SetStreamHandler(syncID, p.handleStream)
	return p
}

func (c SyncProtocol) handleStream(s network.Stream) {
	encoder := cbor.NewEncoder(s)

	// Serialize and send chain ID
	vb := types.NewVariableBlob()
	vb = c.Node.RPC.GetChainID().Serialize(vb)
	err := encoder.Encode(vb)
	if err != nil {
		s.Reset()
		return
	}

	// Serialize and send the head block
	vb = types.NewVariableBlob()
	vb = c.Node.RPC.GetHeadBlock().Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		s.Reset()
		return
	}

	s.Close()
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (c SyncProtocol) InitiateProtocol(ctx context.Context, p peer.ID, errs chan error) {

	// Start a stream with the given peer
	s, err := c.Node.Host.NewStream(ctx, p, syncID)
	if err != nil {
		s.Reset()
		errs <- err
		return
	}

	if ctx.Err() != nil {
		errs <- err
		return
	}

	decoder := cbor.NewDecoder(s)

	// Receive peer's chain ID
	vb := types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		s.Reset()
		return
	}

	// Deserialize and check peer's chain ID
	_, peerChainID, err := types.DeserializeMultihash(vb)
	chainID := c.Node.RPC.GetChainID()
	if !chainID.Equals(peerChainID) {
		errs <- fmt.Errorf("Peer's chain ID does not match")
		s.Reset()
		return
	}

	// Receive peer's head block
	vb = types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		s.Reset()
		return
	}

	// Deserialize and check peer's chain ID
	_, peerHeadBlock, err := types.DeserializeBlockTopology(vb)
	headBlock := c.Node.RPC.GetHeadBlock()
	if peerHeadBlock.Height == headBlock.Height && peerHeadBlock.ID.Equals(&headBlock.ID) {
		errs <- fmt.Errorf("Peer is in sync")
		s.Reset()
		return
	}

	s.Close()
}
