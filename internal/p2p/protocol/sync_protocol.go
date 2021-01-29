package protocol

import (
	"context"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	types "github.com/koinos/koinos-types-golang"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const syncID = "/koinos/sync/1.0.0"

// BroadcastPeerStatus is an enum which represent peer's response
type forkStatus int

// The possible peer status results
const (
	SameFork forkStatus = iota
	DifferentFork
)

// BroadcastResponse is the message a peer returns
type forkCheckResponse struct {
	Status      forkStatus
	StartHeight types.BlockHeightType
}

// SyncProtocol handles broadcasting inventory to peers
type SyncProtocol struct {
	Data Data
}

// NewSyncProtocol constructs a new broadcast protocol object
func NewSyncProtocol(data *Data) *SyncProtocol {
	p := &SyncProtocol{Data: *data}
	return p
}

func (c SyncProtocol) GetProtocolRegistration() (pid protocol.ID, handler network.StreamHandler) {
	return syncID, c.handleStream
}

func (c SyncProtocol) handleStream(s network.Stream) {
	encoder := cbor.NewEncoder(s)

	// Serialize and send chain ID
	vb := types.NewVariableBlob()
	cid, err := c.Data.RPC.GetChainID()
	if err != nil {
		s.Reset()
		return
	}
	vb = cid.ChainID.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		s.Reset()
		return
	}

	// Serialize and send the head block
	headBlock, err := c.Data.RPC.GetHeadBlock() // Cache head block so it doesn't change during communication
	if err != nil {
		s.Reset()
		return
	}
	vb = types.NewVariableBlob()
	vb = headBlock.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		s.Reset()
		return
	}

	decoder := cbor.NewDecoder(s)

	// Receive sender's head block
	vb = types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		s.Reset()
		return
	}

	// Deserialize and and get ancestor of block
	_, senderHeadBlock, err := types.DeserializeHeadInfo(vb)
	ancestor, err := c.Data.RPC.GetBlocksByHeight(&headBlock.ID, senderHeadBlock.Height, 1)
	response := forkCheckResponse{}
	if !ancestor.BlockItems[0].BlockID.Equals(&senderHeadBlock.ID) { // Different fork
		response.StartHeight = 0
		response.Status = DifferentFork
	} else { // Same fork
		response.StartHeight = senderHeadBlock.Height
		response.Status = SameFork
	}

	// Send fork check response
	err = encoder.Encode(response)
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
	s, err := c.Data.Host.NewStream(ctx, p, syncID)
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
	chainID, err := c.Data.RPC.GetChainID()
	if err != nil {
		errs <- err
		s.Reset()
		return
	}
	if !chainID.ChainID.Equals(peerChainID) {
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

	// Deserialize and check peer's head block
	_, peerHeadBlock, err := types.DeserializeHeadInfo(vb)
	headBlock, err := c.Data.RPC.GetHeadBlock()
	if err != nil {
		errs <- err
		s.Reset()
		return
	}
	if peerHeadBlock.Height == headBlock.Height && peerHeadBlock.ID.Equals(&headBlock.ID) {
		errs <- fmt.Errorf("Peer is in sync")
		s.Reset()
		return
	}

	encoder := cbor.NewEncoder(s)

	// Serialize and send my head block to peer for fork check
	vb = types.NewVariableBlob()
	headBlock, err = c.Data.RPC.GetHeadBlock()
	vb = headBlock.Serialize(vb)
	if err != nil {
		errs <- err
		s.Reset()
		return
	}
	err = encoder.Encode(vb)
	if err != nil {
		errs <- err
		s.Reset()
		return
	}

	// Receive fork check response
	forkCheck := forkCheckResponse{}
	err = decoder.Decode(&forkCheck)
	if err != nil {
		errs <- err
		s.Reset()
		return
	}

	// If fork is different, hang up for now
	if forkCheck.Status == DifferentFork {
		errs <- fmt.Errorf("Peer is on a different fork")
	}

	s.Close()
}
