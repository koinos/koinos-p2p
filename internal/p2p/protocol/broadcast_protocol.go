package protocol

import (
	"context"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/fxamacker/cbor/v2" // imports as package "cbor"
)

const broadcastID = "/koinos/broadcast/1.0.0"

// BroadcastProtocol handles broadcasting inventory to peers
type BroadcastProtocol struct {
	Data Data
}

// BroadcastPeerStatus is an enum which represent peer's response
type BroadcastPeerStatus int

// The possible peer status results
const (
	Ok BroadcastPeerStatus = iota
	Error
)

// BroadcastResponse is the message a peer returns
type BroadcastResponse struct {
	Status BroadcastPeerStatus
}

// NewBroadcastProtocol constructs a new broadcast protocol object
func NewBroadcastProtocol(data *Data) *BroadcastProtocol {
	ps := &BroadcastProtocol{Data: *data}

	return ps
}

func (c BroadcastProtocol) GetProtocolRegistration() (pid protocol.ID, handler network.StreamHandler) {
	return broadcastID, c.handleStream
}

func (c *BroadcastProtocol) handleStream(s network.Stream) {
	// Decode hello string
	var message string
	decoder := cbor.NewDecoder(s)
	decoder.Decode(&message)

	// Act on message here

	// Encode response
	response := BroadcastResponse{Status: Ok}
	encoder := cbor.NewEncoder(s)
	encoder.Encode(response)

	s.Close()
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (c *BroadcastProtocol) InitiateProtocol(ctx context.Context, p peer.ID) {
	// Start a stream with the given peer
	s, _ := c.Data.Host.NewStream(ctx, p, broadcastID)

	message := "Koinos 2021"

	// Say hello to other node
	encoder := cbor.NewEncoder(s)
	encoder.Encode(message)

	// Receive response
	var response BroadcastResponse
	decoder := cbor.NewDecoder(s)
	decoder.Decode(&response)

	// handle response here

	s.Close()
}
