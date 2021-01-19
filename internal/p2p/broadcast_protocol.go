package p2p

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/fxamacker/cbor/v2" // imports as package "cbor"
)

const broadcastID = "/broadcast/1.0.0"

// BroadcastProtocol handles broadcasting inventory to peers
type BroadcastProtocol struct {
	Host *KoinosP2PNode
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
func NewBroadcastProtocol(host *KoinosP2PNode) *BroadcastProtocol {
	ps := &BroadcastProtocol{Host: host}
	host.Host.SetStreamHandler(broadcastID, ps.handleStream)
	return ps
}

func (c *BroadcastProtocol) handleStream(s network.Stream) {
	// Decode hello string
	var message string
	decoder := cbor.NewDecoder(s)
	err := decoder.Decode(&message)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Received message from peer: %s\n", message)

	// Encode response
	response := BroadcastResponse{Status: Ok}
	encoder := cbor.NewEncoder(s)
	err = encoder.Encode(response)
	if err != nil {
		panic(err)
	}
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (c *BroadcastProtocol) InitiateProtocol(ctx context.Context, host *KoinosP2PNode, p peer.ID) {
	// Start a stream with the given peer
	s, err := host.Host.NewStream(ctx, p, broadcastID)
	if err != nil {
		panic(err)
	}

	go func() {
		for ctx.Err() == nil {
			message := "Koinos 2021"
			fmt.Printf("Sending message to peer: %s\n", message)

			// Say hello to other node
			encoder := cbor.NewEncoder(s)
			err := encoder.Encode(message)
			if err != nil {
				panic(err)
			}

			// Receive response
			var response BroadcastResponse
			decoder := cbor.NewDecoder(s)
			err = decoder.Decode(&response)
			if err != nil {
				panic(err)
			}

			if response.Status == Ok {
				fmt.Println("Received Ok response from peer.")
			}

			<-ctx.Done()
			s.Reset()
		}
	}()
}
