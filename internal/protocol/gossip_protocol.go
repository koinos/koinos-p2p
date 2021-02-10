package protocol

import (
	"context"
	"log"

	"github.com/fxamacker/cbor/v2"
	types "github.com/koinos/koinos-types-golang"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	// imports as package "cbor"
)

const broadcastID = "/koinos/broadcast/1.0.0"

type idType int

const (
	transactionType idType = iota
	blockType
)

type idBroadcast struct {
	Type idType
	ID   types.Multihash
}

// GossipProtocol handles broadcasting inventory to peers
type GossipProtocol struct {
	Data Data
}

// NewGossipProtocol constructs a new broadcast protocol object
func NewGossipProtocol(data *Data) *GossipProtocol {
	ps := &GossipProtocol{Data: *data}

	return ps
}

// GetProtocolRegistration returns the registration information
func (g *GossipProtocol) GetProtocolRegistration() (pid protocol.ID, handler network.StreamHandler) {
	return broadcastID, g.handleStream
}

func (g *GossipProtocol) writeHandler(s network.Stream) {
	ch := g.Data.Inventory.EnableGossipChannel()
	defer g.Data.Inventory.DisableGossipChannel()
	encoder := cbor.NewEncoder(s)

	for {
		item, ok := <-ch // Wait on broadcast channel
		if !ok {         // If channel was closed, end protocol
			log.Print("Broadcast channel closed. Ending gossip mode.")
			s.Reset()
			return
		}

		// TODO: Consider batching several items before advertising?
		broadcast := idBroadcast{}
		broadcast.ID = item.ID
		switch item.Item.(type) {
		case *types.Block:
			broadcast.Type = blockType

		case *types.Transaction:
			broadcast.Type = transactionType
		}

		err := encoder.Encode(&item)
		if err != nil {
			log.Print(err)
			s.Reset()
			return
		}
	}
}

func (g *GossipProtocol) readHandler(s network.Stream) {
	decoder := cbor.NewDecoder(s)

	for {
		// Receive broadcast from peer
		bc := idBroadcast{}
		err := decoder.Decode(bc)
		log.Print("Received IDBroadcast from peer.")
		if err != nil {
			s.Reset()
			return
		}

		switch bc.Type {
		case transactionType:
			continue

		case blockType:
			continue
		}
	}
}

func (g *GossipProtocol) handleStream(s network.Stream) {
	go g.writeHandler(s)
	go g.readHandler(s)
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (g *GossipProtocol) InitiateProtocol(ctx context.Context, p peer.ID) {
	// Start a stream with the given peer
	s, _ := g.Data.Host.NewStream(ctx, p, broadcastID)

	g.handleStream(s)
}

// CloseProtocol closes a running gossip mode cleanly
func (g *GossipProtocol) CloseProtocol() {
	g.Data.Inventory.DisableGossipChannel()
}
