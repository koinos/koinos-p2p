package protocol

import (
	"context"
	"fmt"
	"log"

	"github.com/fxamacker/cbor/v2"
	"github.com/koinos/koinos-p2p/internal/inventory"
	types "github.com/koinos/koinos-types-golang"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const broadcastID = "/koinos/broadcast/1.0.0"

type idType int

const (
	transactionType idType = iota
	blockType
)

type gossipMessage struct {
	Message interface{}
}

type idAdvertise struct {
	Type idType
	ID   types.Multihash
}

type gossipTransmit struct {
	Type  idType
	Value types.VariableBlob
}

type gossipRequest struct {
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
		advert := idAdvertise{}
		advert.ID = item.ID
		switch item.Item.(type) {
		case *types.Block:
			advert.Type = blockType

		case *types.Transaction:
			advert.Type = transactionType
		}

		message := gossipMessage{Message: advert}

		err := encoder.Encode(&message)
		if err != nil {
			g.hangUp(s, err)
			return
		}
	}
}

func (g *GossipProtocol) readHandler(s network.Stream) {
	decoder := cbor.NewDecoder(s)

	for {
		// Receive broadcast from peer
		gm := gossipMessage{}
		err := decoder.Decode(gm)
		log.Print("Received gossip from peer")
		if err != nil {
			s.Reset()
			return
		}

		go g.processMessage(s, &gm)
	}
}

func (g *GossipProtocol) processMessage(s network.Stream, message *gossipMessage) {
	switch o := message.Message.(type) {
	case idAdvertise:
		g.processIDBroadcast(s, &o)

	case gossipRequest:
		g.processGossipRequest(s, &o)

	case gossipTransmit:
		g.processGossipTransmit(s, &o)
	}
}

func (g *GossipProtocol) processIDBroadcast(s network.Stream, ib *idAdvertise) {
	switch ib.Type {
	case blockType:
		// Ignore if already in inventory
		if g.Data.Inventory.Blocks.Contains(&ib.ID) {
			return
		}

	case transactionType:
		// Ignore if already in inventory
		if g.Data.Inventory.Transactions.Contains(&ib.ID) {
			return
		}

	default:
		g.hangUp(s, fmt.Errorf("Unknown message type"))
	}

	req := gossipRequest{ID: ib.ID, Type: ib.Type}
	message := gossipMessage{Message: req}
	encoder := cbor.NewEncoder(s)
	err := encoder.Encode(&message)
	if err != nil {
		g.hangUp(s, err)
		return
	}
}

func (g *GossipProtocol) processGossipRequest(s network.Stream, gr *gossipRequest) {
	var item *inventory.Item
	var err error

	switch gr.Type {
	case blockType:
		item, err = g.Data.Inventory.Blocks.Fetch(&gr.ID)

	case transactionType:
		item, err = g.Data.Inventory.Transactions.Fetch(&gr.ID)
	}

	// We don't have this item, so it's either a bad request or it timed out after I advertised
	// Right now we will just ignore it and continue
	// TODO: assign naughty points and decide whether or not to hang up
	if err != nil {
		return
	}

	// Serialize the item
	ser, ok := item.Item.(types.Serializeable)
	if !ok {
		g.hangUp(s, fmt.Errorf("Item in database cannot be serialized"))
	}

	vb := types.NewVariableBlob()
	vb = ser.Serialize(vb)

	// Send the requested item to peer
	t := gossipTransmit{Type: gr.Type, Value: *vb}
	message := gossipMessage{Message: t}
	encoder := cbor.NewEncoder(s)
	err = encoder.Encode(&message)
	if err != nil {
		g.hangUp(s, err)
		return
	}
}

func (g *GossipProtocol) processGossipTransmit(s network.Stream, gt *gossipTransmit) {
	var ok bool
	var err error

	switch gt.Type {
	case blockType:
		_, block, err := types.DeserializeBlock(&gt.Value)
		if err != nil {
			g.hangUp(s, err)
		}

		ok, err = g.Data.RPC.ApplyBlock(block)

	case transactionType:
		_, transaction, err := types.DeserializeTransaction(&gt.Value)
		if err != nil {
			g.hangUp(s, err)
		}

		ok, err = g.Data.RPC.ApplyTransaction(transaction)

	default:
		ok = false
		err = fmt.Errorf("Unknown message type")
	}

	if err != nil {
		g.hangUp(s, err)
		return
	}

	if !ok {
		g.hangUp(s, fmt.Errorf("Failed to apply object"))
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

func (g *GossipProtocol) hangUp(s network.Stream, err error) {
	s.Reset()
	g.CloseProtocol()
}

// CloseProtocol closes a running gossip mode cleanly
func (g *GossipProtocol) CloseProtocol() {
	g.Data.Inventory.DisableGossipChannel()
}
