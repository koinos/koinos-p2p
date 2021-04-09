package protocol

import (
	"context"
	"log"

	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// GossipManager manages gossip on a given topic
type GossipManager struct {
	ps        *pubsub.PubSub
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	topicName string
	enabled   bool
	myPeerID  peer.ID
}

// NewGossipManager creates and returns a new instance of gossipManager
func NewGossipManager(ps *pubsub.PubSub, topicName string, id peer.ID) *GossipManager {
	gm := GossipManager{ps: ps, topicName: topicName, enabled: false, myPeerID: id}
	return &gm
}

// RegisterValidator registers the validate function to be used for messages
func (gm *GossipManager) RegisterValidator(val interface{}) {
	gm.ps.RegisterTopicValidator(gm.topicName, val)
}

// Start starts gossiping on this topic
func (gm *GossipManager) Start(ctx context.Context, ch chan<- types.VariableBlob) error {
	if gm.enabled {
		return nil
	}

	topic, err := gm.ps.Join(gm.topicName)
	if err != nil {
		return err
	}
	gm.topic = topic

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}
	gm.sub = sub

	go gm.readMessages(ctx, ch)

	gm.enabled = true

	return nil
}

// Stop stops all gossiping on this topic
func (gm *GossipManager) Stop() {
	if !gm.enabled {
		return
	}

	gm.sub.Cancel()
	gm.sub = nil
	gm.topic = nil
	gm.enabled = false
}

// PublishMessage publishes the given object to this manager's topic
func (gm *GossipManager) PublishMessage(ctx context.Context, vb *types.VariableBlob) bool {
	if !gm.enabled {
		return false
	}

	log.Print("Publishing message")
	gm.topic.Publish(ctx, *vb)

	return true
}

func (gm *GossipManager) readMessages(ctx context.Context, ch chan<- types.VariableBlob) {
	for {
		msg, err := gm.sub.Next(ctx)
		if err != nil {
			close(ch)
			return
		}

		if msg.GetFrom() != gm.myPeerID {
			ch <- types.VariableBlob(msg.Data)
		}
	}
}

// KoinosGossip handles gossip of blocks and transactions
type KoinosGossip struct {
	rpc         rpc.RPC
	Block       *GossipManager
	Transaction *GossipManager
	PubSub      *pubsub.PubSub
}

// NewKoinosGossip constructs a new koinosGossip instance
func NewKoinosGossip(ctx context.Context, rpc rpc.RPC, ps *pubsub.PubSub, id peer.ID) *KoinosGossip {
	block := NewGossipManager(ps, "koinos.blocks", id)
	transaction := NewGossipManager(ps, "koinos.transactions", id)
	kg := KoinosGossip{rpc: rpc, Block: block, Transaction: transaction, PubSub: ps}

	return &kg
}

// Start enables gossip of blocks and transactions
func (kg *KoinosGossip) Start(ctx context.Context) {
	kg.startBlockGossip(ctx)
	kg.startTransactionGossip(ctx)
}

// Stop stops gossiping on both block and transaction topics
func (kg *KoinosGossip) Stop() {
	kg.Block.Stop()
	kg.Transaction.Stop()
}

func (kg *KoinosGossip) startBlockGossip(ctx context.Context) {
	go func() {
		ch := make(chan types.VariableBlob, 8) // TODO: Magic number
		kg.Block.RegisterValidator(kg.validateBlock)
		kg.Block.Start(ctx, ch)
		log.Println("Started block gossip listener")

		// A block that reaches here has already been applied
		// Any postprocessing that might be needed would happen here
		for {
			_, ok := <-ch
			if !ok {
				close(ch)
				return
			}
		}
	}()
}

func (kg *KoinosGossip) validateBlock(ctx context.Context, pid peer.ID, msg *pubsub.Message) bool {
	vb := types.VariableBlob(msg.Data)

	log.Println("Received block via gossip")
	_, blockBroadcast, err := types.DeserializeBlockAccepted(&vb)
	if err != nil { // TODO: Bad message, assign naughty points
		log.Println("Gossiped block is corrupt")
		return false
	}

	// TODO: Fix nil argument
	// TODO: Perhaps this block should sent to the block cache instead?
	if ok, err := kg.rpc.ApplyBlock(ctx, &blockBroadcast.Block); !ok || err != nil {
		log.Printf("Gossiped block not applied - %s from peer %v\n", util.BlockString(&blockBroadcast.Block), msg.ReceivedFrom)
		return false
	}

	log.Printf("Gossiped block applied - %s from peer %v\n", util.BlockString(&blockBroadcast.Block), msg.ReceivedFrom)
	return true
}

func (kg *KoinosGossip) startTransactionGossip(ctx context.Context) {
	go func() {
		ch := make(chan types.VariableBlob, 32) // TODO: Magic number
		kg.Transaction.RegisterValidator(kg.validateTransaction)
		kg.Transaction.Start(ctx, ch)
		log.Println("Started transaction gossip listener")

		// A transaction that reaches here has already been applied
		// Any postprocessing that might be needed would happen here
		for {
			_, ok := <-ch
			if !ok {
				close(ch)
				return
			}
		}
	}()
}

func (kg *KoinosGossip) validateTransaction(ctx context.Context, pid peer.ID, msg *pubsub.Message) bool {
	vb := types.VariableBlob(msg.Data)

	log.Println("Received transaction via gossip")
	_, transaction, err := types.DeserializeTransaction(&vb)
	if err != nil { // TODO: Bad message, assign naughty points
		log.Println("Gossiped transaction is corrupt")
		return false
	}

	// TODO: Perhaps these should be cached?
	if ok, err := kg.rpc.ApplyTransaction(ctx, transaction); !ok || err != nil {
		log.Printf("Gossiped transaction not applied - %s\n", util.TransactionString(transaction))
		return false
	}

	log.Printf("Gossiped transaction applied - %s\n", util.TransactionString(transaction))
	return true
}
