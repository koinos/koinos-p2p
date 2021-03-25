package protocol

import (
	"context"
	"log"

	"github.com/koinos/koinos-p2p/internal/rpc"
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

// StartGossip starts
func (gm *GossipManager) StartGossip(ctx context.Context, ch chan<- types.VariableBlob) error {
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

// StopGossip stops all gossiping on this topic
func (gm *GossipManager) StopGossip() {
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

// StartGossip enables gossip of blocks and transactions
func (kg *KoinosGossip) StartGossip(ctx context.Context) {
	go kg.readBlocks(ctx)
	go kg.readTransactions(ctx)
}

// StopGossip stops gossiping on both block and transaction topics
func (kg *KoinosGossip) StopGossip() {
	kg.Block.StopGossip()
	kg.Transaction.StopGossip()
}

func (kg *KoinosGossip) readBlocks(ctx context.Context) {
	ch := make(chan types.VariableBlob, 8) // TODO: Magic number
	kg.Block.StartGossip(ctx, ch)
	log.Println("Started block gossip listener")

	for {
		vb, ok := <-ch
		if !ok {
			close(ch)
			return
		}

		log.Println("Received block via gossip")
		_, blockBroadcast, err := types.DeserializeBlockAccepted(&vb)
		if err != nil { // TODO: Bad message, assign naughty points
			log.Println("Gossiped block is corrupt")
			continue
		}

		// TODO: Fix nil argument
		// TODO: Perhaps this block should sent to the block cache instead?
		if ok, err := kg.rpc.ApplyBlock(ctx, &blockBroadcast.Block, &blockBroadcast.Topology); !ok || err != nil {
			log.Println("Gossiped block not applied")
			continue
		}

		log.Println("Gossiped block applied")
	}
}

func (kg *KoinosGossip) readTransactions(ctx context.Context) {
	ch := make(chan types.VariableBlob, 32) // TODO: Magic number
	kg.Transaction.StartGossip(ctx, ch)
	log.Println("Started transaction gossip listener")

	for {
		vb, ok := <-ch
		if !ok {
			close(ch)
			return
		}

		log.Println("Received transaction via gossip")
		_, transaction, err := types.DeserializeTransaction(&vb)
		if err != nil { // TODO: Bad message, assign naughty points
			log.Println("Gossiped transaction is corrupt")
			continue
		}

		// TODO: Perhaps these should be cached?
		if ok, err := kg.rpc.ApplyTransaction(ctx, transaction); !ok || err != nil {
			log.Println("Gossiped transaction not applied")
			continue
		}
		log.Println("Gossiped transaction applied")
	}
}
