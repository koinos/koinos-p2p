package protocol

import (
	"context"

	"github.com/koinos/koinos-p2p/internal/rpc"
	types "github.com/koinos/koinos-types-golang"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// GossipManager manages gossip on a given topic
type GossipManager struct {
	ps        *pubsub.PubSub
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	topicName string
	enabled   bool
}

// NewGossipManager creates and returns a new instance of gossipManager
func NewGossipManager(ps *pubsub.PubSub, topicName string) *GossipManager {
	gm := GossipManager{ps: ps, topicName: topicName, enabled: false}
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

		ch <- types.VariableBlob(msg.Data)
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
func NewKoinosGossip(ctx context.Context, rpc rpc.RPC, ps *pubsub.PubSub) *KoinosGossip {
	block := NewGossipManager(ps, "koinos.blocks")
	transaction := NewGossipManager(ps, "koinos.transactions")
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

	for {
		vb, ok := <-ch
		if !ok {
			close(ch)
			return
		}

		_, block, err := types.DeserializeBlock(&vb)
		if err != nil { // TODO: Bad message, assign naughty points
			continue
		}

		if ok, err := kg.rpc.ApplyBlock(block); !ok || err != nil {
			continue
		}
	}
}

func (kg *KoinosGossip) readTransactions(ctx context.Context) {
	ch := make(chan types.VariableBlob, 32) // TODO: Magic number
	kg.Transaction.StartGossip(ctx, ch)

	for {
		vb, ok := <-ch
		if !ok {
			close(ch)
			return
		}

		_, transaction, err := types.DeserializeTransaction(&vb)
		if err != nil { // TODO: Bad message, assign naughty points
			continue
		}

		if ok, err := kg.rpc.ApplyTransaction(transaction); !ok || err != nil {
			continue
		}
	}
}
