package node

import (
	"context"

	types "github.com/koinos/koinos-types-golang"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type gossipManager struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

func NewGossipManager(ps *pubsub.PubSub, topicName string) (*gossipManager, error) {
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, err
	}
	//ps.RegisterTopicValidator()
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}

	gm := gossipManager{topic: topic, sub: sub}
	return &gm, nil
}

func (gm *gossipManager) StartGossip(ctx context.Context, ch chan<- types.VariableBlob) {
	go gm.readMessages(ctx, ch)
}

func (gm *gossipManager) PublishMessage(ctx context.Context, vb *types.VariableBlob) {
	gm.topic.Publish(ctx, *vb)
}

func (gm *gossipManager) readMessages(ctx context.Context, ch chan<- types.VariableBlob) {
	for {
		msg, err := gm.sub.Next(ctx)
		if err != nil {
			close(ch)
			return
		}

		ch <- types.VariableBlob(msg.Data)
	}
}

type KoinosGossip struct {
	node        *KoinosP2PNode
	Block       *gossipManager
	Transaction *gossipManager
	PubSub      *pubsub.PubSub
}

func NewKoinosGossip(ctx context.Context, node *KoinosP2PNode) (*KoinosGossip, error) {
	ps, err := pubsub.NewGossipSub(ctx, node.Host)
	if err != nil {
		return nil, err
	}

	block, err := NewGossipManager(ps, "koinos.blocks")
	if err != nil {
		return nil, err
	}

	transaction, err := NewGossipManager(ps, "koinos.transactions")
	if err != nil {
		return nil, err
	}

	kg := KoinosGossip{node: node, Block: block, Transaction: transaction, PubSub: ps}

	return &kg, nil
}

func (kg *KoinosGossip) StartGossip(ctx context.Context) {
	go kg.readBlocks(ctx)
	go kg.readTransactions(ctx)
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

		if ok, err := kg.node.RPC.ApplyBlock(block); !ok || err != nil {
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

		if ok, err := kg.node.RPC.ApplyTransaction(transaction); !ok || err != nil {
			continue
		}
	}
}
