package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/canonical"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	util "github.com/koinos/koinos-util-golang"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"google.golang.org/protobuf/proto"
)

const (
	transactionBuffer int = 32
	blockBuffer       int = 8

	// BlockTopicName is the block topic string
	BlockTopicName string = "koinos.blocks"

	// TransactionTopicName is the transaction topic string
	TransactionTopicName string = "koinos.transactions"
)

// GossipManager manages gossip on a given topic
type GossipManager struct {
	ps            *pubsub.PubSub
	topic         *pubsub.Topic
	sub           *pubsub.Subscription
	cancel        context.CancelFunc
	peerErrorChan chan<- PeerError
	topicName     string
	enableMutex   sync.Mutex
	Enabled       bool
}

// NewGossipManager creates and returns a new instance of gossipManager
func NewGossipManager(ps *pubsub.PubSub, errChan chan<- PeerError, topicName string) *GossipManager {
	gm := GossipManager{
		ps:            ps,
		peerErrorChan: errChan,
		topicName:     topicName,
		Enabled:       false,
	}

	topic, err := gm.ps.Join(gm.topicName)
	if err != nil {
		log.Errorf("could not connect to gossip topic: %s", gm.topicName)
	} else {
		gm.topic = topic
	}

	return &gm
}

// RegisterValidator registers the validate function to be used for messages
func (gm *GossipManager) RegisterValidator(val interface{}) error {
	return gm.ps.RegisterTopicValidator(gm.topicName, val)
}

// Start starts gossiping on this topic
func (gm *GossipManager) Start(ctx context.Context, ch chan<- []byte) error {
	gm.enableMutex.Lock()
	defer gm.enableMutex.Unlock()
	if gm.Enabled {
		return nil
	}

	if gm.topic == nil {
		return fmt.Errorf("cannot start gossip on nil topic: %s", gm.topicName)
	}

	sub, err := gm.topic.Subscribe()
	if err != nil {
		return err
	}
	gm.sub = sub
	subCtx, cancel := context.WithCancel(ctx)
	gm.cancel = cancel

	go gm.readMessages(subCtx, ch)

	gm.Enabled = true

	return nil
}

// Stop stops all gossiping on this topic
func (gm *GossipManager) Stop() {
	gm.enableMutex.Lock()
	defer gm.enableMutex.Unlock()
	if !gm.Enabled {
		return
	}

	gm.cancel()
	gm.sub.Cancel()
	gm.sub = nil
	gm.Enabled = false
}

// PublishMessage publishes the given object to this manager's topic
func (gm *GossipManager) PublishMessage(ctx context.Context, bytes []byte) bool {
	if !gm.Enabled {
		return false
	}

	log.Debugf("Publishing message")
	_ = gm.topic.Publish(ctx, bytes)

	return true
}

func (gm *GossipManager) readMessages(ctx context.Context, ch chan<- []byte) {
	//
	// The purpose of this function is to move messages from each topic's gm.sub.Next()
	// and send them to a single channel.
	//
	// Note that libp2p has already used the callback passed to RegisterValidator()
	// to validate blocks / transactions.
	//
	for {
		msg, err := gm.sub.Next(ctx)
		if err != nil && !errors.Is(context.DeadlineExceeded, err) {
			log.Warnf("Error getting message for topic %s: %s", gm.topicName, err)
			return
		}

		select {
		case ch <- msg.Data:
		case <-ctx.Done():
			return
		}
	}
}

// GossipEnableHandler is an interface for handling enable/disable gossip requests
type GossipEnableHandler interface {
	EnableGossip(context.Context, bool)
}

// KoinosGossip handles gossip of blocks and transactions
type KoinosGossip struct {
	rpc              rpc.LocalRPC
	block            *GossipManager
	transaction      *GossipManager
	PubSub           *pubsub.PubSub
	PeerErrorChan    chan<- PeerError
	myPeerID         peer.ID
	libProvider      LastIrreversibleBlockProvider
	transactionCache *TransactionCache
}

// NewKoinosGossip constructs a new koinosGossip instance
func NewKoinosGossip(
	ctx context.Context,
	rpc rpc.LocalRPC,
	ps *pubsub.PubSub,
	peerErrorChan chan<- PeerError,
	id peer.ID,
	libProvider LastIrreversibleBlockProvider,
	cache *TransactionCache) *KoinosGossip {

	block := NewGossipManager(ps, peerErrorChan, BlockTopicName)
	transaction := NewGossipManager(ps, peerErrorChan, TransactionTopicName)
	kg := KoinosGossip{
		rpc:              rpc,
		block:            block,
		transaction:      transaction,
		PubSub:           ps,
		PeerErrorChan:    peerErrorChan,
		myPeerID:         id,
		libProvider:      libProvider,
		transactionCache: cache,
	}

	return &kg
}

// EnableGossip satisfies GossipEnableHandler interface
func (kg *KoinosGossip) EnableGossip(ctx context.Context, enable bool) {
	if enable {
		kg.StartGossip(ctx)
	} else {
		kg.StopGossip()
	}
}

// StartGossip enables gossip of blocks and transactions
func (kg *KoinosGossip) StartGossip(ctx context.Context) {
	log.Info("Starting gossip mode")
	kg.startBlockGossip(ctx)
	kg.startTransactionGossip(ctx)
}

// StopGossip stops gossiping on both block and transaction topics
func (kg *KoinosGossip) StopGossip() {
	log.Info("Stopping gossip mode")
	kg.block.Stop()
	kg.transaction.Stop()
}

// PublishTransaction publishes a transaction to the transaction topic
func (kg *KoinosGossip) PublishTransaction(ctx context.Context, transaction *protocol.Transaction) error {
	binary, err := canonical.Marshal(transaction)
	if err != nil {
		return err
	}

	if kg.transaction.Enabled {
		// Add to the transaction cache
		kg.transactionCache.CheckTransactions(transaction)

		log.Infof("Publishing transaction - %s", util.TransactionString(transaction))
		kg.transaction.PublishMessage(context.Background(), binary)
	}

	return nil
}

// PublishBlock publishes a block to the block topic
func (kg *KoinosGossip) PublishBlock(ctx context.Context, block *protocol.Block) error {
	binary, err := canonical.Marshal(block)
	if err != nil {
		return err
	}

	if kg.block.Enabled {
		// Add to the transaction cache
		kg.transactionCache.CheckBlock(block)

		log.Infof("Publishing block - %s", util.BlockString(block))
		kg.block.PublishMessage(context.Background(), binary)
	}

	return nil
}

func (kg *KoinosGossip) startBlockGossip(ctx context.Context) {
	go func() {
		blockChan := make(chan []byte, blockBuffer)
		defer close(blockChan)
		_ = kg.block.RegisterValidator(kg.validateBlock)
		_ = kg.block.Start(ctx, blockChan)
		log.Info("Started block gossip listener")

		// A block that reaches here has already been applied
		// Any postprocessing that might be needed would happen here
		for {
			select {
			case _, ok := <-blockChan:
				if !ok {
					// ok == false means blockChan is closed, so we simply return
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (kg *KoinosGossip) validateBlock(ctx context.Context, pid peer.ID, msg *pubsub.Message) bool {
	err := kg.applyBlock(ctx, pid, msg)
	if err != nil {
		if errors.Is(err, p2perrors.ErrBlockIrreversibility) {
			log.Debug(err.Error())
		} else {
			log.Warnf("Gossiped block not applied from peer %v: %s", msg.ReceivedFrom, err)
			go func() {
				select {
				case kg.PeerErrorChan <- PeerError{id: msg.ReceivedFrom, err: err}:
				case <-ctx.Done():
				}
			}()
		}

		return false
	}
	return true
}

func (kg *KoinosGossip) applyBlock(ctx context.Context, pid peer.ID, msg *pubsub.Message) error {
	log.Debug("Received block via gossip")
	block := &protocol.Block{}
	err := proto.Unmarshal(msg.Data, block)
	if err != nil {
		return fmt.Errorf("%w, %v", p2perrors.ErrDeserialization, err.Error())
	}

	// If the gossip message is from this node, consider it valid but do not apply it (since it has already been applied)
	if msg.GetFrom() == kg.myPeerID {
		return nil
	}

	if block.Id == nil {
		return fmt.Errorf("%w, gossiped block missing id", p2perrors.ErrDeserialization)
	}

	if block.Header == nil {
		return fmt.Errorf("%w, gossiped block missing header", p2perrors.ErrDeserialization)
	}

	if block.Header.Previous == nil {
		return fmt.Errorf("%w, gossiped block missing header.previous", p2perrors.ErrDeserialization)
	}

	if block.Header.Height < kg.libProvider.GetLastIrreversibleBlock().Height {
		return p2perrors.ErrBlockIrreversibility
	}

	// Add transactions to the cache
	kg.transactionCache.CheckBlock(block)

	// TODO: Fix nil argument
	// TODO: Perhaps this block should sent to the block cache instead?
	if _, err := kg.rpc.ApplyBlock(ctx, block); err != nil {
		return fmt.Errorf("%w - %s, %v", p2perrors.ErrBlockApplication, util.BlockString(block), err.Error())
	}

	log.Infof("Gossiped block applied - %s from peer %v", util.BlockString(block), msg.ReceivedFrom)
	return nil
}

func (kg *KoinosGossip) startTransactionGossip(ctx context.Context) {
	go func() {
		transactionChan := make(chan []byte, transactionBuffer)
		defer close(transactionChan)
		_ = kg.transaction.RegisterValidator(kg.validateTransaction)
		_ = kg.transaction.Start(ctx, transactionChan)
		log.Info("Started transaction gossip listener")

		// A transaction that reaches here has already been applied
		// Any postprocessing that might be needed would happen here
		for {
			select {
			case _, ok := <-transactionChan:
				if !ok {
					// ok == false means blockChan is closed, so we simply return
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (kg *KoinosGossip) validateTransaction(ctx context.Context, pid peer.ID, msg *pubsub.Message) bool {
	err := kg.applyTransaction(ctx, pid, msg)
	if err != nil {
		log.Warnf("Gossiped transaction not applied from peer %v: %s", msg.ReceivedFrom, err)
		go func() {
			select {
			case kg.PeerErrorChan <- PeerError{msg.ReceivedFrom, err}:
			case <-ctx.Done():
			}
		}()
		return false
	}
	return true
}

func (kg *KoinosGossip) applyTransaction(ctx context.Context, pid peer.ID, msg *pubsub.Message) error {
	log.Debug("Received transaction via gossip")
	transaction := &protocol.Transaction{}
	err := proto.Unmarshal(msg.Data, transaction)
	if err != nil {
		return fmt.Errorf("%w, %v", p2perrors.ErrDeserialization, err.Error())
	}

	// If the gossip message is from this node, consider it valid but do not apply it (since it has already been applied)
	if msg.GetFrom() == kg.myPeerID {
		return nil
	}

	if transaction.Id == nil {
		return fmt.Errorf("%w, gossiped transaction missing id", p2perrors.ErrDeserialization)
	}

	if kg.transactionCache.CheckTransactions(transaction) > 0 {
		log.Debugf("Gossiped transaction already in cache - %s from peer %v", util.TransactionString(transaction), msg.ReceivedFrom)
		return nil
	}

	if _, err := kg.rpc.ApplyTransaction(ctx, transaction); err != nil {
		return fmt.Errorf("%w - %s, %v", p2perrors.ErrTransactionApplication, util.TransactionString(transaction), err.Error())
	}

	log.Infof("Gossiped transaction applied - %s from peer %v", util.TransactionString(transaction), msg.ReceivedFrom)
	return nil
}
