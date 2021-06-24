package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-p2p/internal/rpc"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	transactionBuffer int = 32
	blockBuffer       int = 8

	// BlockTopicName is the block topic string
	BlockTopicName string = "koinos.blocks"

	// TransactionTopicName is the transaction topic string
	TransactionTopicName string = "koinos.transactions"

	//PeerTopicName is the peer topic string
	PeerTopicName string = "koinos.peers"

	peerAdvertiseTime time.Duration = time.Minute * 1
)

// GossipManager manages gossip on a given topic
type GossipManager struct {
	ps            *pubsub.PubSub
	topic         *pubsub.Topic
	sub           *pubsub.Subscription
	peerErrorChan chan<- PeerError
	topicName     string
	enabled       bool
}

// NewGossipManager creates and returns a new instance of gossipManager
func NewGossipManager(ps *pubsub.PubSub, errChan chan<- PeerError, topicName string) *GossipManager {
	gm := GossipManager{
		ps:            ps,
		peerErrorChan: errChan,
		topicName:     topicName,
		enabled:       false,
	}
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

	log.Debugf("Publishing message")
	gm.topic.Publish(ctx, *vb)

	return true
}

func (gm *GossipManager) readMessages(ctx context.Context, ch chan<- types.VariableBlob) {
	//
	// The purpose of this function is to move messages from each topic's gm.sub.Next()
	// and send them to a single channel.
	//
	// Note that libp2p has already used the callback passed to RegisterValidator()
	// to validate blocks / transactions.
	//
	defer close(ch)
	for {
		msg, err := gm.sub.Next(ctx)
		if err != nil {
			log.Warnf("Error getting message for topic %s: %s", gm.topicName, err)
			return
		}

		select {
		case ch <- types.VariableBlob(msg.Data):
		case <-ctx.Done():
			return
		}
	}
}

// GossipEnableHandler is an interface for handling enable/disable gossip requests
type GossipEnableHandler interface {
	EnableGossip(context.Context, bool)
}

// PeerConnectionHandler handles the function necessary for gossip to connect to peers
type PeerConnectionHandler interface {
	PeerStringToAddress(peerAddr string) (*peer.AddrInfo, error)
	ConnectToPeerAddress(*peer.AddrInfo) error
	GetConnections() []network.Conn
}

// KoinosGossip handles gossip of blocks and transactions
type KoinosGossip struct {
	rpc                   rpc.RPC
	Block                 *GossipManager
	Transaction           *GossipManager
	Peer                  *GossipManager
	PubSub                *pubsub.PubSub
	PeerErrorChan         chan<- PeerError
	Connector             PeerConnectionHandler
	myPeerID              peer.ID
	lastIrreversibleBlock types.BlockHeightType
}

// NewKoinosGossip constructs a new koinosGossip instance
func NewKoinosGossip(
	ctx context.Context,
	rpc rpc.RPC,
	ps *pubsub.PubSub,
	peerErrorChan chan<- PeerError,
	connector PeerConnectionHandler,
	id peer.ID) *KoinosGossip {

	block := NewGossipManager(ps, peerErrorChan, BlockTopicName)
	transaction := NewGossipManager(ps, peerErrorChan, TransactionTopicName)
	peers := NewGossipManager(ps, peerErrorChan, PeerTopicName)
	kg := KoinosGossip{
		rpc:           rpc,
		Block:         block,
		Transaction:   transaction,
		Peer:          peers,
		PubSub:        ps,
		PeerErrorChan: peerErrorChan,
		Connector:     connector,
		myPeerID:      id,
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
	kg.Block.Stop()
	kg.Transaction.Stop()
}

func (kg *KoinosGossip) HandleForkHeads(fh *types.ForkHeads) {
	kg.lastIrreversibleBlock = fh.LastIrreversibleBlock.Height
}

func (kg *KoinosGossip) startBlockGossip(ctx context.Context) {
	go func() {
		blockChan := make(chan types.VariableBlob, blockBuffer)
		defer close(blockChan)
		kg.Block.RegisterValidator(kg.validateBlock)
		kg.Block.Start(ctx, blockChan)
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
		log.Warnf("Gossiped block not applied from peer %v: %s", msg.ReceivedFrom, err)
		select {
		case kg.PeerErrorChan <- PeerError{msg.ReceivedFrom, err}:
		case <-ctx.Done():
		}
		return false
	}
	return true
}

func (kg *KoinosGossip) applyBlock(ctx context.Context, pid peer.ID, msg *pubsub.Message) error {
	vb := types.VariableBlob(msg.Data)

	log.Debug("Received block via gossip")
	_, blockBroadcast, err := types.DeserializeBlockAccepted(&vb)
	if err != nil {
		// TODO: (Issue #5) Bad message, assign naughty points
		return errors.New("block deserialization failed, " + err.Error())
	}

	// If the gossip message is from this node, consider it valid but do not apply it (since it has already been applied)
	if msg.GetFrom() == kg.myPeerID {
		return nil
	}

	if blockBroadcast.Block.Header.Height < kg.lastIrreversibleBlock {
		return errors.New("block is earlier than irreversibility window")
	}

	// TODO: Fix nil argument
	// TODO: Perhaps this block should sent to the block cache instead?
	if _, err := kg.rpc.ApplyBlock(ctx, &blockBroadcast.Block); err != nil {
		return errors.New("block application failed - " + util.BlockString(&blockBroadcast.Block) + ", " + err.Error())
	}

	log.Infof("Gossiped block applied - %s from peer %v", util.BlockString(&blockBroadcast.Block), msg.ReceivedFrom)
	return nil
}

func (kg *KoinosGossip) startTransactionGossip(ctx context.Context) {
	go func() {
		transactionChan := make(chan types.VariableBlob, transactionBuffer)
		defer close(transactionChan)
		kg.Transaction.RegisterValidator(kg.validateTransaction)
		kg.Transaction.Start(ctx, transactionChan)
		log.Debug("Started transaction gossip listener")

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
		select {
		case kg.PeerErrorChan <- PeerError{msg.ReceivedFrom, err}:
		case <-ctx.Done():
		}
		return false
	}
	return true
}

func (kg *KoinosGossip) applyTransaction(ctx context.Context, pid peer.ID, msg *pubsub.Message) error {
	vb := types.VariableBlob(msg.Data)

	log.Debug("Received transaction via gossip")
	_, transaction, err := types.DeserializeTransaction(&vb)
	if err != nil {
		// TODO: (Issue #5) Bad message, assign naughty points
		return errors.New("transaction deserialization failed, " + err.Error())
	}

	// If the gossip message is from this node, consider it valid but do not apply it (since it has already been applied)
	if msg.GetFrom() == kg.myPeerID {
		return nil
	}

	if _, err := kg.rpc.ApplyTransaction(ctx, transaction); err != nil {
		return errors.New("transaction application failed - " + util.TransactionString(transaction) + ", " + err.Error())
	}

	log.Infof("Gossiped transaction applied - %s from peer %v", util.TransactionString(transaction), msg.ReceivedFrom)
	return nil
}

// ----------------------------------------------------------------------------
// Peer Gossip
// ----------------------------------------------------------------------------

func (kg *KoinosGossip) validatePeer(ctx context.Context, pid peer.ID, msg *pubsub.Message) bool {
	sAddr := string(msg.Data)
	addr, err := kg.Connector.PeerStringToAddress(sAddr)

	// If data cannot be interpreted as an address, invalidate it
	if err != nil {
		return false
	}

	// If the peer is from this node, consider it valid but do not add it
	if msg.GetFrom() == kg.myPeerID {
		return true
	}

	// Do not try to connect to myself
	if addr.ID == kg.myPeerID {
		return true
	}

	// Attempt to connect
	err = kg.Connector.ConnectToPeerAddress(addr)
	if err != nil {
		log.Infof("Failed to connect to gossiped peer: %s, %s", sAddr, err)
		return false
	}

	log.Infof("Received peer address via gossip - %s", sAddr)

	return true
}

func (kg *KoinosGossip) addressPublisher(ctx context.Context) {
	for {
		select {
		case <-time.After(peerAdvertiseTime):
			break
		case <-ctx.Done():
			return
		}

		log.Debug("Publishing connected peers...")
		for _, conn := range kg.Connector.GetConnections() {
			s := fmt.Sprintf("%s/p2p/%s", conn.RemoteMultiaddr(), conn.RemotePeer())
			log.Debugf("Published peer: %s", s)
			vb := types.VariableBlob((s))
			kg.Peer.PublishMessage(ctx, &vb)
		}
	}
}

// StartPeerGossip begins exchanging peers over gossip
func (kg *KoinosGossip) StartPeerGossip(ctx context.Context) {
	go func() {
		ch := make(chan types.VariableBlob, transactionBuffer)
		kg.Peer.RegisterValidator(kg.validatePeer)
		kg.Peer.Start(ctx, ch)
		log.Info("Started peer gossip")

		// Start address publisher
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go kg.addressPublisher(cctx)

		for {
			_, ok := <-ch
			if !ok {
				close(ch)
				return
			}
		}
	}()
}
