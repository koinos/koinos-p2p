package protocol

import (
	"context"
	"fmt"
	"log"

	"github.com/fxamacker/cbor/v2"
	types "github.com/koinos/koinos-types-golang"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const batchSize types.UInt64 = 20 // TODO: consider how to tune this and/or make it customizable

// BroadcastPeerStatus is an enum which represent peer's response
type forkStatus int

// The possible peer status results
const (
	SameFork forkStatus = iota
	DifferentFork
)

// BroadcastResponse is the message a peer returns
type forkCheckResponse struct {
	Status      forkStatus
	StartHeight types.BlockHeightType
}

type batchRequest struct {
	StartBlock types.BlockHeightType
	BatchSize  types.UInt64
}

type blockBatch struct {
	VectorBlockItems types.VariableBlob
}

// SyncProtocol handles broadcasting inventory to peers
type SyncProtocol struct {
	Data Data
}

// NewSyncProtocol constructs a new broadcast protocol object
func NewSyncProtocol(data *Data) *SyncProtocol {
	p := &SyncProtocol{Data: *data}
	return p
}

// GetProtocolRegistration returns the registration information
func (c SyncProtocol) GetProtocolRegistration() (pid protocol.ID, handler network.StreamHandler) {
	return SyncID, c.handleStream
}

func min(a, b types.UInt64) types.UInt64 {
	if a < b {
		return a
	}
	return b
}

func (c SyncProtocol) handleStream(s network.Stream) {

	log.Printf("Peer connected to us to sync: %v", s.ID())

	encoder := cbor.NewEncoder(s)
	decoder := cbor.NewDecoder(s)

	log.Printf("%v: checking peer's chain id", s.ID())

	// Serialize and send chain ID
	vb := types.NewVariableBlob()
	chainID, err := c.Data.RPC.GetChainID()
	if err != nil {
		log.Printf("%v: error retrieving chain id", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	vb = chainID.ChainID.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		log.Printf("%v: error sending chain id", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Receive and deserialize peer's chain id
	vb = types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		log.Printf("%v: error retrieving chain id", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	_, peerChainID, err := types.DeserializeMultihash(vb)
	if err != nil {
		log.Printf("%v: error deserializing chain id", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Check against peer's chain id
	if !chainID.ChainID.Equals(peerChainID) {
		log.Printf("%v: peer's chain id does not match", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Serialize and send the head block
	log.Printf("%v: sending head block to peer", s.ID())
	headBlock, err := c.Data.RPC.GetHeadBlock() // Cache head block so it doesn't change during communication
	if err != nil {
		log.Printf("%v: error getting head block", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	vb = types.NewVariableBlob()
	vb = headBlock.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		log.Printf("%v: error serializing head block", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Receive sender's head block
	vb = types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		log.Printf("%v: error retreiving peer's head block", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Deserialize and and get ancestor of block
	_, senderHeadBlock, err := types.DeserializeHeadInfo(vb)
	if err != nil {
		log.Printf("%v: error deserializing peer head block", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	response := forkCheckResponse{
		StartHeight: senderHeadBlock.Height,
		Status:      SameFork,
	}

	// If peer is not in genesis state, check if they are on an ancestor chain of our's
	if senderHeadBlock.Height > 0 {
		ancestor, err := c.Data.RPC.GetBlocksByHeight(&headBlock.ID, senderHeadBlock.Height, 1)

		if err != nil { // Should not happen, requested blocks from ID that we do not have
			log.Printf("%v: error getting oldest ancestor", s.ID())
			log.Printf("%v: %e", s.ID(), err)
			s.Reset()
			return
		}

		if !ancestor.BlockItems[0].BlockID.Equals(&senderHeadBlock.ID) { // Different fork
			response.StartHeight = 0
			response.Status = DifferentFork
		}
	}

	// Send fork check response
	err = encoder.Encode(response)
	if err != nil {
		log.Printf("%v: error sending fork response", s.ID())
		log.Printf("%v: %e", s.ID(), err)
		s.Reset()
		return
	}

	// Handle batch requests until they hang up
	for {
		req := batchRequest{}
		err = decoder.Decode(&req)
		if err != nil {
			log.Printf("%v: error retrieving batch request", s.ID())
			log.Printf("%v: %e", s.ID(), err)
			s.Reset()
			return
		}

		// If they requested more blocks than we have, then there is a problem with the peer
		if (types.UInt64(req.StartBlock) + req.BatchSize - 1) > types.UInt64(headBlock.Height) {
			log.Printf("%v: peer requested more blocks than we know of", s.ID())
			s.Reset()
			return
		}

		log.Printf("%v: responding to batch request for blocks %v-%v", s.ID(), req.StartBlock, types.UInt64(req.StartBlock)+req.BatchSize-1)

		// Fetch blocks
		blocks, err := c.Data.RPC.GetBlocksByHeight(&headBlock.ID,
			req.StartBlock, types.UInt32(min(batchSize, req.BatchSize)))
		if err != nil {
			log.Printf("%v: error getting blocks", s.ID())
			log.Printf("%v: %e", s.ID(), err)
			s.Reset()
			return
		}

		// Create and send a batch
		vb = types.NewVariableBlob()
		vb = blocks.BlockItems.Serialize(vb)
		batch := blockBatch{VectorBlockItems: *vb}
		err = encoder.Encode(batch)
		if err != nil {
			log.Printf("%v: error sending batch response", s.ID())
			log.Printf("%v: %e", s.ID(), err)
			s.Reset()
			return
		}
	}
}

// InitiateProtocol begins the communication with the peer
// TODO: Consider interface for protocols
func (c SyncProtocol) InitiateProtocol(ctx context.Context, p peer.ID, errs chan error) {

	log.Printf("connected to peer to sync: %v", p)

	// Start a stream with the given peer
	s, err := c.Data.Host.NewStream(ctx, p, SyncID)
	if err != nil {
		s.Reset()
		errs <- err
		return
	}

	if ctx.Err() != nil {
		errs <- err
		return
	}

	decoder := cbor.NewDecoder(s)
	encoder := cbor.NewEncoder(s)

	log.Printf("%v: checking peer's chain id", p)

	// Receive and deserialize peer's chain ID
	vb := types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		s.Reset()
		return
	}

	_, peerChainID, err := types.DeserializeMultihash(vb)
	if err != nil {
		log.Printf("%v: error deserializing chain id", p)
		errs <- err
		s.Reset()
		return
	}

	// Get our chain id
	chainID, err := c.Data.RPC.GetChainID()
	if err != nil {
		log.Printf("%v: error retrieving chain id", p)
		log.Printf("%v: %e", p, err)
		errs <- err
		s.Reset()
		return
	}

	// Check against peer's chain id
	if !chainID.ChainID.Equals(peerChainID) {
		log.Printf("%v: peer's chain id does not match", p)
		errs <- fmt.Errorf("%v: peer's chain id does not match", p)
		s.Reset()
		return
	}

	// Send our chain ID
	vb = chainID.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		log.Printf("%v: error sending chain id", p)
		log.Printf("%v: %e", p, err)
		s.Reset()
		return
	}

	// Deserialize and check peer's head block
	log.Printf("%v: checking peer's head block", p)

	// Receive peer's head block
	vb = types.NewVariableBlob()
	err = decoder.Decode(vb)
	if err != nil {
		log.Printf("%v: error receiving peer's head block", p)
		log.Printf("%v: %e", p, err)
		s.Reset()
		return
	}

	_, peerHeadBlock, err := types.DeserializeHeadInfo(vb)
	if err != nil {
		log.Printf("%v: error deserializing peer's head block", p)
		log.Printf("%v: %e", p, err)
		errs <- err
		s.Reset()
		return
	}

	headBlock, err := c.Data.RPC.GetHeadBlock()
	if err != nil {
		log.Printf("%v: error getting my head block", p)
		log.Printf("%v: %e", p, err)
		errs <- err
		s.Reset()
		return
	}

	if peerHeadBlock.Height == headBlock.Height && peerHeadBlock.ID.Equals(&headBlock.ID) {
		errs <- fmt.Errorf("%v: peer is in sync with us, disconnecting", p)
		s.Reset()
		return
	}

	// Serialize and send my head block to peer for fork check
	vb = types.NewVariableBlob()
	vb = headBlock.Serialize(vb)
	err = encoder.Encode(vb)
	if err != nil {
		log.Printf("%v: error sending my head block", p)
		log.Printf("%v: %e", p, err)
		errs <- err
		s.Reset()
		return
	}

	// Receive fork check response
	log.Printf("%v: checking peer's fork status", p)
	forkCheck := forkCheckResponse{}
	err = decoder.Decode(&forkCheck)
	if err != nil {
		log.Printf("%v: error receiving fork status", p)
		log.Printf("%v: %e", p, err)
		errs <- err
		s.Reset()
		return
	}

	// If fork is different, hang up for now
	if forkCheck.Status == DifferentFork {
		errs <- fmt.Errorf("%v: peer is on a different fork", p)
		s.Reset()
		return
	}

	log.Printf("%v: Syncing to block %v", p, peerHeadBlock.Height)

	// Request and apply batches until caught up
	currentHeight := headBlock.Height
	for currentHeight < peerHeadBlock.Height {
		// Request a batch either batchSize long, or the remaining size if it's less
		size := min(types.UInt64(peerHeadBlock.Height)-types.UInt64(currentHeight), batchSize)
		req := batchRequest{StartBlock: currentHeight + 1,
			BatchSize: size}

		log.Printf("%v: requesting blocks %v-%v", p, req.StartBlock, types.UInt64(req.StartBlock)+req.BatchSize-1)

		// Send batch request
		err = encoder.Encode(req)
		if err != nil {
			log.Printf("%v: error sending batch request", p)
			log.Printf("%v: %e", p, err)
			errs <- err
			s.Reset()
			return
		}

		// Receive batch
		batch := blockBatch{}
		err = decoder.Decode(&batch)
		if err != nil {
			log.Printf("%v: error receiving batch request", p)
			log.Printf("%v: %e", p, err)
			errs <- err
			s.Reset()
			return
		}

		log.Printf("%v: pushing blocks %v-%v", p, req.StartBlock, types.UInt64(req.StartBlock)+req.BatchSize-1)

		err = c.applyBlocks(&batch)
		if err != nil {
			log.Printf("%v: error applying blocks", p)
			log.Printf("%v: %e", p, err)
			errs <- err
			s.Reset()
			return
		}

		currentHeight += types.BlockHeightType(size)
	}

	s.Close()
}

func (c SyncProtocol) applyBlocks(batch *blockBatch) error {
	_, blocks, err := types.DeserializeVectorBlockItem(&batch.VectorBlockItems)
	if err != nil {
		return err
	}

	for i := 0; i < len(*blocks); i++ {
		bi := (*blocks)[i]

		bi.Block.Unbox()

		block, _ := bi.Block.GetNative()
		topology := types.NewBlockTopology()
		topology.Height = bi.BlockHeight
		topology.ID.ID = bi.BlockID.ID
		topology.ID.Digest = bi.BlockID.Digest
		block.ActiveData.Unbox()
		if !block.ActiveData.IsBoxed() {
			active, _ := block.ActiveData.GetNative()
			topology.Previous.ID = active.HeaderHashes.ID
			topology.Previous.Digest = active.HeaderHashes.Digests[0]
		}

		ok, err := c.Data.RPC.ApplyBlock(block, topology)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("block apply failed")
		}
	}

	return nil
}
