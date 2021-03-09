package rpc

import (
	"encoding/json"
	"errors"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/util"
	types "github.com/koinos/koinos-types-golang"
)

// KoinosRPC Implementation of RPC Interface
type KoinosRPC struct {
	mq *koinosmq.KoinosMQ
}

// NewKoinosRPC factory
func NewKoinosRPC() *KoinosRPC {
	rpc := KoinosRPC{}
	rpc.mq = koinosmq.GetKoinosMQ()
	return &rpc
}

// GetHeadBlock rpc call
func (k *KoinosRPC) GetHeadBlock() (*types.GetHeadInfoResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetHeadInfoRequest(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "chain", data)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *types.GetHeadInfoResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetHeadInfoResponse:
		response = t
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexptected return type")
	}

	return response, err
}

// ApplyBlock rpc call
// TODO:  Block should be OpaqueBlock
func (k *KoinosRPC) ApplyBlock(block *types.Block, topology *types.BlockTopology) (bool, error) {
	blockSub := types.NewSubmitBlockRequest()
	blockSub.Block = *block
	blockSub.Topology = *topology

	blockSub.VerifyPassiveData = true
	blockSub.VerifyBlockSignature = true
	blockSub.VerifyTransactionSignatures = true

	args := types.ChainRPCRequest{
		Value: blockSub,
	}
	data, err := json.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "chain", data)

	if err != nil {
		return false, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, nil
	}

	response := false

	switch t := responseVariant.Value.(type) {
	case *types.SubmitBlockResponse:
		response = true
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		response = false
	}

	return response, err
}

// ApplyTransaction rpc call
func (k *KoinosRPC) ApplyTransaction(block *types.Transaction) (bool, error) {
	return true, nil
}

// GetBlocksByID rpc call
func (k *KoinosRPC) GetBlocksByID(blockID *types.VectorMultihash) (*types.GetBlocksByIDResponse, error) {
	args := types.BlockStoreRequest{
		Value: &types.GetBlocksByIDRequest{
			BlockID:           *blockID,
			ReturnBlockBlob:   true,
			ReturnReceiptBlob: false,
		},
	}
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "koinos_block", data)
	if err != nil {
		return nil, err
	}

	responseVariant := types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, nil
	}

	var response *types.GetBlocksByIDResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetBlocksByIDResponse:
		response = (*types.GetBlocksByIDResponse)(t)
	case *types.BlockStoreErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// GetBlocksByHeight rpc call
func (k *KoinosRPC) GetBlocksByHeight(blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error) {
	args := types.BlockStoreRequest{
		Value: &types.GetBlocksByHeightRequest{
			HeadBlockID:         *blockID,
			AncestorStartHeight: height,
			NumBlocks:           numBlocks,
			ReturnBlock:         true,
			ReturnReceipt:       false,
		},
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "koinos_block", data)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, nil
	}

	var response *types.GetBlocksByHeightResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetBlocksByHeightResponse:
		response = (*types.GetBlocksByHeightResponse)(t)
	case *types.BlockStoreErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// GetAncestorTopologyAtHeights rpc call
func (k *KoinosRPC) GetAncestorTopologyAtHeights(blockID *types.Multihash, heights []types.BlockHeightType) ([]types.BlockTopology, error) {

	// TODO:  Implement this properly in the block store.
	// This implementation is an inefficient, abstraction-breaking hack that unboxes stuff in the p2p code (where it definitely shouldn't be unboxed).

	result := make([]types.BlockTopology, len(heights))

	for i, h := range heights {
		resp, err := k.GetBlocksByHeight(blockID, h, 1)
		if err != nil {
			return nil, err
		}
		if len(resp.BlockItems) != 1 {
			return nil, errors.New("Unexpected multiple blocks returned")
		}
		resp.BlockItems[0].Block.Unbox()
		block, err := resp.BlockItems[0].Block.GetNative()
		if err != nil {
			return nil, err
		}
		block.ActiveData.Unbox()
		activeData, err := block.ActiveData.GetNative()
		if err != nil {
			return nil, err
		}
		result[i].ID = resp.BlockItems[0].BlockID
		result[i].Height = resp.BlockItems[0].BlockHeight
		result[i].Previous = activeData.PreviousBlock
	}

	return result, nil
}

// GetChainID rpc call
func (k *KoinosRPC) GetChainID() (*types.GetChainIDResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetChainIDRequest(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "chain", data)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, nil
	}

	var response *types.GetChainIDResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetChainIDResponse:
		response = (*types.GetChainIDResponse)(t)
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexptected return type")
	}

	return response, err
}

// SetBroadcastHandler allows a function to be called for every broadcast block
func (k *KoinosRPC) SetBroadcastHandler(topic string, handler func(topic string, data []byte)) {
	mq := koinosmq.GetKoinosMQ()
	mq.SetBroadcastHandler(topic, handler)
}

// GetForkHeads rpc call
func (k *KoinosRPC) GetForkHeads() (*types.GetForkHeadsResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetForkHeadsRequest(),
	}

	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "chain", data)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *types.GetForkHeadsResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetForkHeadsResponse:
		response = (*types.GetForkHeadsResponse)(t)
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// GetTopologyAtHeight finds the blocks at the given height range.
//
// Three steps:
// - (1) Call GetForkHeads() to get the fork heads and LIB from koinosd
// - (2) For each fork, call GetBlocksByHeight() with the given height bounds to get the blocks in that height range on that fork.
// - (3) Finally, do some purely computational cleanup:  Extract the BlockTopology and de-duplicate multiple instances of the same block.
//
func (k *KoinosRPC) GetTopologyAtHeight(height types.BlockHeightType, numBlocks types.UInt32) (*types.GetForkHeadsResponse, []types.BlockTopology, error) {
	forkHeads, err := k.GetForkHeads()
	if err != nil {
		return nil, nil, err
	}

	topologySet := make(map[util.BlockTopologyCmp]util.Void)
	topologySlice := make([]types.BlockTopology, 0, len(forkHeads.ForkHeads))

	for _, head := range forkHeads.ForkHeads {
		//var t types.BlockTopology
		//t.
		blocks, err := k.GetBlocksByHeight(&head.ID, height, numBlocks)
		if err != nil {
			return nil, nil, err
		}

		// Go through each block and extract its topology
		for _, blockItem := range blocks.BlockItems {
			topology := types.BlockTopology{
				ID:     blockItem.BlockID,
				Height: blockItem.BlockHeight,
			}

			if blockItem.BlockHeight != 0 {
				opaqueBlock := blockItem.Block
				opaqueBlock.Unbox()
				block, err := opaqueBlock.GetNative()
				if err != nil {
					return nil, nil, err
				}

				opaqueActive := block.ActiveData
				opaqueActive.Unbox()
				active, err := opaqueActive.GetNative()
				if err != nil {
					return nil, nil, err
				}

				topology.Previous = active.PreviousBlock
			}

			// Add the topology to the set / slice if it's not already there
			cmp := util.BlockTopologyToCmp(topology)
			if _, ok := topologySet[cmp]; !ok {
				topologySet[cmp] = util.Void{}
				topologySlice = append(topologySlice, topology)
			}
		}
	}

	return forkHeads, topologySlice, nil
}
