package rpc

import (
	"encoding/json"
	"errors"

	koinosmq "github.com/koinos/koinos-mq-golang"
	koinos_types "github.com/koinos/koinos-types-golang"
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
func (k *KoinosRPC) GetHeadBlock() (*koinos_types.HeadInfo, error) {
	args := koinos_types.ChainRPCRequest{
		Value: koinos_types.NewGetHeadInfoRequest(),
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

	responseVariant := koinos_types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *koinos_types.HeadInfo

	switch t := responseVariant.Value.(type) {
	case *koinos_types.GetHeadInfoResponse:
		response = (*koinos_types.HeadInfo)(t)
	case *koinos_types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexptected return type")
	}

	return response, err
}

// ApplyBlock rpc call
func (k *KoinosRPC) ApplyBlock(block *koinos_types.Block, topology ...*koinos_types.BlockTopology) (bool, error) {
	blockSub := koinos_types.NewSubmitBlockRequest()
	blockSub.Block = *block

	if len(topology) == 0 {
		// TODO: Fill in Block Topology
	} else {
		blockSub.Topology = *topology[0]
	}

	blockSub.VerifyPassiveData = true
	blockSub.VerifyBlockSignature = true
	blockSub.VerifyTransactionSignatures = true

	args := koinos_types.ChainRPCRequest{
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

	responseVariant := koinos_types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, nil
	}

	response := false

	switch t := responseVariant.Value.(type) {
	case *koinos_types.SubmitBlockResponse:
		response = true
	case *koinos_types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		response = false
	}

	return response, err
}

// ApplyTransaction rpc call
func (k *KoinosRPC) ApplyTransaction(block *koinos_types.Transaction) (bool, error) {
	return true, nil
}

// GetBlocksByHeight rpc call
func (k *KoinosRPC) GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.BlockHeightType, numBlocks koinos_types.UInt32) (*koinos_types.GetBlocksByHeightResponse, error) {
	args := koinos_types.BlockStoreRequest{
		Value: &koinos_types.GetBlocksByHeightRequest{
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

	responseVariant := koinos_types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, nil
	}

	var response *koinos_types.GetBlocksByHeightResponse

	switch t := responseVariant.Value.(type) {
	case *koinos_types.GetBlocksByHeightResponse:
		response = (*koinos_types.GetBlocksByHeightResponse)(t)
	case *koinos_types.BlockStoreErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// GetAncestorTopologyAtHeight rpc call
func (k *KoinosRPC) GetAncestorTopologyAtHeights(blockID *koinos_types.Multihash, heights []koinos_types.BlockHeightType) ([]koinos_types.BlockTopology, error) {

	// TODO:  Implement this properly in the block store.
	// This implementation is an inefficient, abstraction-breaking hack that unboxes stuff in the p2p code (where it definitely shouldn't be unboxed).

	result := make([]koinos_types.BlockTopology, len(heights))

	for i, h := range heights {
		resp := k.GetBlocksByHeight(blockID, h, 1)
		if len(resp.BlockItems) != 1 {
			return nil, errors.New("Unexpected multiple blocks returned")
		}
		resp.BlockItems[0].Block.ActiveData.Unbox()
		activeData, err := resp.BlockItems[0].Block.ActiveData.GetNative()
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
func (k *KoinosRPC) GetChainID() (*koinos_types.GetChainIDResponse, error) {
	args := koinos_types.ChainRPCRequest{
		Value: koinos_types.NewGetChainIDRequest(),
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

	responseVariant := koinos_types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, nil
	}

	var response *koinos_types.GetChainIDResponse

	switch t := responseVariant.Value.(type) {
	case *koinos_types.GetChainIDResponse:
		response = (*koinos_types.GetChainIDResponse)(t)
	case *koinos_types.ChainErrorResponse:
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
func (k *KoinosRPC) GetForkHeads() (*koinos_types.GetForkHeadsResponse, error) {
	args := koinos_types.GetForkHeadsRequest{}

	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.SendRPC("application/json", "chain", data)

	if err != nil {
		return nil, err
	}

	responseVariant := koinos_types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *koinos_types.GetForkHeadsResponse

	switch t := responseVariant.Value.(type) {
	case *koinos_types.GetForkHeadsResponse:
		response = (*koinos_types.GetForkHeadsResponse)(t)
	case *koinos_types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}
