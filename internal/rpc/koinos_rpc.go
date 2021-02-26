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
		err = errors.New("Unexptected return type")
	}

	return response, err
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
