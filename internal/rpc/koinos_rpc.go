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
func (k KoinosRPC) GetHeadBlock() (*koinos_types.HeadInfo, error) {
	args := koinos_types.ChainRPCParams{
		Value: koinos_types.NewGetHeadInfoParams(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var resultBytes []byte
	resultBytes, err = k.mq.SendRPC("application/json", data)

	if err != nil {
		return nil, err
	}

	resultVariant := koinos_types.NewChainRPCResult()
	err = json.Unmarshal(resultBytes, resultVariant)
	if err != nil {
		return nil, nil
	}

	var result *koinos_types.HeadInfo

	switch t := resultVariant.Value.(type) {
	case *koinos_types.GetHeadInfoResult:
		result = (*koinos_types.HeadInfo)(t)
	case *koinos_types.RPCError:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexptected return type")
	}

	return result, err
}

// ApplyBlock rpc call
func (k KoinosRPC) ApplyBlock(block *koinos_types.Block) (bool, error) {
	blockSub := koinos_types.NewSubmitBlockParams()
	blockSub.Block = *block
	// TODO: Fill in Block Topology
	blockSub.VerifyPassiveData = true
	blockSub.VerifyBlockSignature = true
	blockSub.VerifyTransactionSignatures = true

	args := koinos_types.SubmissionItem{
		Value: blockSub,
	}
	data, err := json.Marshal(args)

	if err != nil {
		return false, err
	}

	var resultBytes []byte
	resultBytes, err = k.mq.SendRPC("application/json", data)

	if err != nil {
		return false, err
	}

	resultVariant := koinos_types.NewSubmissionResult()
	err = json.Unmarshal(resultBytes, resultVariant)
	if err != nil {
		return false, nil
	}

	result := false

	switch t := resultVariant.Value.(type) {
	case *koinos_types.BlockSubmissionResult:
		result = true
	case *koinos_types.RPCError:
		err = errors.New(string(t.ErrorText))
	default:
		result = false
	}

	return result, err
}

// ApplyTransaction rpc call
func (k KoinosRPC) ApplyTransaction(block *koinos_types.Transaction) (bool, error) {
	return true, nil
}

// GetBlocksByHeight rpc call
func (k KoinosRPC) GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.BlockHeightType, numBlocks koinos_types.UInt32) (*koinos_types.GetBlocksByHeightResp, error) {
	args := koinos_types.BlockStoreReq{
		Value: koinos_types.GetBlocksByHeightReq{
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

	var resultBytes []byte
	resultBytes, err = k.mq.SendRPC("application/json", data)

	if err != nil {
		return nil, err
	}

	resultVariant := koinos_types.NewBlockStoreResp()
	err = json.Unmarshal(resultBytes, resultVariant)
	if err != nil {
		return nil, nil
	}

	result, ok := resultVariant.Value.(*koinos_types.GetBlocksByHeightResp)
	if !ok {
		return nil, errors.New("Unexpected return type")
	}

	return result, nil
}

// GetChainID rpc call
func (k KoinosRPC) GetChainID() (*koinos_types.GetChainIDResult, error) {
	args := koinos_types.QueryParamItem{
		Value: koinos_types.NewChainRPCParams(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var resultBytes []byte
	resultBytes, err = k.mq.SendRPC("application/json", data)

	if err != nil {
		return nil, err
	}

	resultVariant := koinos_types.NewSubmissionResult()
	err = json.Unmarshal(resultBytes, resultVariant)
	if err != nil {
		return nil, nil
	}

	var result *koinos_types.GetChainIDResult

	switch t := resultVariant.Value.(type) {
	case *koinos_types.GetChainIDResult:
		result = t
	case *koinos_types.RPCError:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexptected return type")
	}

	return result, err
}
