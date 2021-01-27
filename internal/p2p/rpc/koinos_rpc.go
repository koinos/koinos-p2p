package p2p

import (
	"encoding/json"

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
func (k KoinosRPC) GetHeadBlock() (*koinos_types.BlockTopology, error) {
	args := koinos_types.QueryParamItem{
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

	result := koinos_types.NewBlockTopology()
	err = json.Unmarshal(resultBytes, result)

	return result, err
}

// ApplyBlock rpc call
func (k KoinosRPC) ApplyBlock(block *koinos_types.Block) (bool, error) {
	blockSub := koinos_types.NewBlockSubmission()
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

	// TODO: Koinosd needs to return a proper response object
	result := koinos_types.NewBoolean()
	err = json.Unmarshal(resultBytes, result)

	return bool(*result), err
}

// GetBlocksByHeight rpc call
func (k KoinosRPC) GetBlocksByHeight(blockID *koinos_types.Multihash, height koinos_types.BlockHeightType, numBlocks koinos_types.UInt32) (*koinos_types.GetBlocksByHeightResp, error) {
	args := koinos_types.BlockStoreReq{
		Value: koinos_types.GetBlocksByHeightReq{
			HeadBlockID:         *blockID,
			AncestorStartHeight: height,
			NumBlocks:           numBlocks,
			ReturnBlockBlob:     true,
			ReturnReceiptBlob:   false,
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

	result := koinos_types.NewGetBlocksByHeightResp()
	err = json.Unmarshal(resultBytes, result)

	return result, err
}

// GetChainID rpc call
func (k KoinosRPC) GetChainID() (*koinos_types.GetChainIDResult, error) {
	args := koinos_types.QueryParamItem{
		Value: koinos_types.NewGetChainIDParams(),
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

	result := koinos_types.NewGetChainIDResult()
	err = json.Unmarshal(resultBytes, result)

	return result, err
}
