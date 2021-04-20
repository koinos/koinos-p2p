package rpc

import (
	"context"
	"encoding/json"
	"errors"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
)

// RPC service constants
const (
	ChainRPC      = "chain"
	BlockStoreRPC = "block_store"
)

// KoinosRPC Implementation of RPC Interface
type KoinosRPC struct {
	mq *koinosmq.Client
}

// NewKoinosRPC factory
func NewKoinosRPC(mq *koinosmq.Client) *KoinosRPC {
	rpc := new(KoinosRPC)
	rpc.mq = mq
	return rpc
}

// GetHeadBlock rpc call
func (k *KoinosRPC) GetHeadBlock(ctx context.Context) (*types.GetHeadInfoResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetHeadInfoRequest(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)

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
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// ApplyBlock rpc call
// TODO:  Block should be OpaqueBlock - No it shouldn't
func (k *KoinosRPC) ApplyBlock(ctx context.Context, block *types.Block) (bool, error) {
	blockSub := types.NewSubmitBlockRequest()
	blockSub.Block = *block

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
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)

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
func (k *KoinosRPC) ApplyTransaction(ctx context.Context, trx *types.Transaction) (bool, error) {
	trxSub := types.NewSubmitTransactionRequest()
	trxSub.Transaction = *trx

	trxSub.VerifyPassiveData = true
	trxSub.VerifyTransactionSignatures = true

	args := types.ChainRPCRequest{
		Value: trxSub,
	}
	data, err := json.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)

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
	case *types.SubmitTransactionResponse:
		response = true
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		response = false
	}

	return response, err
}

// GetBlocksByID rpc call
func (k *KoinosRPC) GetBlocksByID(ctx context.Context, blockID *types.VectorMultihash) (*types.GetBlocksByIDResponse, error) {
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
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", BlockStoreRPC, data)
	if err != nil {
		return nil, err
	}

	log.Debugf("GetBlocksByID() response: %s", responseBytes)

	responseVariant := types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
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
func (k *KoinosRPC) GetBlocksByHeight(ctx context.Context, blockID *types.Multihash, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetBlocksByHeightResponse, error) {
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
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", BlockStoreRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
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
func (k *KoinosRPC) GetAncestorTopologyAtHeights(ctx context.Context, blockID *types.Multihash, heights []types.BlockHeightType) ([]types.BlockTopology, error) {
	result := make([]types.BlockTopology, len(heights))

	for i, h := range heights {
		resp, err := k.GetBlocksByHeight(ctx, blockID, h, 1)
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

		result[i].ID = resp.BlockItems[0].BlockID
		result[i].Height = resp.BlockItems[0].BlockHeight
		result[i].Previous = block.Header.Previous
	}

	return result, nil
}

// GetChainID rpc call
func (k *KoinosRPC) GetChainID(ctx context.Context) (*types.GetChainIDResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetChainIDRequest(),
	}
	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)
	log.Debugf("GetChainID() response was %s", responseBytes)

	if err != nil {
		return nil, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *types.GetChainIDResponse

	switch t := responseVariant.Value.(type) {
	case *types.GetChainIDResponse:
		response = (*types.GetChainIDResponse)(t)
	case *types.ChainErrorResponse:
		err = errors.New(string(t.ErrorText))
	default:
		err = errors.New("Unexpected return type")
	}

	return response, err
}

// GetForkHeads rpc call
func (k *KoinosRPC) GetForkHeads(ctx context.Context) (*types.GetForkHeadsResponse, error) {
	args := types.ChainRPCRequest{
		Value: types.NewGetForkHeadsRequest(),
	}

	data, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)

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
func (k *KoinosRPC) GetTopologyAtHeight(ctx context.Context, height types.BlockHeightType, numBlocks types.UInt32) (*types.GetForkHeadsResponse, []types.BlockTopology, error) {
	forkHeads, err := k.GetForkHeads(ctx)
	if err != nil {
		log.Warnf("GetTopologyAtHeight(%d, %d) returned error %s after GetForkHeads()", height, numBlocks, err.Error())
		return nil, nil, err
	}
	if numBlocks == 0 {
		return forkHeads, []types.BlockTopology{}, nil
	}

	topologySet := make(map[util.BlockTopologyCmp]util.Void)
	topologySlice := make([]types.BlockTopology, 0, len(forkHeads.ForkHeads))

	for _, head := range forkHeads.ForkHeads {
		blocks, err := k.GetBlocksByHeight(ctx, &head.ID, height, numBlocks)
		if err != nil {
			headStr, err2 := json.Marshal(head)
			if err2 != nil {
				log.Warnf("GetTopologyAtHeight(%d, %d) tried to print error %s but got another error %s", height, numBlocks, err, err2)
			}

			log.Warnf("GetTopologyAtHeight(%d, %d) returned error %s after GetBlocksByHeight(), head=%s", height, numBlocks, err, headStr)
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

				topology.Previous = block.Header.Previous
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

// IsConnectedToBlockStore returns if the AMQP connection can currently communicate
// with the block store microservice.
func (k *KoinosRPC) IsConnectedToBlockStore(ctx context.Context) (bool, error) {
	args := types.BlockStoreRequest{
		Value: &types.BlockStoreReservedRequest{},
	}

	data, err := json.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", BlockStoreRPC, data)

	if err != nil {
		return false, err
	}

	responseVariant := types.NewBlockStoreResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, err
	}

	return true, nil
}

// IsConnectedToChain returns if the AMQP connection can currently communicate
// with the chain microservice.
func (k *KoinosRPC) IsConnectedToChain(ctx context.Context) (bool, error) {
	args := types.ChainRPCRequest{
		Value: &types.ChainReservedRequest{},
	}

	data, err := json.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/json", ChainRPC, data)

	if err != nil {
		return false, err
	}

	responseVariant := types.NewChainRPCResponse()
	err = json.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, err
	}

	return true, nil
}
