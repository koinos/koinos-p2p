package rpc

import (
	"context"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
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
func (k *KoinosRPC) GetHeadBlock(ctx context.Context) (*chain.GetHeadInfoResponse, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_GetHeadInfo{
			GetHeadInfo: &chain.GetHeadInfoRequest{},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *chain.GetHeadInfoResponse

	switch t := responseVariant.Response.(type) {
	case *chain.ChainResponse_GetHeadInfo:
		response = t.GetHeadInfo
	case *chain.ChainResponse_Error:
		err = errors.New("chain rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected chain rpc response")
	}

	return response, err
}

// ApplyBlock rpc call
func (k *KoinosRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chain.SubmitBlockResponse, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_SubmitBlock{
			SubmitBlock: &chain.SubmitBlockRequest{
				Block:                      block,
				VerifyPassiveData:          true,
				VerifyBlockSignature:       true,
				VerifyTransactionSignature: true,
			},
		},
	}

	log.Infof("%s", args.String())

	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *chain.SubmitBlockResponse

	switch t := responseVariant.Response.(type) {
	case *chain.ChainResponse_SubmitBlock:
		response = t.SubmitBlock
	case *chain.ChainResponse_Error:
		err = errors.New("chain rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected chain rpc response")
	}

	return response, err
}

// ApplyTransaction rpc call
func (k *KoinosRPC) ApplyTransaction(ctx context.Context, trx *protocol.Transaction) (*chain.SubmitTransactionResponse, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_SubmitTransaction{
			SubmitTransaction: &chain.SubmitTransactionRequest{
				Transaction: trx,
			},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *chain.SubmitTransactionResponse

	switch t := responseVariant.Response.(type) {
	case *chain.ChainResponse_SubmitTransaction:
		response = t.SubmitTransaction
	case *chain.ChainResponse_Error:
		err = errors.New("chain rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected chain rpc response")
	}

	return response, err
}

// GetBlocksByID rpc call
func (k *KoinosRPC) GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error) {
	var idBytes = make([][]byte, len(blockIDs))
	for i, id := range blockIDs {
		idBytes[i] = []byte(id)
	}

	args := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_GetBlocksById{
			GetBlocksById: &block_store.GetBlocksByIdRequest{
				BlockId:       idBytes,
				ReturnBlock:   true,
				ReturnReceipt: false,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)
	if err != nil {
		return nil, err
	}

	log.Debugf("GetBlocksByID() response: %s", responseBytes)

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *block_store.GetBlocksByIdResponse

	switch t := responseVariant.Response.(type) {
	case *block_store.BlockStoreResponse_GetBlocksById:
		response = t.GetBlocksById
	case *block_store.BlockStoreResponse_Error:
		err = errors.New("block_store rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected block_store rpc response")
	}

	return response, err
}

// GetBlocksByHeight rpc call
func (k *KoinosRPC) GetBlocksByHeight(ctx context.Context, blockID *multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	args := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_GetBlocksByHeight{
			GetBlocksByHeight: &block_store.GetBlocksByHeightRequest{
				HeadBlockId:         *blockID,
				AncestorStartHeight: height,
				NumBlocks:           numBlocks,
				ReturnBlock:         true,
				ReturnReceipt:       false,
			},
		},
	}

	json, _ := protojson.Marshal(args)
	log.Info(string(json))
	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *block_store.GetBlocksByHeightResponse

	switch t := responseVariant.Response.(type) {
	case *block_store.BlockStoreResponse_GetBlocksByHeight:
		response = t.GetBlocksByHeight
	case *block_store.BlockStoreResponse_Error:
		err = errors.New("block_store rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected block_store rpc response")
	}

	return response, err
}

// GetChainID rpc call
func (k *KoinosRPC) GetChainID(ctx context.Context) (*chain.GetChainIdResponse, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_GetChainId{
			GetChainId: &chain.GetChainIdRequest{},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	log.Debugf("GetChainID() response was %s", responseBytes)

	if err != nil {
		return nil, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *chain.GetChainIdResponse

	switch t := responseVariant.Response.(type) {
	case *chain.ChainResponse_GetChainId:
		response = t.GetChainId
	case *chain.ChainResponse_Error:
		err = errors.New("chain rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected chain rpc response")
	}

	return response, err
}

// GetForkHeads rpc call
func (k *KoinosRPC) GetForkHeads(ctx context.Context) (*chain.GetForkHeadsResponse, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_GetForkHeads{
			GetForkHeads: &chain.GetForkHeadsRequest{},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return nil, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)

	if err != nil {
		return nil, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, err
	}

	var response *chain.GetForkHeadsResponse

	switch t := responseVariant.Response.(type) {
	case *chain.ChainResponse_GetForkHeads:
		response = t.GetForkHeads
	case *chain.ChainResponse_Error:
		err = errors.New("chain rpc error, " + string(t.Error.GetMessage()))
	default:
		err = errors.New("unexpected chain rpc response")
	}

	return response, err
}

// IsConnectedToBlockStore returns if the AMQP connection can currently communicate
// with the block store microservice.
func (k *KoinosRPC) IsConnectedToBlockStore(ctx context.Context) (bool, error) {
	args := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_Reserved{
			Reserved: &rpc.ReservedRpc{},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)

	if err != nil {
		return false, err
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, err
	}

	return true, nil
}

// IsConnectedToChain returns if the AMQP connection can currently communicate
// with the chain microservice.
func (k *KoinosRPC) IsConnectedToChain(ctx context.Context) (bool, error) {
	args := &chain.ChainRequest{
		Request: &chain.ChainRequest_Reserved{
			Reserved: &rpc.ReservedRpc{},
		},
	}

	data, err := proto.Marshal(args)

	if err != nil {
		return false, err
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)

	if err != nil {
		return false, err
	}

	responseVariant := &chain.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, err
	}

	return true, nil
}
