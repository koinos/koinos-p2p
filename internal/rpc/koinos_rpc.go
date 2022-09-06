package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/canonical"
	"github.com/koinos/koinos-proto-golang/koinos/chain"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	chainrpc "github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
)

// RPC service constants
const (
	ChainRPC      = "chain"
	BlockStoreRPC = "block_store"
)

type chainError struct {
	Code int64 `json:"code"`
}

// KoinosRPC implements LocalRPC implementation by communicating with a local Koinos node via AMQP
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
func (k *KoinosRPC) GetHeadBlock(ctx context.Context) (*chainrpc.GetHeadInfoResponse, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_GetHeadInfo{
			GetHeadInfo: &chainrpc.GetHeadInfoRequest{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w GetHeadBlock, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w GetHeadBlock, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w GetHeadBlock, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w GetHeadBlock, %s", p2perrors.ErrDeserialization, err)
	}

	var response *chainrpc.GetHeadInfoResponse

	switch t := responseVariant.Response.(type) {
	case *chainrpc.ChainResponse_GetHeadInfo:
		response = t.GetHeadInfo
	case *chainrpc.ChainResponse_Error:
		err = fmt.Errorf("%w GetHeadBlock, chain rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w GetHeadBlock, unexpected chain rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// ApplyBlock rpc call
func (k *KoinosRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chainrpc.SubmitBlockResponse, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_SubmitBlock{
			SubmitBlock: &chainrpc.SubmitBlockRequest{
				Block: block,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w ApplyBlock, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w ApplyBlock, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w ApplyBlock, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w ApplyBlock, %s", p2perrors.ErrDeserialization, err)
	}

	var response *chainrpc.SubmitBlockResponse

	switch t := responseVariant.Response.(type) {
	case *chainrpc.ChainResponse_SubmitBlock:
		response = t.SubmitBlock
	case *chainrpc.ChainResponse_Error:
		eData := chainError{}
		if jsonErr := json.Unmarshal([]byte(responseVariant.Response.(*chainrpc.ChainResponse_Error).Error.Data), &eData); jsonErr != nil {
			if eData.Code == int64(chain.ErrorCode_unknown_previous_block) {
				err = p2perrors.ErrUnknownPreviousBlock
				break
			} else if eData.Code == int64(chain.ErrorCode_pre_irreversibility_block) {
				err = p2perrors.ErrBlockIrreversibility
				break
			}
		}
		err = fmt.Errorf("%w ApplyBlock, chain rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w ApplyBlock, unexpected chain rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// ApplyTransaction rpc call
func (k *KoinosRPC) ApplyTransaction(ctx context.Context, trx *protocol.Transaction) (*chainrpc.SubmitTransactionResponse, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_SubmitTransaction{
			SubmitTransaction: &chainrpc.SubmitTransactionRequest{
				Transaction: trx,
				Broadcast:   true,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w ApplyTransaction, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w ApplyTransaction, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w ApplyTransaction, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w ApplyTransaction, %s", p2perrors.ErrDeserialization, err)
	}

	var response *chainrpc.SubmitTransactionResponse

	switch t := responseVariant.Response.(type) {
	case *chainrpc.ChainResponse_SubmitTransaction:
		response = t.SubmitTransaction
	case *chainrpc.ChainResponse_Error:
		err = fmt.Errorf("%w ApplyTransaction, chain rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w ApplyTransaction, unexpected chain rpc response", p2perrors.ErrLocalRPC)
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
				BlockIds:      idBytes,
				ReturnBlock:   true,
				ReturnReceipt: false,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w GetBlocksByID, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w GetBlocksByID, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w GetBlocksByID, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w GetBlocksByID, %s", p2perrors.ErrDeserialization, err)
	}

	var response *block_store.GetBlocksByIdResponse

	switch t := responseVariant.Response.(type) {
	case *block_store.BlockStoreResponse_GetBlocksById:
		response = t.GetBlocksById
	case *block_store.BlockStoreResponse_Error:
		err = fmt.Errorf("%w GetBlocksByID, block_store rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w GetBlocksByID, unexpected block_store rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// GetBlocksByHeight rpc call
func (k *KoinosRPC) GetBlocksByHeight(ctx context.Context, blockID multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	args := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_GetBlocksByHeight{
			GetBlocksByHeight: &block_store.GetBlocksByHeightRequest{
				HeadBlockId:         blockID,
				AncestorStartHeight: height,
				NumBlocks:           numBlocks,
				ReturnBlock:         true,
				ReturnReceipt:       false,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w GetBlocksByHeight, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w GetBlocksByHeight, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w GetBlocksByHeight, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w GetBlocksByHeight, %s", p2perrors.ErrDeserialization, err)
	}

	var response *block_store.GetBlocksByHeightResponse

	switch t := responseVariant.Response.(type) {
	case *block_store.BlockStoreResponse_GetBlocksByHeight:
		response = t.GetBlocksByHeight
	case *block_store.BlockStoreResponse_Error:
		err = fmt.Errorf("%w GetBlocksByHeight, block_store rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w GetBlocksByHeight, unexpected block_store rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// GetChainID rpc call
func (k *KoinosRPC) GetChainID(ctx context.Context) (*chainrpc.GetChainIdResponse, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_GetChainId{
			GetChainId: &chainrpc.GetChainIdRequest{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w GetChainID, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w GetChainID, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w GetChainID, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w GetChainID, %s", p2perrors.ErrDeserialization, err)
	}

	var response *chainrpc.GetChainIdResponse

	switch t := responseVariant.Response.(type) {
	case *chainrpc.ChainResponse_GetChainId:
		response = t.GetChainId
	case *chainrpc.ChainResponse_Error:
		err = fmt.Errorf("%w GetChainID, chain rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w GetChainID, unexpected chain rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// GetForkHeads rpc call
func (k *KoinosRPC) GetForkHeads(ctx context.Context) (*chainrpc.GetForkHeadsResponse, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_GetForkHeads{
			GetForkHeads: &chainrpc.GetForkHeadsRequest{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w GetForkHeads, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w GetForkHeads, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w GetForkHeads, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return nil, fmt.Errorf("%w, %s", p2perrors.ErrDeserialization, err)
	}

	var response *chainrpc.GetForkHeadsResponse

	switch t := responseVariant.Response.(type) {
	case *chainrpc.ChainResponse_GetForkHeads:
		response = t.GetForkHeads
	case *chainrpc.ChainResponse_Error:
		err = fmt.Errorf("%w GetForkHeads, chain rpc error, %s", p2perrors.ErrLocalRPC, string(t.Error.GetMessage()))
	default:
		err = fmt.Errorf("%w GetForkHeads, unexpected chain rpc response", p2perrors.ErrLocalRPC)
	}

	return response, err
}

// BroadcastGossipStatus broadcasts the gossip status to the
func (k *KoinosRPC) BroadcastGossipStatus(enabled bool) error {
	status := &broadcast.GossipStatus{Enabled: enabled}
	data, err := canonical.Marshal(status)
	if err != nil {
		return fmt.Errorf("%w BroadcastGossipStatus, %s", p2perrors.ErrSerialization, err)
	}

	return k.mq.Broadcast("application/octet-stream", "koinos.gossip.status", data)
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
		return false, fmt.Errorf("%w IsConnectedToBlockStore, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", BlockStoreRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("%w IsConnectedToBlockStore, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return false, fmt.Errorf("%w IsConnectedToBlockStore, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, fmt.Errorf("%w IsConnectedToBlockStore, %s", p2perrors.ErrDeserialization, err)
	}

	return true, nil
}

// IsConnectedToChain returns if the AMQP connection can currently communicate
// with the chain microservice.
func (k *KoinosRPC) IsConnectedToChain(ctx context.Context) (bool, error) {
	args := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_Reserved{
			Reserved: &rpc.ReservedRpc{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return false, fmt.Errorf("%w IsConnectedToChain, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", ChainRPC, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("%w IsConnectedToChain, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return false, fmt.Errorf("%w IsConnectedToChain, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &chainrpc.ChainResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, fmt.Errorf("%w IsConnectedToChain, %s", p2perrors.ErrDeserialization, err)
	}

	return true, nil
}
