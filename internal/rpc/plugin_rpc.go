package rpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/plugin"
)

const (
	PluginPrefix = "plugin."
)

// PluginRPC implements LocalRPC implementation by communicating with a local plugin via AMQP
type PluginRPC struct {
	mq   *koinosmq.Client
	Name string
}

// NewPluginRPC factory
func NewPluginRPC(mq *koinosmq.Client, name string) *PluginRPC {
	rpc := new(PluginRPC)
	rpc.mq = mq
	rpc.Name = PluginPrefix + name
	return rpc
}

// IsConnectedToPlugin returns if the AMQP connection can currently communicate
// with the plugin microservice.
func (k *PluginRPC) IsConnectedToPlugin(ctx context.Context) (bool, error) {
	args := &plugin.PluginRequest{
		Request: &plugin.PluginRequest_Reserved{
			Reserved: &rpc.ReservedRpc{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return false, fmt.Errorf("%w IsConnectedToPlugin, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", k.Name, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("%w IsConnectedToPlugin, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return false, fmt.Errorf("%w IsConnectedToPlugin, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &plugin.PluginResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, fmt.Errorf("%w IsConnectedToPlugin, %s", p2perrors.ErrDeserialization, err)
	}

	return true, nil
}

// SubmitData submits data to the plugin microservice.
func (k *PluginRPC) SubmitData(ctx context.Context, data []byte) error {
	args := &plugin.PluginRequest{
		Request: &plugin.PluginRequest_SubmitData{
			SubmitData: &plugin.SubmitDataRequest{
				Data: data,
			},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return fmt.Errorf("%w SubmitData, %s", p2perrors.ErrSerialization, err)
	}

	var responseBytes []byte
	responseBytes, err = k.mq.RPCContext(ctx, "application/octet-stream", k.Name, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w SubmitData, %s", p2perrors.ErrLocalRPCTimeout, err)
		}
		return fmt.Errorf("%w SubmitData, %s", p2perrors.ErrLocalRPC, err)
	}

	responseVariant := &plugin.PluginResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return fmt.Errorf("%w SubmitData, %s", p2perrors.ErrDeserialization, err)
	}

	return nil
}
