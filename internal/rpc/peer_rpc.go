package rpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"
)

// PeerRPC implements RemoteRPC interface by communicating via libp2p's gorpc
type PeerRPC struct {
	client *gorpc.Client
	peerID peer.ID
}

// NewPeerRPC creates a PeerRPC
func NewPeerRPC(client *gorpc.Client, peerID peer.ID) *PeerRPC {
	return &PeerRPC{client: client, peerID: peerID}
}

// GetChainID rpc call
func (p *PeerRPC) GetChainID(ctx context.Context) (id multihash.Multihash, err error) {
	rpcReq := &GetChainIDRequest{}
	rpcResp := &GetChainIDResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetChainID", rpcReq, rpcResp)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPCTimeout, err)
		}
		err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPC, err)
	}
	return rpcResp.ID, err
}

// GetHeadBlock rpc call
func (p *PeerRPC) GetHeadBlock(ctx context.Context) (id multihash.Multihash, height uint64, err error) {
	rpcReq := &GetHeadBlockRequest{}
	rpcResp := &GetHeadBlockResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetHeadBlock", rpcReq, rpcResp)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPCTimeout, err)
		}
		err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPC, err)
	}
	return rpcResp.ID, rpcResp.Height, err
}

// GetAncestorBlockID rpc call
func (p *PeerRPC) GetAncestorBlockID(ctx context.Context, parentID multihash.Multihash, childHeight uint64) (id multihash.Multihash, err error) {
	rpcReq := &GetAncestorBlockIDRequest{
		ParentID:    parentID,
		ChildHeight: childHeight,
	}
	rpcResp := &GetAncestorBlockIDResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetAncestorBlockID", rpcReq, rpcResp)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPCTimeout, err)
		}
		err = fmt.Errorf("%w, %s", p2perrors.ErrPeerRPC, err)
	}
	return rpcResp.ID, err
}

// GetBlocks rpc call
func (p *PeerRPC) GetBlocks(ctx context.Context, headBlockID multihash.Multihash, startBlockHeight uint64, numBlocks uint32) (blocks []protocol.Block, err error) {
	rpcReq := &GetBlocksRequest{
		HeadBlockID:      headBlockID,
		StartBlockHeight: startBlockHeight,
		NumBlocks:        numBlocks,
	}
	rpcResp := &GetBlocksResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetBlocks", rpcReq, rpcResp)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w, %s", p2perrors.ErrPeerRPCTimeout, err)
		}
		return nil, fmt.Errorf("%w, %s", p2perrors.ErrPeerRPC, err)
	}

	blocks = make([]protocol.Block, len(rpcResp.Blocks))

	for i, blockBytes := range rpcResp.Blocks {
		err = proto.Unmarshal(blockBytes, &blocks[i])
		if err != nil {
			return nil, fmt.Errorf("%w, %s", p2perrors.ErrDeserialization, err)
		}
	}

	return blocks, nil
}
