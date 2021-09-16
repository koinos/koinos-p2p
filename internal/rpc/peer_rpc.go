package rpc

import (
	"context"
	"encoding/hex"

	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	peer "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type PeerRPC struct {
	client *gorpc.Client
	peerID peer.ID
}

func NewPeerRPC(client *gorpc.Client, peerID peer.ID) *PeerRPC {
	return &PeerRPC{client: client, peerID: peerID}
}

func (p *PeerRPC) GetChainID(ctx context.Context) (id *multihash.Multihash, err error) {
	rpcReq := &GetChainIDRequest{}
	rpcResp := &GetChainIDResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetChainID", rpcReq, rpcResp)
	return &rpcResp.ID, err
}

func (p *PeerRPC) GetHeadBlock(ctx context.Context) (id *multihash.Multihash, height uint64, err error) {
	rpcReq := &GetHeadBlockRequest{}
	rpcResp := &GetHeadBlockResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetHeadBlock", rpcReq, rpcResp)
	return &rpcResp.ID, rpcResp.Height, err
}

func (p *PeerRPC) GetAncestorBlockID(ctx context.Context, parentID *multihash.Multihash, childHeight uint64) (id *multihash.Multihash, err error) {
	rpcReq := &GetAncestorBlockIDRequest{
		ParentID:    parentID,
		ChildHeight: childHeight,
	}
	rpcResp := &GetAncestorBlockIDResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetAncestorBlockID", rpcReq, rpcResp)
	return &rpcResp.ID, err
}

func (p *PeerRPC) GetBlocks(ctx context.Context, headBlockID *multihash.Multihash, startBlockHeight uint64, numBlocks uint32) (blocks []protocol.Block, err error) {
	rpcReq := &GetBlocksRequest{
		HeadBlockID:      headBlockID,
		StartBlockHeight: startBlockHeight,
		NumBlocks:        numBlocks,
	}
	rpcResp := &GetBlocksResponse{}
	err = p.client.CallContext(ctx, p.peerID, "PeerRPCService", "GetBlocks", rpcReq, rpcResp)
	if err != nil {
		return nil, err
	}

	blocks = make([]protocol.Block, len(rpcResp.Blocks))

	for i, blockBytes := range rpcResp.Blocks {
		log.Info(hex.EncodeToString(blockBytes))
		err = proto.Unmarshal(blockBytes, &blocks[i])
		if err != nil {
			return nil, err
		}
		bytes, _ := protojson.Marshal(&blocks[i])
		log.Info(string(bytes))
	}

	return blocks, nil
}
