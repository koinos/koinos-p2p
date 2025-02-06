package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Masterminds/semver/v3"
	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// PeerConnection handles the sync portion of a connection to a peer
type PeerConnection struct {
	id       peer.ID
	version  *semver.Version
	isSynced bool
	opts     *options.PeerConnectionOptions

	libProvider     LastIrreversibleBlockProvider
	localRPC        rpc.LocalRPC
	peerRPC         rpc.RemoteRPC
	applicator      *Applicator
	peerErrorChan   chan<- PeerError
	versionProvider ProtocolVersionProvider
}

func (p *PeerConnection) handshake(ctx context.Context) error {
	// Check Peer's protocol version
	version, err := p.versionProvider.GetProtocolVersion(ctx, p.id)
	if err == nil {
		p.version = version
	} else {
		// TODO: Remove to reject when protocol is missing
		if errors.Is(err, p2perrors.ErrProtocolMissing) {
			p.version = semver.New(0, 0, 0, "", "")
		} else {
			return err
		}
	}

	// Get my chain id
	rpcContext, cancelLocalGetChainID := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
	defer cancelLocalGetChainID()
	myChainID, err := p.localRPC.GetChainID(rpcContext)
	if err != nil {
		return err
	}

	// Get peer's chain id
	rpcContext, cancelPeerGetChainID := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelPeerGetChainID()
	peerChainID, err := p.peerRPC.GetChainID(rpcContext)
	if err != nil {
		return p2perrors.ErrChainIDMismatch
	}

	if !bytes.Equal(myChainID.ChainId, peerChainID) {
		return p2perrors.ErrChainIDMismatch
	}

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelGetPeerHead()
	peerHeadID, _, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	for _, checkpoint := range p.opts.Checkpoints {
		rpcContext, cancel := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
		defer cancel()
		peerBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, checkpoint.BlockHeight)
		if err != nil {
			return p2perrors.ErrCheckpointMismatch
		}

		if !bytes.Equal(peerBlock, checkpoint.BlockID) {
			return p2perrors.ErrCheckpointMismatch
		}
	}

	return nil
}

func (p *PeerConnection) requestSyncBlocks(ctx context.Context) error {
	// Get my last irreversible block
	lib := p.libProvider.GetLastIrreversibleBlock()

	// Get peer's head block
	rpcContext, cancelGetPeerHead := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
	defer cancelGetPeerHead()
	peerHeadID, peerHeadHeight, err := p.peerRPC.GetHeadBlock(rpcContext)
	if err != nil {
		return err
	}

	// If the peer is in the past, it is not an error, but we don't need anything from them
	if peerHeadHeight <= lib.Height {
		p.isSynced = true
		return nil
	}

	// If we already know about the peer's head block, don't request anything
	rpcContext, cancelCheckPeerHeadBlock := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
	defer cancelCheckPeerHeadBlock()
	localBlocks, err := p.localRPC.GetBlocksByID(rpcContext, []multihash.Multihash{peerHeadID})
	if err != nil {
		return err
	}

	if len(localBlocks.BlockItems) != 1 {
		return fmt.Errorf("%w: unexpected number of block items returned", p2perrors.ErrLocalRPC)
	}

	// We already know of the peer's head block, don't request anything and consider ourselves sycned
	if localBlocks.BlockItems[0].BlockHeight != 0 {
		p.isSynced = true
		return nil
	}

	// If LIB is 0, we are still at genesis and could connect to any chain
	if lib.Height > 0 {
		// Check if my LIB connect's to peer's head block
		rpcContext, cancelGetAncestorBlock := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
		defer cancelGetAncestorBlock()
		ancestorBlock, err := p.peerRPC.GetAncestorBlockID(rpcContext, peerHeadID, lib.Height)
		if err != nil {
			return err
		}

		if !bytes.Equal([]byte(ancestorBlock), lib.Id) {
			return p2perrors.ErrChainNotConnected
		}
	}

	startRequestBlock := lib.Height

	// If we are synced, try and narrow the range of blocks we may need.
	// With a 60 block irreversibility, this will loop a maximum of 6 times
	if p.isSynced {
		localRpcContext, cancelLocalGetHeadBlock := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
		defer cancelLocalGetHeadBlock()
		myHeadBlock, err := p.localRPC.GetHeadBlock(localRpcContext)
		if err != nil {
			return err
		}

		headHeight := uint64(math.Min(float64(myHeadBlock.HeadTopology.Height), float64(peerHeadHeight)))

		for startRequestBlock < headHeight {
			stepSize := uint64(math.Round(float64(headHeight-startRequestBlock) / 2))
			newStart := startRequestBlock + stepSize

			localRpcContext, cancelLocalGetBlocksByHeight := context.WithTimeout(ctx, p.opts.LocalRPCTimeout)
			defer cancelLocalGetBlocksByHeight()
			localBlocks, err := p.localRPC.GetBlocksByHeight(localRpcContext, myHeadBlock.HeadTopology.Id, newStart, 1)
			if err != nil {
				return err
			}
			if len(localBlocks.BlockItems) != 1 {
				return fmt.Errorf("%w: unexpected number of block items returned", p2perrors.ErrLocalRPC)
			}

			peerRpcContext, cancelPeerGetBlocksByHeight := context.WithTimeout(ctx, p.opts.RemoteRPCTimeout)
			defer cancelPeerGetBlocksByHeight()
			peerBlocks, err := p.peerRPC.GetBlocks(peerRpcContext, peerHeadID, newStart, 1)
			if err != nil {
				return err
			}
			if len(peerBlocks) != 1 {
				return fmt.Errorf("%w: unexpected number of block items returned", p2perrors.ErrLocalRPC)
			}

			if bytes.Equal(peerBlocks[0].Id, localBlocks.BlockItems[0].BlockId) {
				startRequestBlock = newStart
			} else {
				break
			}
		}
	}

	blocksToRequest := peerHeadHeight - startRequestBlock
	if blocksToRequest > p.opts.BlockRequestBatchSize {
		blocksToRequest = p.opts.BlockRequestBatchSize
	}

	if blocksToRequest == 0 {
		p.isSynced = true
		return nil
	}

	// Request blocks
	if blocksToRequest == p.opts.BlockRequestBatchSize {
		log.Infof("Requesting blocks %v-%v from peer %s", startRequestBlock+1, startRequestBlock+1+blocksToRequest, p.id)
	}

	rpcContext, cancelGetBlocks := context.WithTimeout(ctx, p.opts.BlockRequestTimeout)
	defer cancelGetBlocks()
	blocks, err := p.peerRPC.GetBlocks(rpcContext, peerHeadID, startRequestBlock+1, uint32(blocksToRequest))
	if err != nil {
		return err
	}

	// Apply blocks to local node
	for i := range blocks {
		applicatorContext, cancelApplyBlock := context.WithTimeout(ctx, p.opts.ApplicatorTimeout)
		defer cancelApplyBlock()

		err = p.applicator.ApplyBlock(applicatorContext, &blocks[i])

		if err != nil {
			// If it was a local RPC timeout, do not wrap it
			if errors.Is(err, p2perrors.ErrLocalRPCTimeout) {
				return err
			}

			if errors.Is(err, p2perrors.ErrForkBomb) {
				return err
			}

			// If we are applying a now irreversible block, it is probably we synced further with another peer,
			// just keep applying blocks until we are caught up or we encounter a different error.
			if errors.Is(err, p2perrors.ErrBlockIrreversibility) {
				continue
			}

			return fmt.Errorf("%w: %s", p2perrors.ErrBlockApplication, err.Error())
		}
	}

	// We will consider ourselves as syncing if we have more than 5 blocks to sync
	p.isSynced = peerHeadHeight-blocks[len(blocks)-1].Header.Height < p.opts.SyncedBlockDelta

	return nil
}

func (p *PeerConnection) connectionLoop(ctx context.Context) {
	for {
		// Request sync blocks.
		// If there is an error, report it
		err := p.requestSyncBlocks(ctx)
		if err != nil {
			select {
			case p.peerErrorChan <- PeerError{id: p.id, err: err}:
			case <-ctx.Done():
				return
			}
		}

		// Get sleep time if we are synced or not
		sleepTime := time.Second
		if p.isSynced {
			sleepTime = p.opts.SyncedSleepTime
		}

		// Sleep and then repeat
		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return
		}
	}
}

// Start syncing to the peer
func (p *PeerConnection) Start(ctx context.Context) {
	go func() {
		for {
			// Does the handshake in a loop until we are successful
			// or the connection is closed, sleeping between attempts
			err := p.handshake(ctx)
			if err != nil {
				go func() {
					select {
					case p.peerErrorChan <- PeerError{id: p.id, err: err}:
					case <-ctx.Done():
					}
				}()
			} else {
				go p.connectionLoop(ctx)
				return
			}
			select {
			case <-time.After(p.opts.HandshakeRetryTime):
			case <-ctx.Done():
				return
			}
		}
	}()
}

// NewPeerConnection creates a PeerConnection
func NewPeerConnection(
	id peer.ID,
	libProvider LastIrreversibleBlockProvider,
	localRPC rpc.LocalRPC,
	peerRPC rpc.RemoteRPC,
	peerErrorChan chan<- PeerError,
	opts *options.PeerConnectionOptions,
	applicator *Applicator,
	versionProvider ProtocolVersionProvider) *PeerConnection {
	return &PeerConnection{
		id:              id,
		isSynced:        false,
		opts:            opts,
		libProvider:     libProvider,
		localRPC:        localRPC,
		peerRPC:         peerRPC,
		applicator:      applicator,
		peerErrorChan:   peerErrorChan,
		versionProvider: versionProvider,
	}
}
