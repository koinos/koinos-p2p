package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
)

type applicatorTestRPC struct {
	blocksToFail     map[string]void
	unlinkableBlocks map[string]void
	head             []byte
}

func (b *applicatorTestRPC) GetHeadBlock(ctx context.Context) (*chain.GetHeadInfoResponse, error) {
	return &chain.GetHeadInfoResponse{
		HeadTopology: &koinos.BlockTopology{
			Id:       []byte{0},
			Height:   0,
			Previous: []byte{},
		},
	}, nil
}

func (b *applicatorTestRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chain.SubmitBlockResponse, error) {
	if _, ok := b.blocksToFail[string(block.Id)]; ok {
		return nil, p2perrors.ErrBlockApplication
	}

	if _, ok := b.unlinkableBlocks[string(block.Id)]; ok {
		return nil, p2perrors.ErrUnknownPreviousBlock
	}

	return &chain.SubmitBlockResponse{}, nil
}

func (b *applicatorTestRPC) ApplyTransaction(ctx context.Context, block *protocol.Transaction) (*chain.SubmitTransactionResponse, error) {
	return &chain.SubmitTransactionResponse{}, nil
}

func (b *applicatorTestRPC) GetBlocksByHeight(ctx context.Context, blockIDs multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	return &block_store.GetBlocksByHeightResponse{}, nil
}

func (b *applicatorTestRPC) GetChainID(ctx context.Context) (*chain.GetChainIdResponse, error) {
	return &chain.GetChainIdResponse{}, nil
}

func (b *applicatorTestRPC) GetForkHeads(ctx context.Context) (*chain.GetForkHeadsResponse, error) {
	return &chain.GetForkHeadsResponse{}, nil
}

func (b *applicatorTestRPC) GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error) {
	return &block_store.GetBlocksByIdResponse{}, nil
}

func (b *applicatorTestRPC) BroadcastGossipStatus(enabled bool) error {
	return nil
}

func (b *applicatorTestRPC) IsConnectedToBlockStore(ctx context.Context) (bool, error) {
	return true, nil
}

func (b *applicatorTestRPC) IsConnectedToChain(ctx context.Context) (bool, error) {
	return true, nil
}

func TestApplicator(t *testing.T) {
	ctx := context.Background()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
	}

	applicator, err := NewApplicator(ctx, &rpc, *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	// Blocks will be applied using the following topology
	//
	// 0 -- 1 -- 2a -- 3a
	//           2b  \ 3b
	//
	// These blocks will be applied in the following order
	//
	// 2b (does not connect)
	// 3a (block is in the future, passes)
	// 3b (block is in the future, fails)
	// 2a (success immediately, triggers 3)
	// 1  (succeeds immediately)

	block1 := &protocol.Block{
		Id: []byte{0x01},
		Header: &protocol.BlockHeader{
			Height:   1,
			Previous: []byte{0},
		},
	}

	block2a := &protocol.Block{
		Id: []byte{0x02, 0x0a},
		Header: &protocol.BlockHeader{
			Height:   2,
			Previous: block1.Id,
		},
	}

	block2b := &protocol.Block{
		Id: []byte{0x02, 0x0b},
		Header: &protocol.BlockHeader{
			Height:   2,
			Previous: []byte{0x00, 0x00},
		},
	}

	block3a := &protocol.Block{
		Id: []byte{0x03, 0x0a},
		Header: &protocol.BlockHeader{
			Height:   3,
			Previous: block2a.Id,
		},
	}

	block3b := &protocol.Block{
		Id: []byte{0x03, 0x0b},
		Header: &protocol.BlockHeader{
			Height:   3,
			Previous: block2a.Id,
		},
	}

	rpc.blocksToFail[string(block3b.Id)] = void{}
	rpc.unlinkableBlocks[string(block2b.Id)] = void{}

	applicator.Start(ctx)

	testChan1 := make(chan struct{})

	go func() {
		err := applicator.ApplyBlock(ctx, block2b)
		if err != p2perrors.ErrBlockIrreversibility {
			t.Errorf("block2b - ErrBlockIrreversibility expected but not returned, was: %v", err)
		}
		testChan1 <- struct{}{}
	}()

	testChan2 := make(chan struct{})

	go func() {
		err := applicator.ApplyBlock(ctx, block3a)
		if err != nil {
			t.Error(err)
		}

		rpc.head = block3a.Id

		applicator.HandleBlockBroadcast(
			&broadcast.BlockAccepted{
				Block: block3a,
				Head:  true,
			},
		)

		testChan2 <- struct{}{}
	}()

	testChan3 := make(chan struct{})

	go func() {
		err := applicator.ApplyBlock(ctx, block3b)
		if err != p2perrors.ErrBlockApplication {
			t.Errorf("block3b - ErrBlockApplication expected but not returned, was: %v", err)
		}

		testChan3 <- struct{}{}
	}()

	testChan4 := make(chan struct{})

	go func() {
		err := applicator.ApplyBlock(ctx, block2a)
		if err != nil {
			t.Error(err)
		}

		rpc.head = block2a.Id

		applicator.HandleBlockBroadcast(
			&broadcast.BlockAccepted{
				Block: block2a,
				Head:  true,
			},
		)

		applicator.HandleForkHeads(
			&broadcast.ForkHeads{
				LastIrreversibleBlock: &koinos.BlockTopology{
					Id:       block2a.Id,
					Height:   block2a.Header.Height,
					Previous: block2a.Header.Previous,
				},
			},
		)

		testChan4 <- struct{}{}
	}()

	time.Sleep(100 * time.Millisecond)

	err = applicator.ApplyBlock(ctx, block1)
	if err != nil {
		t.Error(err)
	}
	rpc.head = block1.Id
	applicator.HandleBlockBroadcast(
		&broadcast.BlockAccepted{
			Block: block1,
			Head:  true,
		},
	)

	<-testChan1
	<-testChan2
	<-testChan3
	<-testChan4
}

func TestApplicatorLimits(t *testing.T) {
	ctx := context.Background()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
	}

	applicator, err := NewApplicator(ctx, &rpc, options.ApplicatorOptions{MaxPendingBlocks: 5, MaxHeightDelta: 5})
	if err != nil {
		t.Error(err)
	}

	blocks := make([]*protocol.Block, 0)

	for i := 0; i < 6; i++ {
		blocks = append(blocks, &protocol.Block{
			Id: []byte{byte(i)},
			Header: &protocol.BlockHeader{
				Height:   1,
				Previous: []byte{byte(i)},
			},
		})

		rpc.unlinkableBlocks[string(blocks[i].Id)] = void{}
	}

	applicator.Start(ctx)

	testChans := make([]chan struct{}, 0, 5)

	for i := 0; i < 5; i++ {
		testChans = append(testChans, make(chan struct{}))
		go func(block *protocol.Block, signalChan chan<- struct{}) {
			err := applicator.ApplyBlock(ctx, block)
			if err != p2perrors.ErrBlockIrreversibility {
				t.Errorf("block2b - ErrBlockIrreversibility expected but not returned, was: %v", err)
			}
			signalChan <- struct{}{}
		}(blocks[i], testChans[i])
	}

	time.Sleep(100 * time.Millisecond)

	testChan := make(chan struct{})

	go func() {
		err = applicator.ApplyBlock(ctx, blocks[5])
		if err != p2perrors.ErrMaxPendingBlocks {
			t.Errorf("block2b - ErrMaxPendingBlocks expected but not returned, was: %v", err)
		}

		testChan <- struct{}{}
	}()

	time.Sleep(100 * time.Millisecond)

	applicator.HandleForkHeads(
		&broadcast.ForkHeads{
			LastIrreversibleBlock: &koinos.BlockTopology{
				Height: 2,
			},
		},
	)

	futureBlock := &protocol.Block{
		Id: []byte{0x08},
		Header: &protocol.BlockHeader{
			Height:   8,
			Previous: []byte{0},
		},
	}

	err = applicator.ApplyBlock(ctx, futureBlock)
	if err != p2perrors.ErrMaxHeight {
		t.Errorf("block2b - ErrBlockApplication expected but not returned, was: %v", err)
	}

	for _, ch := range testChans {
		<-ch
	}
	<-testChan
}