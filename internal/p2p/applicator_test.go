package p2p

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/v2/koinos"
	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/v2/koinos/chain"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	chainrpc "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/chain"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"
)

type applicatorTestRPC struct {
	blocksToFail     map[string]void
	unlinkableBlocks map[string]void
	head             []byte
	invalidNonceTrxs map[string]void
}

func (b *applicatorTestRPC) GetHeadBlock(ctx context.Context) (*chainrpc.GetHeadInfoResponse, error) {
	return &chainrpc.GetHeadInfoResponse{
		HeadTopology: &koinos.BlockTopology{
			Id:       []byte{0},
			Height:   0,
			Previous: []byte{},
		},
	}, nil
}

func (b *applicatorTestRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chainrpc.SubmitBlockResponse, error) {
	if _, ok := b.blocksToFail[string(block.Id)]; ok {
		return nil, p2perrors.ErrBlockApplication
	}

	if _, ok := b.unlinkableBlocks[string(block.Id)]; ok {
		return nil, p2perrors.ErrUnknownPreviousBlock
	}

	return &chainrpc.SubmitBlockResponse{}, nil
}

func (b *applicatorTestRPC) ApplyTransaction(ctx context.Context, trx *protocol.Transaction) (*chainrpc.SubmitTransactionResponse, error) {
	if _, ok := b.invalidNonceTrxs[string(trx.Id)]; ok {
		return nil, p2perrors.ErrInvalidNonce
	}

	return &chainrpc.SubmitTransactionResponse{}, nil
}

func (b *applicatorTestRPC) GetBlocksByHeight(ctx context.Context, blockIDs multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	return &block_store.GetBlocksByHeightResponse{}, nil
}

func (b *applicatorTestRPC) GetChainID(ctx context.Context) (*chainrpc.GetChainIdResponse, error) {
	return &chainrpc.GetChainIdResponse{}, nil
}

func (b *applicatorTestRPC) GetForkHeads(ctx context.Context) (*chainrpc.GetForkHeadsResponse, error) {
	return &chainrpc.GetForkHeadsResponse{}, nil
}

func (b *applicatorTestRPC) GetBlocksByID(ctx context.Context, blockIDs []multihash.Multihash) (*block_store.GetBlocksByIdResponse, error) {
	return &block_store.GetBlocksByIdResponse{}, nil
}

func (b *applicatorTestRPC) BroadcastGossipStatus(ctx context.Context, enabled bool) error {
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
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
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

func TestReset(t *testing.T) {
	ctx := context.Background()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	applicator.Start(ctx)

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

	testChan1 := make(chan struct{})
	testChan2 := make(chan struct{})
	testChan3 := make(chan struct{})
	testChan4 := make(chan struct{})
	testChan5 := make(chan struct{})

	rpc.unlinkableBlocks[string(block1.Id)] = void{}

	go func() {
		err := applicator.ApplyBlock(ctx, block1)
		if err != p2perrors.ErrBlockApplicationTimeout {
			t.Errorf("block1 - ErrBlockApplicationTimeout expected but not returned, was: %v", err)
		}
		testChan1 <- struct{}{}
	}()

	go func() {
		err := applicator.ApplyBlock(ctx, block2a)
		if err != p2perrors.ErrBlockApplicationTimeout {
			t.Errorf("block2a - ErrBlockApplicationTimeout expected but not returned, was: %v", err)
		}
		testChan2 <- struct{}{}
	}()

	go func() {
		err := applicator.ApplyBlock(ctx, block2b)
		if err != p2perrors.ErrBlockApplicationTimeout {
			t.Errorf("block2b - ErrBlockApplicationTimeout expected but not returned, was: %v", err)
		}
		testChan3 <- struct{}{}
	}()

	go func() {
		err := applicator.ApplyBlock(ctx, block3a)
		if err != p2perrors.ErrBlockApplicationTimeout {
			t.Errorf("block3a - ErrBlockApplicationTimeout expected but not returned, was: %v", err)
		}
		testChan4 <- struct{}{}
	}()

	go func() {
		err := applicator.ApplyBlock(ctx, block3b)
		if err != p2perrors.ErrBlockApplicationTimeout {
			t.Errorf("block3b - ErrBlockApplicationTimeout expected but not returned, was: %v", err)
		}
		testChan5 <- struct{}{}
	}()

	time.Sleep(time.Second)

	// Lengths are sufficient for this test as other tests check correctness by behavior
	if len(applicator.blocksById) != 5 {
		t.Errorf("Expected 5 blocks by ID, was: %v", len(applicator.blocksById))
	}

	if len(applicator.blocksByPrevious) != 4 {
		t.Errorf("Expected 3 entries for blocks by previous, was: %v", len(applicator.blocksByPrevious))
	}

	if len(applicator.blocksByHeight) != 3 {
		t.Errorf("Expected 3 entries for blocks by height, was: %v", len(applicator.blocksByHeight))
	}

	if len(applicator.pendingBlocks) != 0 {
		t.Errorf("Expected 0 entries in pending blocks, was: %v", len(applicator.pendingBlocks))
	}

	applicator.Reset(ctx)

	time.Sleep(time.Second)

	if len(applicator.blocksById) != 0 {
		t.Errorf("Expected 0 blocks by ID, was: %v", len(applicator.blocksById))
	}

	if len(applicator.blocksByPrevious) != 0 {
		t.Errorf("Expected 0 entries for blocks by previous, was: %v", len(applicator.blocksByPrevious))
	}

	if len(applicator.blocksByHeight) != 0 {
		t.Errorf("Expected 0 entries for blocks by height, was: %v", len(applicator.blocksByHeight))
	}

	if len(applicator.pendingBlocks) != 0 {
		t.Errorf("Expected 0 entries in pending blocks, was: %v", len(applicator.pendingBlocks))
	}

	<-testChan1
	<-testChan2
	<-testChan3
	<-testChan4
	<-testChan5
}

func TestApplicatorLimits(t *testing.T) {
	ctx := context.Background()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(10*time.Minute), options.ApplicatorOptions{MaxPendingBlocks: 5, MaxHeightDelta: 5})
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

func TestDelayBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	block := &protocol.Block{
		Id: []byte{0x01},
		Header: &protocol.BlockHeader{
			Height:    1,
			Previous:  []byte{0},
			Timestamp: uint64(time.Now().Add(6 * time.Second).UnixMilli()),
		},
	}

	applicator.Start(ctx)

	timer, timerCancel := context.WithTimeout(ctx, 6*time.Second)
	defer timerCancel()

	go func() {
		select {
		case <-timer.Done():
			t.Error("block not applied in time")
		case <-ctx.Done():
		}
	}()

	err = applicator.ApplyBlock(ctx, block)

	if err != nil {
		t.Error(err)
	}

	cancel()
}

func TestInvalidNonce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}
	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	nonce := &chain.ValueType{
		Kind: &chain.ValueType_Uint64Value{
			Uint64Value: 0,
		},
	}
	nonceBytes, err := proto.Marshal(nonce)
	if err != nil {
		t.Error(err)
	}

	goodTrx := &protocol.Transaction{
		Id: []byte{0},
		Header: &protocol.TransactionHeader{
			Payer: []byte{0},
			Nonce: nonceBytes,
		},
	}

	badTrx := &protocol.Transaction{
		Id: []byte{1},
		Header: &protocol.TransactionHeader{
			Payer: []byte{0},
			Nonce: nonceBytes,
		},
	}

	rpc.invalidNonceTrxs[string(badTrx.Id)] = void{}

	applicator.Start(ctx)

	err = applicator.ApplyTransaction(ctx, goodTrx)
	if err != nil {
		t.Error(err)
	}

	trx1 := &protocol.Transaction{
		Id: []byte{0},
		Header: &protocol.TransactionHeader{
			Payer: []byte{0},
			Nonce: nonceBytes,
		},
	}

	nonce.Kind = &chain.ValueType_Uint64Value{
		Uint64Value: 1,
	}
	nonceBytes, err = proto.Marshal(nonce)
	if err != nil {
		t.Error(err)
	}

	trx2 := &protocol.Transaction{
		Id: []byte{1},
		Header: &protocol.TransactionHeader{
			Payer: []byte{0},
			Nonce: nonceBytes,
		},
	}

	rpc.invalidNonceTrxs[string(trx2.Id)] = void{}

	applicator.Start(ctx)
	testChan1 := make(chan struct{})
	defer close(testChan1)

	go func() {
		err := applicator.ApplyTransaction(ctx, trx2)
		fmt.Printf("%v - Trx2 Err: %v\n", time.Now(), err)
		if err != nil {
			t.Error(err)
		}
		testChan1 <- struct{}{}
	}()

	time.Sleep(100 * time.Millisecond)

	delete(rpc.invalidNonceTrxs, string(trx2.Id))

	err = applicator.ApplyTransaction(ctx, trx1)
	if err != nil {
		t.Error(err)
	}

	trxBroadcast := &broadcast.TransactionAccepted{
		Transaction: trx1,
	}

	applicator.HandleTransactionBroadcast(trxBroadcast)

	<-testChan1
}

func TestValidateBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	block := &protocol.Block{
		Id: []byte{0x01},
		Header: &protocol.BlockHeader{
			Height:   1,
			Previous: []byte{0},
		},
	}

	err = applicator.validateBlock(block)
	if err != nil {
		t.Error(err)
	}

	block.Header.Previous = nil
	err = applicator.validateBlock(block)
	if err == nil {
		t.Error("validateBlock should fail with a nil previous block")
	} else if !errors.Is(err, p2perrors.ErrInvalidBlock) {
		t.Errorf("expected validateBlock to return ErrInvalidBlock, was: %e", err)
	}

	block.Header.Previous = []byte{0}
	block.Id = nil
	if err == nil {
		t.Error("validateBlock should fail with a nil block id")
	} else if !errors.Is(err, p2perrors.ErrInvalidBlock) {
		t.Errorf("expected validateBlock to return ErrInvalidBlock, was: %e", err)
	}

	block.Id = []byte{0x01}
	block.Header = nil
	if err == nil {
		t.Error("validateBlock should fail with a nil header")
	} else if !errors.Is(err, p2perrors.ErrInvalidBlock) {
		t.Errorf("expected validateBlock to return ErrInvalidBlock, was: %e", err)
	}
}

func TestValidateTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rpc := applicatorTestRPC{
		blocksToFail:     make(map[string]void),
		unlinkableBlocks: make(map[string]void),
		head:             []byte{0x00},
		invalidNonceTrxs: make(map[string]void),
	}

	applicator, err := NewApplicator(ctx, &rpc, NewTransactionCache(time.Minute), *options.NewApplicatorOptions())
	if err != nil {
		t.Error(err)
	}

	trx := &protocol.Transaction{
		Id: []byte{0},
		Header: &protocol.TransactionHeader{
			Payer: []byte{0},
			Nonce: []byte{0},
		},
	}

	err = applicator.validateTransaction(trx)
	if err != nil {
		t.Error(err)
	}

	trx.Id = nil
	err = applicator.validateTransaction(trx)
	if err == nil {
		t.Error("validateTransaction should fail with a nil transaction id")
	} else if !errors.Is(err, p2perrors.ErrInvalidTransaction) {
		t.Errorf("expected validateTransaction to return ErrInvalidTransaction, was: %e", err)
	}

	trx.Id = []byte{0}
	trx.Header.Payer = nil
	err = applicator.validateTransaction(trx)
	if err == nil {
		t.Error("validateTransaction should fail with a nil transaction id")
	} else if !errors.Is(err, p2perrors.ErrInvalidTransaction) {
		t.Errorf("expected validateTransaction to return ErrInvalidTransaction, was: %e", err)
	}

	trx.Header.Payer = []byte{0}
	trx.Header.Nonce = nil
	err = applicator.validateTransaction(trx)
	if err == nil {
		t.Error("validateTransaction should fail with a nil transaction nonce")
	} else if !errors.Is(err, p2perrors.ErrInvalidTransaction) {
		t.Errorf("expected validateTransaction to return ErrInvalidTransaction, was: %e", err)
	}

	trx.Header = nil
	err = applicator.validateTransaction(trx)
	if err == nil {
		t.Error("validateTransaction should fail with a nil transaction header")
	} else if !errors.Is(err, p2perrors.ErrInvalidTransaction) {
		t.Errorf("expected validateTransaction to return ErrInvalidTransaction, was: %e", err)
	}
}
