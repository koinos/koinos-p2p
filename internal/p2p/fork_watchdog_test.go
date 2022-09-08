package p2p

import (
	"testing"

	"github.com/koinos/koinos-p2p/internal/p2perrors"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
)

func TestForkWatchdog(t *testing.T) {
	fw := NewForkWatchdog()

	block := &protocol.Block{
		Id: []byte{0x01, 0x02, 0x03, 0x04},
		Header: &protocol.BlockHeader{
			Signer:   []byte{0x01, 0x01, 0x01},
			Height:   1,
			Previous: []byte{0x01},
		},
	}

	err := fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x05}

	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x06}

	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x07}

	err = fw.Add(block)
	if err != p2perrors.ErrForkBomb {
		t.Error("should have detected fork bomb")
	}

	fw.Purge(block.Header.Height)

	block.Id = []byte{0x01, 0x02, 0x03, 0x04}

	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x05}

	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x06}

	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Id = []byte{0x01, 0x02, 0x03, 0x07}

	block.Header.Previous = []byte{0x02}

	// Different parent block should not trigger fork bomb
	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	block.Header.Previous = []byte{0x01}

	err = fw.Add(block)
	if err != p2perrors.ErrForkBomb {
		t.Error("should have detected fork bomb")
	}

	block.Header.Height = 2
	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}

	fw.Purge(2)
	block.Header.Height = 1
	err = fw.Add(block)
	if err != nil {
		t.Error(err)
	}
}
