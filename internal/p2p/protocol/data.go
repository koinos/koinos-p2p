package protocol

import (
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/koinos/koinos-p2p/internal/p2p/rpc"
)

// Data
type Data struct {
	RPC  rpc.RPC
	Host host.Host
}

type Protocol interface {
	GetProtocolRegistration() (pid protocol.ID, handler network.StreamHandler)
}
