package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/rpc"
)

func getChannelError(errs chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

/**
 * AppendFlag is a helper for the golang flag module (import "flag") that lets you specify a flag multiple times.
 */
type AppendFlag []string

func (a *AppendFlag) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func (a *AppendFlag) String() string {
	return strings.Join(*a, ",")
}

func main() {
	var peerFlags AppendFlag
	var addr = flag.String("listen", "/ip4/127.0.0.1/tcp/8889", "The multiaddress on which the node will listen")
	var seed = flag.Int("seed", 0, "Random seed with which the node will generate an ID")
	flag.Var(&peerFlags, "peer", "Address of a peer to which to connect (may specify multiple)")
	flag.Var(&peerFlags, "p", "Address of a peer to which to connect (may specify multiple) (short)")
	var amqpFlag = flag.String("a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	flag.Parse()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)
	mq.Start()

	host, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(), int64(*seed))
	if err != nil {
		panic(err)
	}
	log.Printf("Starting node at address: %s\n", host.GetPeerAddress())

	// Connect to a peer
	for _, pid := range peerFlags {
		if pid != "" {
			log.Printf("Connecting to peer %s and sending broadcast\n", pid)
			peer, err := host.ConnectToPeer(pid)
			if err != nil {
				panic(err)
			}

			errs := make(chan error, 1)
			host.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID, errs)
			err = getChannelError(errs)
			if err != nil {
				panic(err)
			}
		}
	}

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	host.Close()
}
