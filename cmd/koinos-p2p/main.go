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

/**
 * appendFlag is a helper for the golang flag module (import "flag") that lets you specify a flag multiple times.
 *
 * Golint doesn't like it when we export appendFlag, so we don't export it.
 */
type appendFlag []string

func (a *appendFlag) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func (a *appendFlag) String() string {
	return strings.Join(*a, ",")
}

func main() {
	var peerFlags appendFlag
	var directFlags appendFlag
	var addr = flag.String("listen", "/ip4/127.0.0.1/tcp/8888", "The multiaddress on which the node will listen")
	var seed = flag.Int("seed", 0, "Random seed with which the node will generate an ID")
	flag.Var(&peerFlags, "peer", "Address of a peer to which to connect (may specify multiple)")
	flag.Var(&peerFlags, "p", "Address of a peer to which to connect (may specify multiple) (short)")
	flag.Var(&directFlags, "direct", "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	var pexFlag = flag.Bool("pex", true, "Exchange peers with other nodes")
	var amqpFlag = flag.String("a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	flag.Parse()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)
	mq.Start()

	opt := node.NewKoinosP2POptions()
	opt.EnablePeerExchange = *pexFlag
	opt.InitialPeers = peerFlags
	opt.DirectPeers = directFlags

	host, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(), int64(*seed), *opt)
	if err != nil {
		panic(err)
	}
	log.Printf("Starting node at address: %s\n", host.GetPeerAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	host.Close()
}
