package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	flag "github.com/spf13/pflag"
)

func main() {
	var addr = flag.StringP("listen", "l", "/ip4/127.0.0.1/tcp/8888", "The multiaddress on which the node will listen")
	var seed = flag.IntP("seed", "s", 0, "Random seed with which the node will generate an ID")
	var amqpFlag = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")
	var peerFlags = flag.StringSliceP("peer", "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	var directFlags = flag.StringSliceP("direct", "d", []string{}, "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	var pexFlag = flag.BoolP("pex", "x", true, "Exchange peers with other nodes")
	var bootstrapFlag = flag.BoolP("bootstrap", "b", false, "Function as bootstrap node (always PRUNE, see libp2p gossip pex docs)")
	var gossipFlag = flag.BoolP("gossip", "g", true, "Enable gossip mode")
	var forceGossipFlag = flag.BoolP("force-gossip", "G", false, "Force gossip mode")
	var verboseFlag = flag.BoolP("verbose", "v", false, "Enable verbose debug messages")

	flag.Parse()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)

	config := options.NewConfig()

	config.NodeOptions.EnablePeerExchange = *pexFlag
	config.NodeOptions.EnableBootstrap = *bootstrapFlag
	config.NodeOptions.EnableGossip = *gossipFlag
	config.NodeOptions.EnableDebugMessages = *verboseFlag
	config.NodeOptions.ForceGossip = *forceGossipFlag

	config.NodeOptions.InitialPeers = *peerFlags
	config.NodeOptions.DirectPeers = *directFlags

	node, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(mq), int64(*seed), config)
	if err != nil {
		panic(err)
	}
	err = node.Start(context.Background())
	if err != nil {
		panic(err)
	}

	mq.Start()
	log.Printf("Starting node at address: %s\n", node.GetPeerAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	node.Close()
}
