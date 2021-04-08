package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	flag "github.com/spf13/pflag"
)

const (
	amqpConnectAttemptSeconds = 3
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	var addr = flag.StringP("listen", "l", "/ip4/127.0.0.1/tcp/8888", "The multiaddress on which the node will listen")
	var seed = flag.StringP("seed", "s", "", "Seed string with which the node will generate an ID (A randomized seed will be generated if none is provided)")
	var amqp = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")
	var peerAddresses = flag.StringSliceP("peer", "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	var directAddresses = flag.StringSliceP("direct", "d", []string{}, "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	var peerExchange = flag.BoolP("pex", "x", true, "Exchange peers with other nodes")
	var bootstrap = flag.BoolP("bootstrap", "b", false, "Function as bootstrap node (always PRUNE, see libp2p gossip pex docs)")
	var gossip = flag.BoolP("gossip", "g", true, "Enable gossip mode")
	var forceGossip = flag.BoolP("force-gossip", "G", false, "Force gossip mode")
	var verbose = flag.BoolP("verbose", "v", false, "Enable verbose debug messages")

	flag.Parse()

	client := koinosmq.NewClient(*amqp)
	requestHandler := koinosmq.NewRequestHandler(*amqp)

	config := options.NewConfig()

	config.NodeOptions.EnablePeerExchange = *peerExchange
	config.NodeOptions.EnableBootstrap = *bootstrap
	config.NodeOptions.EnableGossip = *gossip
	config.NodeOptions.ForceGossip = *forceGossip

	config.NodeOptions.InitialPeers = *peerAddresses
	config.NodeOptions.DirectPeers = *directAddresses

	config.SetEnableDebugMessages(*verbose)

	client.Start()

	koinosRPC := rpc.NewKoinosRPC(client)

	log.Println("Attemtping to connect to block_store...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToBlockStore(ctx)
		if val {
			log.Println("Connected")
			break
		}
	}

	log.Println("Attempting to connect to chain...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToChain(ctx)
		if val {
			log.Println("Connected")
			break
		}
	}

	node, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(client), requestHandler, *seed, config)
	if err != nil {
		panic(err)
	}

	requestHandler.Start()

	err = node.Start(context.Background())
	if err != nil {
		panic(err)
	}

	log.Printf("Starting node at address: %s\n", node.GetPeerAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	node.Close()
}
