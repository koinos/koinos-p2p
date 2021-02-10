package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
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

//"-a amqp://guest:guest@localhost:5672 -listen /ip4/127.0.0.1/tcp/8889 -peer /ip4/127.0.0.1/tcp/8888"
func main() {
	var addr = flag.String("listen", "/ip4/127.0.0.1/tcp/8889", "The multiaddress on which the node will listen")
	var seed = flag.Int("seed", 0, "Random seed with which the node will generate an ID")
	var peer = flag.String("peer", "/ip4/127.0.0.1/tcp/8888/p2p/Qmeq45rCLjFt573aFKgLrcAmAMSmYy9WXTuetDsELM2r8m", "Address of a peer to which to connect")
	var amqpFlag = flag.String("a", "amqp://guest:guest@localhost:5673/", "AMQP server URL")
	var sync = flag.String("sync", "true", "Is this a sync node")

	flag.Parse()

	var addrStr, peerStr, amqpFlagStr string
	var seedInt int

	if *sync == "true" {
		addrStr = "/ip4/127.0.0.1/tcp/8889"
		addr = &addrStr
		peerStr = "/ip4/127.0.0.1/tcp/8888/p2p/QmexAnfpHrhMmAC5UNQVS8iBuUUgDrMbMY17Cck2gKrqeX"
		peer = &peerStr
		amqpFlagStr = "amqp://guest:guest@localhost:5673/"
		amqpFlag = &amqpFlagStr
	} else {
		addrStr = "/ip4/127.0.0.1/tcp/8888"
		addr = &addrStr
		peerStr = ""
		peer = &peerStr
		seedInt = 1
		seed = &seedInt
		amqpFlagStr = "amqp://guest:guest@localhost:5672/"
		amqpFlag = &amqpFlagStr
	}

	mq := koinosmq.NewKoinosMQ(*amqpFlag)
	mq.Start()

	host, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(), int64(*seed))
	if err != nil {
		panic(err)
	}
	log.Printf("Starting node at address: %s\n", host.GetPeerAddress())

	// Connect to a peer
	if *peer != "" {
		log.Println("Connecting to peer and sending broadcast")
		_, err := host.ConnectToPeer(*peer)
		if err != nil {
			panic(err)
		}

		//if *sync {
		errs := make(chan error, 1)
		host.Protocols.Sync.InitiateProtocol(context.Background(), peer.ID, errs)
		err = getChannelError(errs)
		if err != nil {
			panic(err)
		}
		//} else {
		//	go host.Protocols.Broadcast.InitiateProtocol(context.Background(), peer.ID)
		//}
	}

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	host.Close()
}
