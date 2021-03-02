package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/rpc"
	flag "github.com/spf13/pflag"
)

func getChannelError(errs chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func main() {
	var addr = flag.StringP("listen", "l", "/ip4/127.0.0.1/tcp/8888", "The multiaddress on which the node will listen")
	var seed = flag.IntP("seed", "s", 0, "Random seed with which the node will generate an ID")
	var peerFlags = flag.StringSliceP("peer", "p", []string{}, "")
	var amqpFlag = flag.StringP("ampq", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	flag.Parse()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)
	mq.Start()

	host, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(), int64(*seed))
	if err != nil {
		panic(err)
	}
	log.Printf("Starting node at address: %s\n", host.GetPeerAddress())

	// Connect to a peer
	for _, pid := range *peerFlags {
		if pid != "" {
			log.Printf("Connecting to peer %s and sending broadcast\n", pid)
			_, err := host.ConnectToPeer(pid)
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
