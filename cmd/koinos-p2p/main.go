package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/koinos/koinos-p2p/internal/p2p"
)

func main() {
	var addr = flag.String("listen", "/ip4/127.0.0.1/tcp/8888", "The multiaddress on which the node will listen")
	var seed = flag.Int("seed", 0, "Random seed with which the node will generate an ID")
	var peer = flag.String("peer", "", "Address of a peer to which to connect")

	flag.Parse()

	host, _ := p2p.NewKoinosP2PNode(*addr, int64(*seed))
	log.Printf("Starting node at with address: %s\n", host.GetPeerAddress())

	bp := p2p.NewBroadcastProtocol(host)

	// Connect to a peer
	if *peer != "" {
		fmt.Println("Connecting to peer and sending broadcast")
		peer, err := host.ConnectToPeer(*peer)
		if err != nil {
			panic(err)
		}

		ctx, cancel := host.MakeContext()
		defer cancel()

		bp.InitiateProtocol(ctx, host, peer.ID)
	}

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
	// Shut the node down
	host.Close()
}
