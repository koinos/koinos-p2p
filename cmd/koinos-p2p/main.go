package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	util "github.com/koinos/koinos-util-golang"
	flag "github.com/spf13/pflag"
)

const (
	baseDirOption      = "basedir"
	amqpOption         = "amqp"
	listenOption       = "listen"
	seedOption         = "seed"
	peerOption         = "peer"
	directOption       = "direct"
	peerExchangeOption = "pex"
	bootstrapOption    = "bootstrap"
	gossipOption       = "gossip"
	forceGossipOption  = "forceGossip"
	logLevelOption     = "log-level"
	instanceIDOption   = "instance-id"
)

const (
	baseDirDefault      = ".koinos"
	amqpDefault         = "amqp://guest:guest@localhost:5672/"
	listenDefault       = "/ip4/127.0.0.1/tcp/8888"
	seedDefault         = ""
	peerExchangeDefault = true
	bootstrapDefault    = false
	gossipDefault       = true
	forceGossipDefault  = false
	verboseDefault      = false
	logLevelDefault     = "info"
	instanceIDDefault   = ""
)

const (
	amqpConnectAttemptSeconds = 3
)

const (
	appName = "p2p"
	logDir  = "logs"
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	baseDir := flag.StringP(baseDirOption, "d", baseDirDefault, "Koinos base directory")
	amqp := flag.StringP(amqpOption, "a", "", "AMQP server URL")
	addr := flag.StringP(listenOption, "l", "", "The multiaddress on which the node will listen")
	seed := flag.StringP(seedOption, "s", "", "Seed string with which the node will generate an ID (A randomized seed will be generated if none is provided)")
	peerAddresses := flag.StringSliceP(peerOption, "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	directAddresses := flag.StringSliceP(directOption, "D", []string{}, "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	peerExchange := flag.BoolP(peerExchangeOption, "x", true, "Exchange peers with other nodes")
	bootstrap := flag.BoolP(bootstrapOption, "b", false, "Function as bootstrap node (always PRUNE, see libp2p gossip pex docs)")
	gossip := flag.BoolP(gossipOption, "g", true, "Enable gossip mode")
	forceGossip := flag.BoolP(forceGossipOption, "G", false, "Force gossip mode")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this node")

	flag.Parse()

	*baseDir = util.InitBaseDir(*baseDir)
	util.EnsureDir(*baseDir)
	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.P2P, yamlConfig.Global)
	*addr = util.GetStringOption(listenOption, listenDefault, *addr, yamlConfig.P2P)
	*seed = util.GetStringOption(seedOption, seedDefault, *seed, yamlConfig.P2P)
	*peerAddresses = util.GetStringSliceOption(peerOption, *peerAddresses, yamlConfig.P2P)
	*directAddresses = util.GetStringSliceOption(directOption, *directAddresses, yamlConfig.P2P)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.P2P, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.P2P, yamlConfig.Global)

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(util.GetAppDir(*baseDir, appName), logDir, "p2p.log")
	err := log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel))
	}

	client := koinosmq.NewClient(*amqp, koinosmq.ExponentialBackoff)
	requestHandler := koinosmq.NewRequestHandler(*amqp)

	config := options.NewConfig()

	config.NodeOptions.EnablePeerExchange = *peerExchange
	config.NodeOptions.EnableBootstrap = *bootstrap
	config.NodeOptions.EnableGossip = *gossip
	config.NodeOptions.ForceGossip = *forceGossip

	config.NodeOptions.InitialPeers = *peerAddresses
	config.NodeOptions.DirectPeers = *directAddresses

	client.Start()

	koinosRPC := rpc.NewKoinosRPC(client)

	log.Info("Attempting to connect to block_store...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToBlockStore(ctx)
		if val {
			log.Info("Connected")
			break
		}
		time.Sleep(amqpConnectAttemptSeconds * time.Second)
	}

	log.Info("Attempting to connect to chain...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToChain(ctx)
		if val {
			log.Info("Connected")
			break
		}
		time.Sleep(amqpConnectAttemptSeconds * time.Second)
	}

	node, err := node.NewKoinosP2PNode(context.Background(), *addr, rpc.NewKoinosRPC(client), requestHandler, *seed, config)
	if err != nil {
		panic(err)
	}

	requestHandler.Start()

	node.Start(context.Background())

	log.Infof("Starting node at address: %s", node.GetPeerAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
	// Shut the node down
	node.Close()
}
