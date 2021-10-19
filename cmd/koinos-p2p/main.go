package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	libp2plog "github.com/ipfs/go-log"

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
	checkpointOption   = "checkpoint"
	peerExchangeOption = "pex"
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

	// Set libp2p log level
	libp2plog.SetAllLoggers(libp2plog.LevelFatal)

	baseDir := flag.StringP(baseDirOption, "d", baseDirDefault, "Koinos base directory")
	amqp := flag.StringP(amqpOption, "a", "", "AMQP server URL")
	addr := flag.StringP(listenOption, "l", "", "The multiaddress on which the node will listen")
	seed := flag.StringP(seedOption, "s", "", "Seed string with which the node will generate an ID (A randomized seed will be generated if none is provided)")
	peerAddresses := flag.StringSliceP(peerOption, "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	directAddresses := flag.StringSliceP(directOption, "D", []string{}, "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	checkpoints := flag.StringSliceP(checkpointOption, "c", []string{}, "Block checkpoint in the form height:blockid (may specify multiple times)")
	peerExchange := flag.BoolP(peerExchangeOption, "x", peerExchangeDefault, "Exchange peers with other nodes")
	gossip := flag.BoolP(gossipOption, "g", gossipDefault, "Enable gossip mode")
	forceGossip := flag.BoolP(forceGossipOption, "G", forceGossipDefault, "Force gossip mode")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this node")

	flag.Parse()

	*baseDir = util.InitBaseDir(*baseDir)
	util.EnsureDir(*baseDir)
	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.P2P, yamlConfig.Global)
	*addr = util.GetStringOption(listenOption, listenDefault, *addr, yamlConfig.P2P, yamlConfig.Global)
	*seed = util.GetStringOption(seedOption, seedDefault, *seed, yamlConfig.P2P, yamlConfig.Global)
	*peerAddresses = util.GetStringSliceOption(peerOption, *peerAddresses, yamlConfig.P2P, yamlConfig.Global)
	*directAddresses = util.GetStringSliceOption(directOption, *directAddresses, yamlConfig.P2P, yamlConfig.Global)
	*checkpoints = util.GetStringSliceOption(checkpointOption, *checkpoints, yamlConfig.P2P, yamlConfig.Global)
	*peerExchange = util.GetBoolOption(peerExchangeOption, peerExchangeDefault, *peerExchange, yamlConfig.P2P, yamlConfig.Global)
	*gossip = util.GetBoolOption(gossipOption, *gossip, gossipDefault, yamlConfig.P2P, yamlConfig.Global, yamlConfig.Global)
	*forceGossip = util.GetBoolOption(forceGossipOption, *forceGossip, forceGossipDefault, yamlConfig.P2P, yamlConfig.Global)
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

	config.NodeOptions.InitialPeers = *peerAddresses
	config.NodeOptions.DirectPeers = *directAddresses

	if !(*gossip) {
		config.GossipToggleOptions.AlwaysDisable = true
	}
	if *forceGossip {
		config.GossipToggleOptions.AlwaysEnable = true
	}

	for _, checkpoint := range *checkpoints {
		parts := strings.SplitN(checkpoint, ":", 2)
		if len(parts) != 2 {
			log.Errorf("Checkpoint option must be in form blockHeight:blockID, was '%s'", checkpoint)
		}
		blockHeight, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			log.Errorf("Could not parse checkpoint block height '%s': %s", parts[0], err.Error())
		}

		// Replace with base64 later
		//blockID, err := base64.URLEncoding.DecodeString(parts[1])
		blockID, err := hex.DecodeString(parts[1])
		config.PeerConnectionOptions.Checkpoints = append(config.PeerConnectionOptions.Checkpoints, options.Checkpoint{BlockHeight: blockHeight, BlockID: blockID})
	}

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
