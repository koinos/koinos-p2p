package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	libp2plog "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	util "github.com/koinos/koinos-util-golang"
	flag "github.com/spf13/pflag"
)

const (
	baseDirOption       = "basedir"
	amqpOption          = "amqp"
	listenOption        = "listen"
	seedOption          = "seed"
	peerOption          = "peer"
	checkpointOption    = "checkpoint"
	disableGossipOption = "disable-gossip"
	forceGossipOption   = "force-gossip"
	logLevelOption      = "log-level"
	instanceIDOption    = "instance-id"
	jobsOption          = "jobs"
	versionOption       = "version"
)

const (
	baseDirDefault       = ".koinos"
	amqpDefault          = "amqp://guest:guest@localhost:5672/"
	listenDefault        = "/ip4/127.0.0.1/tcp/8888"
	seedDefault          = ""
	disableGossipDefault = false
	forceGossipDefault   = false
	logLevelDefault      = "info"
	instanceIDDefault    = ""
)

const (
	appName = "p2p"
	logDir  = "logs"
)

// Version display values
const (
	DisplayAppName = "Koinos P2P"
	Version        = "v1.0.0"
)

// Gets filled in by the linker
var Commit string

func main() {
	// Set libp2p log level
	libp2plog.SetAllLoggers(libp2plog.LevelFatal)

	jobsDefault := runtime.NumCPU()

	var baseDir string

	baseDirPtr := flag.StringP(baseDirOption, "d", baseDirDefault, "Koinos base directory")
	amqp := flag.StringP(amqpOption, "a", "", "AMQP server URL")
	addr := flag.StringP(listenOption, "L", "", "The multiaddress on which the node will listen")
	seed := flag.StringP(seedOption, "s", "", "Seed string with which the node will generate an ID (A randomized seed will be generated if none is provided)")
	peerAddresses := flag.StringSliceP(peerOption, "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	checkpoints := flag.StringSliceP(checkpointOption, "c", []string{}, "Block checkpoint in the form height:blockid (may specify multiple times)")
	disableGossip := flag.BoolP(disableGossipOption, "g", disableGossipDefault, "Disable gossip mode")
	forceGossip := flag.BoolP(forceGossipOption, "G", forceGossipDefault, "Force gossip mode to always be enabled")
	logLevel := flag.StringP(logLevelOption, "l", "", "The log filtering level (debug, info, warn, error)")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this node")
	jobs := flag.IntP(jobsOption, "j", jobsDefault, "Number of RPC jobs to run")
	version := flag.BoolP(versionOption, "v", false, "Print version and exit")

	flag.Parse()

	if *version {
		fmt.Println(makeVersionString())
		os.Exit(0)
	}

	baseDir, err := util.InitBaseDir(*baseDirPtr)
	if err != nil {
		fmt.Printf("Could not initialize base directory '%v'\n", baseDir)
		os.Exit(1)
	}

	yamlConfig := util.InitYamlConfig(baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.P2P, yamlConfig.Global)
	*addr = util.GetStringOption(listenOption, listenDefault, *addr, yamlConfig.P2P, yamlConfig.Global)
	*seed = util.GetStringOption(seedOption, seedDefault, *seed, yamlConfig.P2P, yamlConfig.Global)
	*peerAddresses = util.GetStringSliceOption(peerOption, *peerAddresses, yamlConfig.P2P, yamlConfig.Global)
	*checkpoints = util.GetStringSliceOption(checkpointOption, *checkpoints, yamlConfig.P2P, yamlConfig.Global)
	*disableGossip = util.GetBoolOption(disableGossipOption, disableGossipDefault, *disableGossip, yamlConfig.P2P, yamlConfig.Global)
	*forceGossip = util.GetBoolOption(forceGossipOption, forceGossipDefault, *forceGossip, yamlConfig.P2P, yamlConfig.Global)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.P2P, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.P2P, yamlConfig.Global)
	*jobs = util.GetIntOption(jobsOption, jobsDefault, *jobs, yamlConfig.P2P, yamlConfig.Global)

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(util.GetAppDir(baseDir, appName), logDir, "p2p.log")
	err = log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		fmt.Printf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel)
		os.Exit(1)
	}

	log.Info(makeVersionString())

	if *jobs < 1 {
		log.Errorf("Option '%v' must be greater than 0 (was %v)", jobsOption, *jobs)
		os.Exit(1)
	}

	client := koinosmq.NewClient(*amqp, koinosmq.ExponentialBackoff)
	requestHandler := koinosmq.NewRequestHandler(*amqp, uint(*jobs), koinosmq.ExponentialBackoff)

	config := options.NewConfig()

	for _, peerStr := range *peerAddresses {
		ma, err := multiaddr.NewMultiaddr(peerStr)

		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		addr, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Warnf("Error parsing peer address: %v", err)
		}

		config.NodeOptions.InitialPeers = append(config.NodeOptions.InitialPeers, *addr)
	}

	if *disableGossip {
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
		if err != nil {
			log.Errorf("Error decoding checkpoint block id: %s", err)
		}
		config.PeerConnectionOptions.Checkpoints = append(config.PeerConnectionOptions.Checkpoints, options.Checkpoint{BlockHeight: blockHeight, BlockID: blockID})
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	<-client.Start(ctx)

	koinosRPC := rpc.NewKoinosRPC(client)

	log.Info("Attempting to connect to block_store...")
	for {
		blockStoreCtx, blockStoreCancel := context.WithCancel(ctx)
		defer blockStoreCancel()
		val, _ := koinosRPC.IsConnectedToBlockStore(blockStoreCtx)
		if val {
			log.Info("Connected")
			break
		}
	}

	log.Info("Attempting to connect to chain...")
	for {
		chainCtx, chainCancel := context.WithCancel(ctx)
		defer chainCancel()
		val, _ := koinosRPC.IsConnectedToChain(chainCtx)
		if val {
			log.Info("Connected")
			break
		}
	}

	node, err := node.NewKoinosP2PNode(ctx, *addr, rpc.NewKoinosRPC(client), requestHandler, *seed, config)
	if err != nil {
		panic(err)
	}

	<-requestHandler.Start(ctx)

	node.Start(ctx)

	log.Infof("Starting node at address: %s", node.GetAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
	// Shut the node down
	node.Close()
}

func makeVersionString() string {
	commitString := ""
	if len(Commit) >= 8 {
		commitString = fmt.Sprintf("(%s)", Commit[0:8])
	}

	return fmt.Sprintf("%s %s %s", DisplayAppName, Version, commitString)
}
