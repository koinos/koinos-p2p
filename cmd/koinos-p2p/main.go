package main

import (
	"context"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	flag "github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
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
	verboseOption      = "verbose"
)

const (
	baseDirDefault      = ".koinos"
	amqpDefault         = "amqp://guest.guest@localhost:5672/"
	listenDefault       = "/ip4/127.0.0.1/tcp/8888"
	seedDefault         = ""
	peerExchangeDefault = true
	bootstrapDefault    = false
	gossipDefault       = true
	forceGossipDefault  = false
	verboseDefault      = false
)

const (
	amqpConnectAttemptSeconds = 3
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	var baseDir = flag.StringP(baseDirOption, "d", baseDirDefault, "Koinos base directory")
	var amqp = flag.StringP(amqpOption, "a", "", "AMQP server URL")
	var addr = flag.StringP(listenOption, "l", "", "The multiaddress on which the node will listen")
	var seed = flag.StringP(seedOption, "s", "", "Seed string with which the node will generate an ID (A randomized seed will be generated if none is provided)")
	var peerAddresses = flag.StringSliceP(peerOption, "p", []string{}, "Address of a peer to which to connect (may specify multiple)")
	var directAddresses = flag.StringSliceP(directOption, "D", []string{}, "Address of a peer to connect using gossipsub.WithDirectPeers (may specify multiple) (should be reciprocal)")
	var peerExchange = flag.BoolP(peerExchangeOption, "x", true, "Exchange peers with other nodes")
	var bootstrap = flag.BoolP(bootstrapOption, "b", false, "Function as bootstrap node (always PRUNE, see libp2p gossip pex docs)")
	var gossip = flag.BoolP(gossipOption, "g", true, "Enable gossip mode")
	var forceGossip = flag.BoolP(forceGossipOption, "G", false, "Force gossip mode")
	var verbose = flag.BoolP(verboseOption, "v", false, "Enable verbose debug messages")

	flag.Parse()

	if !filepath.IsAbs(*baseDir) {
		homedir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		*baseDir = filepath.Join(homedir, *baseDir)
	}

	ensureDir(*baseDir)

	yamlConfigPath := filepath.Join(*baseDir, "config.yml")
	if _, err := os.Stat(yamlConfigPath); os.IsNotExist(err) {
		yamlConfigPath = filepath.Join(*baseDir, "config.yaml")
	}

	yamlConfig := yamlConfig{}
	if _, err := os.Stat(yamlConfigPath); err == nil {
		data, err := ioutil.ReadFile(yamlConfigPath)
		if err != nil {
			panic(err)
		}

		err = yaml.Unmarshal(data, &yamlConfig)
		if err != nil {
			panic(err)
		}
	} else {
		yamlConfig.Global = make(map[string]interface{})
		yamlConfig.P2P = make(map[string]interface{})
	}

	*amqp = getStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.P2P, yamlConfig.Global)
	*addr = getStringOption(listenOption, listenDefault, *addr, yamlConfig.P2P)
	*seed = getStringOption(seedOption, seedDefault, *seed, yamlConfig.P2P)
	*peerAddresses = getStringSliceOption(peerOption, *peerAddresses, yamlConfig.P2P)
	*directAddresses = getStringSliceOption(directOption, *directAddresses, yamlConfig.P2P)

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

	log.Println("Attempting to connect to block_store...")
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

type yamlConfig struct {
	Global map[string]interface{} `yaml:"global,omitempty"`
	P2P    map[string]interface{} `yaml:"p2p,omitempty"`
}

func getStringOption(key string, defaultValue string, cliArg string, configs ...map[string]interface{}) string {
	if cliArg != "" {
		return cliArg
	}

	for _, config := range configs {
		if v, ok := config[key]; ok {
			if option, ok := v.(string); ok {
				return option
			}
		}
	}

	return defaultValue
}

func getStringSliceOption(key string, cliArg []string, configs ...map[string]interface{}) []string {
	stringSlice := cliArg

	for _, config := range configs {
		if v, ok := config[key]; ok {
			if slice, ok := v.([]interface{}); ok {
				for _, option := range slice {
					if str, ok := option.(string); ok {
						stringSlice = append(stringSlice, str)
					}
				}
			}
		}
	}

	return stringSlice
}

func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
}
