package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-p2p/internal/node"
	"github.com/koinos/koinos-p2p/internal/options"
	"github.com/koinos/koinos-p2p/internal/rpc"
	"github.com/koinos/koinos-p2p/internal/util"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
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
	logLevelOption     = "log-level"
	instanceIDOption   = "instance-id"
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

// Log consts
const (
	maxSize         = 128
	maxBackups      = 32
	maxAge          = 64
	compressBackups = true
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

	*baseDir = initBaseDir(*baseDir)
	ensureDir(*baseDir)
	yamlConfig := initYamlConfig(*baseDir)

	// Generate Instance ID
	if *instanceID == "" {
		*instanceID = util.GenerateBase58ID(5)
	}

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(getAppDir(*baseDir, appName), logDir, "p2p.log")
	level, err := stringToLogLevel(*logLevel)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel))
	}
	initLogger(level, logFilename, appID)

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

	client.Start()

	koinosRPC := rpc.NewKoinosRPC(client)

	zap.L().Info("Attempting to connect to block_store...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToBlockStore(ctx)
		if val {
			zap.L().Info("Connected")
			break
		}
	}

	zap.L().Info("Attempting to connect to chain...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpConnectAttemptSeconds*time.Second)
		defer cancel()
		val, _ := koinosRPC.IsConnectedToChain(ctx)
		if val {
			zap.L().Info("Connected")
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

	zap.S().Info("Starting node at address: %s", node.GetPeerAddress())

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	zap.L().Info("Shutting down node...")
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

func stringToLogLevel(level string) (zapcore.Level, error) {
	switch level {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.InfoLevel, errors.New("")
	}
}

func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
}

func getAppDir(baseDir string, appName string) string {
	return path.Join(baseDir, appName)
}

func initBaseDir(baseDir string) string {
	if !filepath.IsAbs(baseDir) {
		homedir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		baseDir = filepath.Join(homedir, baseDir)
	}
	ensureDir(baseDir)

	return baseDir
}

func initLogger(level zapcore.Level, logFilename string, appID string) {
	// Construct production encoder config, set time format
	e := zap.NewDevelopmentEncoderConfig()
	e.EncodeTime = util.KoinosTimeEncoder
	e.EncodeLevel = util.KoinosColorLevelEncoder

	// Construct JSON encoder for file output
	fileEncoder := zapcore.NewJSONEncoder(e)

	// Construct Console encoder for console output
	consoleEncoder := util.NewKoinosEncoder(e, appID)

	// Construct lumberjack log roller
	lj := &lumberjack.Logger{
		Filename:   logFilename,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		Compress:   compressBackups,
	}

	// Construct core
	coreFunc := zap.WrapCore(func(zapcore.Core) zapcore.Core {
		return zapcore.NewTee(
			zapcore.NewCore(fileEncoder, zapcore.AddSync(lj), level),
			zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
		)
	})

	// Construct logger
	logger, err := zap.NewProduction(coreFunc)
	if err != nil {
		panic(fmt.Sprintf("Error constructing logger: %v", err))
	}

	// Set global logger
	zap.ReplaceGlobals(logger)
}

func initYamlConfig(baseDir string) *yamlConfig {
	yamlConfigPath := filepath.Join(baseDir, "config.yml")
	if _, err := os.Stat(yamlConfigPath); os.IsNotExist(err) {
		yamlConfigPath = filepath.Join(baseDir, "config.yaml")
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

	return &yamlConfig
}
