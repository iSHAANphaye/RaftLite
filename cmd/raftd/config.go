package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// DaemonConfig holds all configuration settings for raftd.
type DaemonConfig struct {
	NodeID          string
	ListenAddr      string
	ClientAddr      string
	Peers           map[string]string // NodeID -> Address
	DataDir         string
	ElectionMinMs   int
	ElectionMaxMs   int
	HeartbeatMs     int
	Sync            bool
	ClusterToken    string
	LogLevel        string
}

// ParseConfig reads configuration from command-line flags and environment variables.
func ParseConfig() (*DaemonConfig, error) {
	fs := flag.NewFlagSet("raftd", flag.ContinueOnError)

	var (
		flagNodeID        = fs.String("node-id", envOrDefault("RAFTLITE_NODE_ID", ""), "Unique identifier for this node")
		flagListenAddr    = fs.String("listen-addr", envOrDefault("RAFTLITE_LISTEN_ADDR", "0.0.0.0:9091"), "Internal consensus gRPC bind address")
		flagClientAddr    = fs.String("client-addr", envOrDefault("RAFTLITE_CLIENT_ADDR", "0.0.0.0:8081"), "External client KV gRPC bind address")
		flagPeers         = fs.String("peers", envOrDefault("RAFTLITE_PEERS", ""), "Comma-separated peer list: node2=host2:port,node3=host3:port")
		flagDataDir       = fs.String("data-dir", envOrDefault("RAFTLITE_DATA_DIR", "./data"), "Directory for WAL and state files")
		flagElectionMinMs = fs.Int("election-min-ms", envIntOrDefault("RAFTLITE_ELECTION_MIN_MS", 150), "Minimum election timeout in ms")
		flagElectionMaxMs = fs.Int("election-max-ms", envIntOrDefault("RAFTLITE_ELECTION_MAX_MS", 300), "Maximum election timeout in ms")
		flagHeartbeatMs   = fs.Int("heartbeat-ms", envIntOrDefault("RAFTLITE_HEARTBEAT_MS", 50), "Leader heartbeat interval in ms")
		flagSync          = fs.Bool("sync", envBoolOrDefault("RAFTLITE_SYNC", true), "Enforce synchronous fsync on writes")
		flagClusterToken  = fs.String("cluster-token", envOrDefault("RAFTLITE_CLUSTER_TOKEN", ""), "Shared cluster authorization token")
		flagLogLevel      = fs.String("log-level", envOrDefault("RAFTLITE_LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	)

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	if *flagNodeID == "" {
		return nil, fmt.Errorf("node-id is required (provide --node-id or RAFTLITE_NODE_ID)")
	}

	peersMap := make(map[string]string)
	if *flagPeers != "" {
		entries := strings.Split(*flagPeers, ",")
		for _, e := range entries {
			e = strings.TrimSpace(e)
			if e == "" {
				continue
			}
			parts := strings.SplitN(e, "=", 2)
			if len(parts) == 2 {
				peersMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	return &DaemonConfig{
		NodeID:        *flagNodeID,
		ListenAddr:    *flagListenAddr,
		ClientAddr:    *flagClientAddr,
		Peers:         peersMap,
		DataDir:       *flagDataDir,
		ElectionMinMs: *flagElectionMinMs,
		ElectionMaxMs: *flagElectionMaxMs,
		HeartbeatMs:   *flagHeartbeatMs,
		Sync:          *flagSync,
		ClusterToken:  *flagClusterToken,
		LogLevel:      *flagLogLevel,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	if val := os.Getenv(key); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return fallback
}

// ToRaftConfig converts DaemonConfig to consensus engine Config.
func (c *DaemonConfig) ToRaftConfig() time.Duration {
	return time.Duration(c.HeartbeatMs) * time.Millisecond
}
