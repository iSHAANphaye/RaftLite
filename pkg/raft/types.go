package raft

import (
	"time"

	"github.com/raftlite/pkg/kv"
)

// NodeRole represents the current consensus role of a Raft node.
type NodeRole string

const (
	RoleFollower  NodeRole = "FOLLOWER"
	RoleCandidate NodeRole = "CANDIDATE"
	RoleLeader    NodeRole = "LEADER"
)

// Config defines the runtime configuration for a Raft node.
type Config struct {
	NodeID             string
	Peers              map[string]string // PeerID -> Address (e.g. "node-2" -> "127.0.0.1:9002")
	StorageDir         string
	SyncOnWrite        bool
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	ApplyCh            chan kv.ApplyMsg
}

// DefaultConfig returns reasonable defaults for production operation.
func DefaultConfig(nodeID string, peers map[string]string, storageDir string) Config {
	return Config{
		NodeID:             nodeID,
		Peers:              peers,
		StorageDir:         storageDir,
		SyncOnWrite:        true,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		ApplyCh:            make(chan kv.ApplyMsg, 1000),
	}
}
