package raft

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplication_BasicProposalAndCommit(t *testing.T) {
	tempBase := t.TempDir()
	net := NewMockNetwork()

	peerMap := map[string]string{
		"node-1": "127.0.0.1:9001",
		"node-2": "127.0.0.1:9002",
		"node-3": "127.0.0.1:9003",
	}

	nodes := make([]*RaftNode, 3)
	for i := 1; i <= 3; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		peers := make(map[string]string)
		for k, v := range peerMap {
			if k != nodeID {
				peers[k] = v
			}
		}

		cfg := DefaultConfig(nodeID, peers, filepath.Join(tempBase, nodeID))
		cfg.ElectionTimeoutMin = 40 * time.Millisecond
		cfg.ElectionTimeoutMax = 80 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond

		n, err := NewRaftNode(cfg, net)
		require.NoError(t, err)
		net.Register(n)
		nodes[i-1] = n
	}

	for _, n := range nodes {
		n.Start()
		defer n.Stop()
	}

	// 1. Wait for Leader election
	var leader *RaftNode
	assert.Eventually(t, func() bool {
		for _, n := range nodes {
			_, role, _, _, _, _ := n.GetStatus()
			if role == RoleLeader {
				leader = n
				return true
			}
		}
		return false
	}, 1*time.Second, 20*time.Millisecond)

	require.NotNil(t, leader)

	// 2. Propose command
	idx, _, ok := leader.Propose([]byte("test-command-1"), raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND)
	assert.True(t, ok)
	assert.GreaterOrEqual(t, idx, uint64(1))

	// 3. Verify entry commits across quorum
	assert.Eventually(t, func() bool {
		committedCount := 0
		for _, n := range nodes {
			_, _, _, commitIdx, _, _ := n.GetStatus()
			if commitIdx >= idx {
				committedCount++
			}
		}
		return committedCount >= 2
	}, 1*time.Second, 20*time.Millisecond)
}
