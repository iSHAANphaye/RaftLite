package raft

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/raftlite/pkg/storage"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElection_Restriction_StaleCandidateRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig("node-1", map[string]string{"node-2": ""}, dir)
	net := NewMockNetwork()
	node, err := NewRaftNode(cfg, net)
	require.NoError(t, err)
	defer node.Stop()

	// Simulate receiver node-1 has log at Term 2, Index 5
	node.state.mu.Lock()
	node.state.currentTerm = 2
	node.state.log = append(node.state.log, &raftlitev1.LogEntry{Term: 2, Index: 1})
	node.state.log = append(node.state.log, &raftlitev1.LogEntry{Term: 2, Index: 2})
	node.state.mu.Unlock()

	// Candidate with lower lastLogTerm (Term 1) -> must be rejected
	resp, err := node.HandleRequestVote(&raftlitev1.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-2",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)

	// Candidate with shorter index at same term -> must be rejected
	resp, err = node.HandleRequestVote(&raftlitev1.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-2",
		LastLogIndex: 1,
		LastLogTerm:  2,
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)

	// Candidate with equal or greater index and equal term -> must be granted
	resp, err = node.HandleRequestVote(&raftlitev1.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-2",
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)

	// Check persistence
	savedTerm, savedVotedFor, err := storage.LoadMeta(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), savedTerm)
	assert.Equal(t, "node-2", savedVotedFor)
}

func TestElection_SingleNodeElectsSelf(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig("node-solo", nil, dir)
	cfg.ElectionTimeoutMin = 20 * time.Millisecond
	cfg.ElectionTimeoutMax = 40 * time.Millisecond

	net := NewMockNetwork()
	node, err := NewRaftNode(cfg, net)
	require.NoError(t, err)
	net.Register(node)

	node.Start()
	defer node.Stop()

	// Wait for election timeout to trigger
	assert.Eventually(t, func() bool {
		_, role, term, _, _, _ := node.GetStatus()
		return role == RoleLeader && term >= 1
	}, 500*time.Millisecond, 20*time.Millisecond)
}

func TestElection_ThreeNodesClusterElection(t *testing.T) {
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
		// Create peers excluding self
		peers := make(map[string]string)
		for k, v := range peerMap {
			if k != nodeID {
				peers[k] = v
			}
		}

		cfg := DefaultConfig(nodeID, peers, filepath.Join(tempBase, nodeID))
		cfg.ElectionTimeoutMin = 40 * time.Millisecond
		cfg.ElectionTimeoutMax = 80 * time.Millisecond

		n, err := NewRaftNode(cfg, net)
		require.NoError(t, err)
		net.Register(n)
		nodes[i-1] = n
	}

	for _, n := range nodes {
		n.Start()
		defer n.Stop()
	}

	// Wait for exactly one leader to emerge
	var leaderID string
	assert.Eventually(t, func() bool {
		leaderCount := 0
		for _, n := range nodes {
			_, role, _, _, _, _ := n.GetStatus()
			if role == RoleLeader {
				leaderCount++
				leaderID = n.config.NodeID
			}
		}
		return leaderCount == 1
	}, 1*time.Second, 20*time.Millisecond)

	assert.NotEmpty(t, leaderID)
}
