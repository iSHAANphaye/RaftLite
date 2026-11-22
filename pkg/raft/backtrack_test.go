package raft

import (
	"testing"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBacktrack_GapFollowerTooShort(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig("follower", nil, dir)
	n, err := NewRaftNode(cfg, nil)
	require.NoError(t, err)
	defer n.Stop()

	// Follower has 3 entries
	n.state.mu.Lock()
	n.state.currentTerm = 2
	n.state.log = append(n.state.log,
		&raftlitev1.LogEntry{Term: 1, Index: 1},
		&raftlitev1.LogEntry{Term: 1, Index: 2},
		&raftlitev1.LogEntry{Term: 2, Index: 3},
	)
	n.state.mu.Unlock()

	// Leader sends AppendEntries with PrevLogIndex = 10 (gap)
	resp, err := n.HandleAppendEntries(&raftlitev1.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 10,
		PrevLogTerm:  2,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(0), resp.ConflictTerm)
	assert.Equal(t, uint64(4), resp.ConflictFirstIndex) // len(log) = 3 + 1 = 4
}

func TestBacktrack_TermMismatchBacktracksToFirstIndex(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig("follower", nil, dir)
	n, err := NewRaftNode(cfg, nil)
	require.NoError(t, err)
	defer n.Stop()

	// Follower has entries:
	// Index 1 (Term 1), Index 2 (Term 2), Index 3 (Term 2), Index 4 (Term 2)
	n.state.mu.Lock()
	n.state.currentTerm = 3
	n.state.log = append(n.state.log,
		&raftlitev1.LogEntry{Term: 1, Index: 1},
		&raftlitev1.LogEntry{Term: 2, Index: 2},
		&raftlitev1.LogEntry{Term: 2, Index: 3},
		&raftlitev1.LogEntry{Term: 2, Index: 4},
	)
	n.state.mu.Unlock()

	// Leader sends PrevLogIndex = 4, but Leader believes PrevLogTerm is 3
	resp, err := n.HandleAppendEntries(&raftlitev1.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader",
		PrevLogIndex: 4,
		PrevLogTerm:  3,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(2), resp.ConflictTerm)
	assert.Equal(t, uint64(2), resp.ConflictFirstIndex) // Index 2 was the first entry of Term 2
}

func TestBacktrack_LeaderCalculatesNextIndexCorrectly(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig("leader", nil, dir)
	leader, err := NewRaftNode(cfg, nil)
	require.NoError(t, err)
	defer leader.Stop()

	leader.state.mu.Lock()
	leader.state.role = RoleLeader
	leader.state.currentTerm = 4
	leader.state.log = append(leader.state.log,
		&raftlitev1.LogEntry{Term: 1, Index: 1},
		&raftlitev1.LogEntry{Term: 1, Index: 2},
		&raftlitev1.LogEntry{Term: 3, Index: 3},
		&raftlitev1.LogEntry{Term: 4, Index: 4},
	)
	leader.state.nextIndex["peer-1"] = 5
	leader.state.mu.Unlock()

	// Peer reports ConflictTerm = 2 (which leader does not have), ConflictFirstIndex = 3
	resp := &raftlitev1.AppendEntriesResponse{
		Success:            false,
		ConflictTerm:       2,
		ConflictFirstIndex: 3,
	}

	leader.state.mu.Lock()
	leader.handleConflictBacktrackLocked("peer-1", resp)
	nextIdx := leader.state.nextIndex["peer-1"]
	leader.state.mu.Unlock()

	assert.Equal(t, uint64(3), nextIdx)
}
