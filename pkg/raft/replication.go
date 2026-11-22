package raft

import (
	"context"
	"time"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// becomeLeaderLocked transitions a candidate to leader and initializes peer indices.
func (n *RaftNode) becomeLeaderLocked() {
	n.state.role = RoleLeader
	n.state.leaderID = n.config.NodeID

	// Stop election timer on leader to prevent self-timeout
	n.stopElectionTimerLocked()

	lastLogIndex, _ := n.state.lastLogIndexAndTermLocked()
	peers := n.getPeerIDs()

	for _, peerID := range peers {
		n.state.nextIndex[peerID] = lastLogIndex + 1
		n.state.matchIndex[peerID] = 0
	}

	// Send immediate heartbeats to suppress competing elections
	go n.broadcastAppendEntries(true)
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers concurrently.
func (n *RaftNode) broadcastAppendEntries(isHeartbeat bool) {
	n.state.mu.RLock()
	if n.state.role != RoleLeader {
		n.state.mu.RUnlock()
		return
	}
	peers := n.getPeerIDs()
	n.state.mu.RUnlock()

	for _, peerID := range peers {
		go n.replicateToPeer(peerID, isHeartbeat)
	}
}

// replicateToPeer builds and sends an AppendEntries RPC to a single peer.
func (n *RaftNode) replicateToPeer(peerID string, isHeartbeat bool) {
	n.state.mu.RLock()
	if n.state.role != RoleLeader {
		n.state.mu.RUnlock()
		return
	}

	nextIdx, ok := n.state.nextIndex[peerID]
	if !ok {
		nextIdx = 1
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex < uint64(len(n.state.log)) {
		prevLogTerm = n.state.log[prevLogIndex].Term
	}

	var entries []*raftlitev1.LogEntry
	if !isHeartbeat && nextIdx < uint64(len(n.state.log)) {
		entries = make([]*raftlitev1.LogEntry, len(n.state.log)-int(nextIdx))
		copy(entries, n.state.log[nextIdx:])
	}

	req := &raftlitev1.AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.config.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.state.commitIndex,
	}
	currentTerm := n.state.currentTerm
	n.state.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := n.transport.SendAppendEntries(ctx, peerID, req)
	if err != nil {
		return
	}

	n.state.mu.Lock()
	defer n.state.mu.Unlock()

	// If peer reported higher term, step down to Follower
	if resp.Term > n.state.currentTerm {
		n.state.currentTerm = resp.Term
		n.state.role = RoleFollower
		n.state.votedFor = ""
		_ = n.state.persistMetaLocked()
		n.resetElectionTimerLocked()
		return
	}

	if n.state.role != RoleLeader || currentTerm != n.state.currentTerm {
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex
		matchIdx := req.PrevLogIndex + uint64(len(req.Entries))
		if matchIdx > n.state.matchIndex[peerID] {
			n.state.matchIndex[peerID] = matchIdx
			n.state.nextIndex[peerID] = matchIdx + 1
		}
		// Check if commitIndex can be advanced
		n.advanceCommitIndexLocked()
	} else {
		// Fast Log Conflict Backtracking (§TICKET-RL-007)
		n.handleConflictBacktrackLocked(peerID, resp)
		// Immediately retry replication
		go n.replicateToPeer(peerID, false)
	}
}

// HandleAppendEntries handles incoming AppendEntries RPCs from the leader.
func (n *RaftNode) HandleAppendEntries(req *raftlitev1.AppendEntriesRequest) (*raftlitev1.AppendEntriesResponse, error) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()

	// 1. Reply false if req.Term < currentTerm (§5.1)
	if req.Term < n.state.currentTerm {
		return &raftlitev1.AppendEntriesResponse{
			Term:    n.state.currentTerm,
			Success: false,
		}, nil
	}

	// If term is greater, update term and convert to Follower
	if req.Term > n.state.currentTerm {
		n.state.currentTerm = req.Term
		n.state.role = RoleFollower
		n.state.votedFor = ""
		_ = n.state.persistMetaLocked()
	}

	// Valid leader confirmed: reset election timer and record leaderID
	n.state.role = RoleFollower
	n.state.leaderID = req.LeaderId
	n.resetElectionTimerLocked()

	lastLogIndex, _ := n.state.lastLogIndexAndTermLocked()

	// 2. Fast Log Backtracking Case 1: Follower log too short (§5.3)
	if lastLogIndex < req.PrevLogIndex {
		return &raftlitev1.AppendEntriesResponse{
			Term:               n.state.currentTerm,
			Success:            false,
			ConflictTerm:       0,
			ConflictFirstIndex: lastLogIndex + 1,
		}, nil
	}

	// 3. Fast Log Backtracking Case 2: Term mismatch at PrevLogIndex
	if req.PrevLogIndex > 0 && n.state.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		conflictTerm := n.state.log[req.PrevLogIndex].Term
		var conflictFirstIndex uint64 = req.PrevLogIndex
		for i := req.PrevLogIndex; i >= 1; i-- {
			if n.state.log[i].Term == conflictTerm {
				conflictFirstIndex = i
			} else {
				break
			}
		}

		return &raftlitev1.AppendEntriesResponse{
			Term:               n.state.currentTerm,
			Success:            false,
			ConflictTerm:       conflictTerm,
			ConflictFirstIndex: conflictFirstIndex,
		}, nil
	}

	// 4. Log consistency confirmed: Append any new entries not already in the log
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)
		if index < uint64(len(n.state.log)) {
			if n.state.log[index].Term != entry.Term {
				// Conflict: truncate existing entry and all following entries from log and WAL
				_ = n.state.wal.Truncate(index - 1)
				n.state.log = n.state.log[:index]
				_ = n.state.wal.Append(entry)
				n.state.log = append(n.state.log, entry)
			}
		} else {
			// Append new entry
			_ = n.state.wal.Append(entry)
			n.state.log = append(n.state.log, entry)
		}
	}

	// 5. Update commitIndex (§5.3)
	if req.LeaderCommit > n.state.commitIndex {
		newLastIndex, _ := n.state.lastLogIndexAndTermLocked()
		if req.LeaderCommit < newLastIndex {
			n.state.commitIndex = req.LeaderCommit
		} else {
			n.state.commitIndex = newLastIndex
		}
		n.notifyApply()
	}

	newMatchIndex, _ := n.state.lastLogIndexAndTermLocked()
	return &raftlitev1.AppendEntriesResponse{
		Term:       n.state.currentTerm,
		Success:    true,
		MatchIndex: newMatchIndex,
	}, nil
}
