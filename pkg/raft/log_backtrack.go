package raft

import (
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// handleConflictBacktrackLocked applies the fast conflict backtracking algorithm
// to recalculate nextIndex for a peer after a rejected AppendEntries RPC.
func (n *RaftNode) handleConflictBacktrackLocked(peerID string, resp *raftlitev1.AppendEntriesResponse) {
	if resp.ConflictTerm == 0 {
		// Case 1: Follower's log is too short (gap between prevLogIndex and follower's lastLogIndex)
		n.state.nextIndex[peerID] = resp.ConflictFirstIndex
	} else {
		// Case 2: Follower has a conflicting term at prevLogIndex
		// Search leader's own log for entries matching ConflictTerm
		var lastIndexWithTerm uint64
		found := false

		for i := len(n.state.log) - 1; i >= 1; i-- {
			if n.state.log[i].Term == resp.ConflictTerm {
				lastIndexWithTerm = n.state.log[i].Index
				found = true
				break
			}
		}

		if found {
			// Leader has entries with ConflictTerm: jump to first entry after that term
			n.state.nextIndex[peerID] = lastIndexWithTerm + 1
		} else {
			// Leader does not have entries with ConflictTerm: jump to follower's first index of that term
			n.state.nextIndex[peerID] = resp.ConflictFirstIndex
		}
	}

	// Safety boundary: nextIndex must be at least 1
	if n.state.nextIndex[peerID] < 1 {
		n.state.nextIndex[peerID] = 1
	}
}
