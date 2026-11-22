package raft

import (
	"sort"

	"github.com/raftlite/pkg/kv"
)

// advanceCommitIndexLocked checks if leader can advance commitIndex based on quorum matchIndex.
// Enforces Raft §5.4.2: Leaders only commit entries from their current term by counting replicas.
func (n *RaftNode) advanceCommitIndexLocked() {
	if n.state.role != RoleLeader {
		return
	}

	peers := n.getPeerIDs()
	totalNodes := len(peers) + 1
	majority := totalNodes/2 + 1

	lastLogIndex, _ := n.state.lastLogIndexAndTermLocked()
	oldCommit := n.state.commitIndex

	// Check each candidate index N from commitIndex + 1 to lastLogIndex
	for candidateN := n.state.commitIndex + 1; candidateN <= lastLogIndex; candidateN++ {
		// Invariant §5.4.2: entry must be from the current term
		if n.state.log[candidateN].Term != n.state.currentTerm {
			continue
		}

		// Count replicas that have matchIndex >= candidateN
		matchCount := 1 // Leader has replicated entry locally
		for _, peerID := range peers {
			if n.state.matchIndex[peerID] >= candidateN {
				matchCount++
			}
		}

		if matchCount >= majority {
			n.state.commitIndex = candidateN
		}
	}

	if n.state.commitIndex > oldCommit {
		n.notifyApply()
		// Immediately broadcast updated commitIndex to all followers
		go n.broadcastAppendEntries(true)
	}
}

// notifyApply signals the apply worker goroutine that commitIndex has advanced.
func (n *RaftNode) notifyApply() {
	select {
	case n.applyCond <- struct{}{}:
	default:
	}
}

// applyWorker reads committed entries sequentially and delivers them to applyCh.
func (n *RaftNode) applyWorker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCond:
		}

		for {
			n.state.mu.Lock()
			if n.state.lastApplied >= n.state.commitIndex {
				n.state.mu.Unlock()
				break
			}

			n.state.lastApplied++
			entry := n.state.log[n.state.lastApplied]
			applyMsg := kv.ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			n.state.mu.Unlock()

			// Deliver to state machine consumer without holding mutex
			select {
			case n.config.ApplyCh <- applyMsg:
			case <-n.stopCh:
				return
			}
		}
	}
}

// medianIndex helper computes the median matchIndex across all nodes.
func computeMajorityMatchIndex(matches []uint64) uint64 {
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	// Median of sorted list of length N
	mid := len(matches) / 2
	return matches[mid]
}
