package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// randomElectionDuration returns a random duration between min and max timeouts.
func (n *RaftNode) randomElectionDuration() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	delta := max - min
	if delta <= 0 {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(delta)))
}

// resetElectionTimer restarts the election timer with a fresh randomized duration.
// Must be called with lock held or via thread-safe reset.
func (n *RaftNode) resetElectionTimerLocked() {
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(n.randomElectionDuration())
		return
	}
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(n.randomElectionDuration())
}

// startElection triggers an election cycle when the election timer expires.
func (n *RaftNode) startElection() {
	n.state.mu.Lock()
	if n.state.role == RoleLeader {
		n.state.mu.Unlock()
		return
	}

	// 1. Transition to Candidate
	n.state.role = RoleCandidate
	n.state.currentTerm++
	n.state.votedFor = n.config.NodeID
	_ = n.state.persistMetaLocked()

	currentTerm := n.state.currentTerm
	lastLogIndex, lastLogTerm := n.state.lastLogIndexAndTermLocked()
	n.resetElectionTimerLocked()

	// 2. Request votes from peers
	peers := n.getPeerIDs()
	totalNodes := len(peers) + 1
	majority := totalNodes/2 + 1

	// Single-node cluster immediately wins election
	if majority <= 1 {
		n.becomeLeaderLocked()
		n.state.mu.Unlock()
		return
	}
	n.state.mu.Unlock()

	var votesMu sync.Mutex
	votesGranted := 1 // Vote for self

	for _, peerID := range peers {
		go func(targetPeer string) {
			req := &raftlitev1.RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  n.config.NodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := n.transport.SendRequestVote(ctx, targetPeer, req)
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

			// Tally vote if still candidate in the same election term
			if n.state.role == RoleCandidate && resp.Term == currentTerm && resp.VoteGranted {
				votesMu.Lock()
				votesGranted++
				gotMajority := (votesGranted >= majority)
				votesMu.Unlock()

				if gotMajority && n.state.role == RoleCandidate {
					n.becomeLeaderLocked()
				}
			}
		}(peerID)
	}
}

// HandleRequestVote processes an incoming RequestVote RPC from a candidate.
func (n *RaftNode) HandleRequestVote(req *raftlitev1.RequestVoteRequest) (*raftlitev1.RequestVoteResponse, error) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()

	// 1. Term check
	if req.Term < n.state.currentTerm {
		return &raftlitev1.RequestVoteResponse{
			Term:        n.state.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If candidate has higher term, convert to Follower
	if req.Term > n.state.currentTerm {
		n.state.currentTerm = req.Term
		n.state.role = RoleFollower
		n.state.votedFor = ""
		_ = n.state.persistMetaLocked()
	}

	// 2. Election Restriction (§5.4.1): candidate's log must be as up-to-date as receiver's
	myLastIndex, myLastTerm := n.state.lastLogIndexAndTermLocked()
	logOk := false
	if req.LastLogTerm > myLastTerm {
		logOk = true
	} else if req.LastLogTerm == myLastTerm && req.LastLogIndex >= myLastIndex {
		logOk = true
	}

	// 3. Grant vote if not already voted for another candidate in this term and log is valid
	canVote := (n.state.votedFor == "" || n.state.votedFor == req.CandidateId)
	if canVote && logOk {
		n.state.votedFor = req.CandidateId
		_ = n.state.persistMetaLocked()
		n.resetElectionTimerLocked()
		return &raftlitev1.RequestVoteResponse{
			Term:        n.state.currentTerm,
			VoteGranted: true,
		}, nil
	}

	return &raftlitev1.RequestVoteResponse{
		Term:        n.state.currentTerm,
		VoteGranted: false,
	}, nil
}
