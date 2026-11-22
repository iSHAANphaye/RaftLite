package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// Transport defines the network interface for exchanging Raft RPCs with peers.
type Transport interface {
	SendRequestVote(ctx context.Context, targetPeer string, req *raftlitev1.RequestVoteRequest) (*raftlitev1.RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, targetPeer string, req *raftlitev1.AppendEntriesRequest) (*raftlitev1.AppendEntriesResponse, error)
}

// RaftNode represents an active instance of a Raft consensus engine node.
type RaftNode struct {
	config    Config
	state     *RaftState
	transport Transport

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyCond chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewRaftNode initializes a RaftNode with state and transport.
func NewRaftNode(cfg Config, transport Transport) (*RaftNode, error) {
	state, err := NewRaftState(cfg.StorageDir, cfg.SyncOnWrite)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to init state: %w", err)
	}

	node := &RaftNode{
		config:          cfg,
		state:           state,
		transport:       transport,
		heartbeatTicker: time.NewTicker(cfg.HeartbeatInterval),
		applyCond:       make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}

	return node, nil
}

// Start begins election timers, heartbeat loops, and apply workers.
func (n *RaftNode) Start() {
	n.state.mu.Lock()
	n.resetElectionTimerLocked()
	n.state.mu.Unlock()

	n.wg.Add(2)
	go n.runLoop()
	go func() {
		defer n.wg.Done()
		n.applyWorker()
	}()
}

// runLoop is the main event loop handling election timeouts and heartbeat ticks.
func (n *RaftNode) runLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionTimer.C:
			n.startElection()

		case <-n.heartbeatTicker.C:
			n.state.mu.RLock()
			isLeader := (n.state.role == RoleLeader)
			n.state.mu.RUnlock()

			if isLeader {
				n.broadcastAppendEntries(true)
			}
		}
	}
}

// Propose submits a new command to the consensus log.
// Returns the allocated index, current term, and whether this node was the leader.
func (n *RaftNode) Propose(data []byte, entryType raftlitev1.LogEntryType) (uint64, uint64, bool) {
	n.state.mu.Lock()
	if n.state.role != RoleLeader {
		n.state.mu.Unlock()
		return 0, 0, false
	}

	newIndex := uint64(len(n.state.log))
	newTerm := n.state.currentTerm

	entry := &raftlitev1.LogEntry{
		Term:  newTerm,
		Index: newIndex,
		Type:  entryType,
		Data:  data,
	}

	_ = n.state.wal.Append(entry)
	n.state.log = append(n.state.log, entry)
	n.state.mu.Unlock()

	// Trigger replication to peers immediately
	go n.broadcastAppendEntries(false)

	return newIndex, newTerm, true
}

// GetStatus returns the public inspection status of this node.
func (n *RaftNode) GetStatus() (string, NodeRole, uint64, uint64, uint64, string) {
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	return n.config.NodeID, n.state.role, n.state.currentTerm, n.state.commitIndex, n.state.lastApplied, n.state.leaderID
}

// IsLeader returns whether this node currently considers itself leader.
func (n *RaftNode) IsLeader() bool {
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	return n.state.role == RoleLeader
}

// PeerIndexInfo captures peer replication indexes.
type PeerIndexInfo struct {
	MatchIndex uint64
	NextIndex  uint64
}

// GetPeersStatus returns the replication indexes for all peers.
func (n *RaftNode) GetPeersStatus() map[string]PeerIndexInfo {
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	res := make(map[string]PeerIndexInfo, len(n.state.matchIndex))
	for p, match := range n.state.matchIndex {
		res[p] = PeerIndexInfo{
			MatchIndex: match,
			NextIndex:  n.state.nextIndex[p],
		}
	}
	return res
}

// ReadIndex implements the Raft linearizable ReadIndex protocol (§6.4).
func (n *RaftNode) ReadIndex(ctx context.Context) (uint64, error) {
	n.state.mu.RLock()
	if n.state.role != RoleLeader {
		hint := n.state.leaderID
		n.state.mu.RUnlock()
		return 0, fmt.Errorf("not leader (hint: %s)", hint)
	}
	readIndex := n.state.commitIndex
	currentTerm := n.state.currentTerm
	n.state.mu.RUnlock()

	peers := n.getPeerIDs()
	if len(peers) == 0 {
		return readIndex, nil
	}

	majority := (len(peers)+1)/2 + 1
	var votes int32 = 1
	var wg sync.WaitGroup

	for _, peerID := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			n.state.mu.RLock()
			prevIndex := n.state.nextIndex[p] - 1
			var prevTerm uint64
			if prevIndex > 0 && prevIndex < uint64(len(n.state.log)) {
				prevTerm = n.state.log[prevIndex].Term
			}
			req := &raftlitev1.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     n.config.NodeID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      nil,
				LeaderCommit: readIndex,
			}
			n.state.mu.RUnlock()

			resp, err := n.transport.SendAppendEntries(ctx, p, req)
			if err == nil && resp != nil && resp.Success {
				atomic.AddInt32(&votes, 1)
			}
		}(peerID)
	}

	wg.Wait()
	if int(votes) < majority {
		return 0, errors.New("failed to acquire quorum heartbeat for read index")
	}

	return readIndex, nil
}

// GetLeaderHint returns current known leader ID or address.
func (n *RaftNode) GetLeaderHint() string {
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	return n.state.leaderID
}

// getPeerIDs returns a list of peer node IDs.
func (n *RaftNode) getPeerIDs() []string {
	peers := make([]string, 0, len(n.config.Peers))
	for peerID := range n.config.Peers {
		peers = append(peers, peerID)
	}
	return peers
}

// Stop cleanly terminates timers, workers, and closes WAL.
func (n *RaftNode) Stop() {
	close(n.stopCh)
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	n.wg.Wait()

	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if n.state.wal != nil {
		_ = n.state.wal.Close()
	}
}
