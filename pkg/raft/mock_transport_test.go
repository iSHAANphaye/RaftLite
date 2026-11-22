package raft

import (
	"context"
	"fmt"
	"sync"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// MockNetwork routes RPCs between in-memory Raft nodes.
type MockNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*RaftNode
}

func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		nodes: make(map[string]*RaftNode),
	}
}

func (m *MockNetwork) Register(node *RaftNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.config.NodeID] = node
}

func (m *MockNetwork) SendRequestVote(ctx context.Context, targetPeer string, req *raftlitev1.RequestVoteRequest) (*raftlitev1.RequestVoteResponse, error) {
	m.mu.RLock()
	node, ok := m.nodes[targetPeer]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("network: target peer %s not found", targetPeer)
	}
	return node.HandleRequestVote(req)
}

func (m *MockNetwork) SendAppendEntries(ctx context.Context, targetPeer string, req *raftlitev1.AppendEntriesRequest) (*raftlitev1.AppendEntriesResponse, error) {
	m.mu.RLock()
	node, ok := m.nodes[targetPeer]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("network: target peer %s not found", targetPeer)
	}
	return node.HandleAppendEntries(req)
}
