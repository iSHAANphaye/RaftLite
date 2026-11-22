package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/raftlite/pkg/chaos"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	ClusterTokenHeader = "x-raftlite-cluster-token"
)

// PeerClient manages the persistent gRPC connection to a remote cluster peer.
type PeerClient struct {
	ID      string
	Address string
	conn    *grpc.ClientConn
	client  raftlitev1.RaftServiceClient
	mu      sync.Mutex
}

// PeerManager maintains the registry of peer nodes and their active gRPC connections.
type PeerManager struct {
	mu           sync.RWMutex
	localNodeID  string
	peers        map[string]*PeerClient
	clusterToken string
	chaosCtrl    *chaos.ChaosController
}

// NewPeerManager creates a PeerManager for connecting to cluster peers.
func NewPeerManager(localNodeID string, peers map[string]string, clusterToken string, chaosCtrl *chaos.ChaosController) *PeerManager {
	pm := &PeerManager{
		localNodeID:  localNodeID,
		peers:        make(map[string]*PeerClient),
		clusterToken: clusterToken,
		chaosCtrl:    chaosCtrl,
	}

	for id, addr := range peers {
		pm.peers[id] = &PeerClient{
			ID:      id,
			Address: addr,
		}
	}
	return pm
}

// GetClient retrieves or initializes the RaftServiceClient connection to a peer.
func (pm *PeerManager) GetClient(ctx context.Context, peerID string) (raftlitev1.RaftServiceClient, error) {
	pm.mu.RLock()
	peer, exists := pm.peers[peerID]
	pm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("cluster: peer %s not registered", peerID)
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.client != nil && peer.conn != nil {
		return peer.client, nil
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if pm.chaosCtrl != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(chaos.UnaryClientInterceptor(pm.chaosCtrl, pm.localNodeID)))
	}

	conn, err := grpc.DialContext(ctx, peer.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster: dial %s at %s failed: %w", peerID, peer.Address, err)
	}

	peer.conn = conn
	peer.client = raftlitev1.NewRaftServiceClient(conn)
	return peer.client, nil
}

// Close terminates all active peer connections.
func (pm *PeerManager) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, p := range pm.peers {
		p.mu.Lock()
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn = nil
			p.client = nil
		}
		p.mu.Unlock()
	}
}
