package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/raftlite/pkg/client"
	"github.com/raftlite/pkg/cluster"
	"github.com/raftlite/pkg/kv"
	"github.com/raftlite/pkg/raft"
	"github.com/raftlite/pkg/server"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type inProcessNode struct {
	id         string
	addr       string
	dataDir    string
	node       *raft.RaftNode
	applier    *kv.Applier
	peerMgr    *cluster.PeerManager
	grpcServer *grpc.Server
	listener   net.Listener
}

func startInProcessCluster(t *testing.T, n int) ([]*inProcessNode, *client.Client, func()) {
	// 1. Allocate ephemeral listeners
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	nodes := make([]*inProcessNode, n)
	for i := 0; i < n; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		peers := make(map[string]string)
		for j := 0; j < n; j++ {
			if i != j {
				peers[fmt.Sprintf("node-%d", j+1)] = addrs[j]
			}
		}

		dataDir, err := os.MkdirTemp("", "raftlite-test-*")
		require.NoError(t, err)

		peerMgr := cluster.NewPeerManager(nodeID, peers, "", nil)
		transport := cluster.NewGrpcTransport(peerMgr)

		applyCh := make(chan kv.ApplyMsg, 1000)
		raftCfg := raft.Config{
			NodeID:             nodeID,
			Peers:              peers,
			StorageDir:         dataDir,
			SyncOnWrite:        true,
			HeartbeatInterval:  30 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			ApplyCh:            applyCh,
		}

		rNode, err := raft.NewRaftNode(raftCfg, transport)
		require.NoError(t, err)

		applier := kv.NewApplier(nil, nil, applyCh)
		srv := server.NewServer(rNode, applier, peers, "")

		grpcSrv := grpc.NewServer()
		raftlitev1.RegisterRaftServiceServer(grpcSrv, srv)
		raftlitev1.RegisterKVServiceServer(grpcSrv, srv)

		go func(lis net.Listener, s *grpc.Server) {
			_ = s.Serve(lis)
		}(listeners[i], grpcSrv)

		rNode.Start()

		nodes[i] = &inProcessNode{
			id:         nodeID,
			addr:       addrs[i],
			dataDir:    dataDir,
			node:       rNode,
			applier:    applier,
			peerMgr:    peerMgr,
			grpcServer: grpcSrv,
			listener:   listeners[i],
		}
	}

	c := client.NewClient(client.Config{
		Endpoints: addrs,
		Timeout:   2 * time.Second,
	})

	cleanup := func() {
		c.Close()
		for _, nd := range nodes {
			if nd.grpcServer != nil {
				nd.grpcServer.Stop()
			}
			if nd.peerMgr != nil {
				nd.peerMgr.Close()
			}
			if nd.node != nil {
				nd.node.Stop()
			}
			if nd.applier != nil {
				nd.applier.Close()
			}
			_ = os.RemoveAll(nd.dataDir)
		}
	}

	return nodes, c, cleanup
}

func TestIntegration_ClusterElectionAndBasicKV(t *testing.T) {
	_, c, cleanup := startInProcessCluster(t, 3)
	defer cleanup()

	// Wait for a leader to be elected
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()

	// 1. Put key
	putResp, err := c.Put(ctx, "cluster:test:k1", "val1")
	require.NoError(t, err)
	assert.True(t, putResp.Success)

	// 2. Linearizable Get key
	getResp, err := c.Get(ctx, "cluster:test:k1", true)
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, "val1", getResp.Value)

	// 3. Compare-And-Swap
	casResp, err := c.CAS(ctx, "cluster:test:k1", "val1", "val2")
	require.NoError(t, err)
	assert.True(t, casResp.Swapped)

	// Verify CAS value
	getResp2, err := c.Get(ctx, "cluster:test:k1", true)
	require.NoError(t, err)
	assert.Equal(t, "val2", getResp2.Value)

	// 4. Failed CAS with mismatched expected value
	casRespFail, err := c.CAS(ctx, "cluster:test:k1", "wrong-val", "val3")
	require.NoError(t, err)
	assert.False(t, casRespFail.Swapped)
	assert.Equal(t, "val2", casRespFail.CurrentValue)

	// 5. Delete key
	delResp, err := c.Delete(ctx, "cluster:test:k1")
	require.NoError(t, err)
	assert.True(t, delResp.Found)

	// Verify deleted
	getRespDel, err := c.Get(ctx, "cluster:test:k1", true)
	require.NoError(t, err)
	assert.False(t, getRespDel.Found)
}

func TestIntegration_Deduplication(t *testing.T) {
	_, c, cleanup := startInProcessCluster(t, 3)
	defer cleanup()

	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	clientID := "dedup-client-1"

	// First request with sequence 1
	r1, err := c.PutWithSeq(ctx, "dedup:key", "initial", clientID, 1)
	require.NoError(t, err)
	assert.True(t, r1.Success)

	// Second request with SAME sequence 1 but different payload
	r2, err := c.PutWithSeq(ctx, "dedup:key", "should_be_suppressed", clientID, 1)
	require.NoError(t, err)
	assert.True(t, r2.Success)

	// Value in KV store must still be "initial" because duplicate was served from cache
	getResp, err := c.Get(ctx, "dedup:key", true)
	require.NoError(t, err)
	assert.Equal(t, "initial", getResp.Value)
}
