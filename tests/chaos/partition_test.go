package chaos_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/raftlite/pkg/chaos"
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

type clusterNode struct {
	id         string
	addr       string
	dataDir    string
	node       *raft.RaftNode
	applier    *kv.Applier
	peerMgr    *cluster.PeerManager
	grpcServer *grpc.Server
}

func startChaosCluster(t *testing.T, n int, chaosCtrl *chaos.ChaosController) ([]*clusterNode, []string, func()) {
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	nodes := make([]*clusterNode, n)
	for i := 0; i < n; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		peers := make(map[string]string)
		for j := 0; j < n; j++ {
			if i != j {
				peers[fmt.Sprintf("node-%d", j+1)] = addrs[j]
			}
		}

		dataDir, err := os.MkdirTemp("", "raftlite-chaos-*")
		require.NoError(t, err)

		peerMgr := cluster.NewPeerManager(nodeID, peers, "", chaosCtrl)
		transport := cluster.NewGrpcTransport(peerMgr)

		applyCh := make(chan kv.ApplyMsg, 1000)
		raftCfg := raft.Config{
			NodeID:             nodeID,
			Peers:              peers,
			StorageDir:         dataDir,
			SyncOnWrite:        true,
			HeartbeatInterval:  25 * time.Millisecond,
			ElectionTimeoutMin: 80 * time.Millisecond,
			ElectionTimeoutMax: 150 * time.Millisecond,
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

		nodes[i] = &clusterNode{
			id:         nodeID,
			addr:       addrs[i],
			dataDir:    dataDir,
			node:       rNode,
			applier:    applier,
			peerMgr:    peerMgr,
			grpcServer: grpcSrv,
		}
	}

	cleanup := func() {
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

	return nodes, addrs, cleanup
}

func TestChaos_NetworkPartitionMajorityMinority(t *testing.T) {
	chaosCtrl := chaos.NewChaosController()
	_, addrs, cleanup := startChaosCluster(t, 5, chaosCtrl)
	defer cleanup()

	// Allow cluster to elect an initial leader
	time.Sleep(300 * time.Millisecond)

	// Partition: {node-1, node-2} vs {node-3, node-4, node-5}
	minority := []string{"node-1", "node-2"}
	majority := []string{"node-3", "node-4", "node-5"}
	chaosCtrl.PartitionNodes(minority, majority)

	// Wait for election stabilization in majority partition
	time.Sleep(300 * time.Millisecond)

	majorityClient := client.NewClient(client.Config{
		Endpoints: []string{addrs[2], addrs[3], addrs[4]},
		Timeout:   1 * time.Second,
	})
	defer majorityClient.Close()

	// 1. Majority partition should successfully commit writes (3/5 quorum satisfied)
	ctx := context.Background()
	putResp, err := majorityClient.Put(ctx, "part-key", "majority-value")
	require.NoError(t, err)
	assert.True(t, putResp.Success)

	// 2. Minority partition cannot commit writes (max votes = 2 < 3)
	minorityClient := client.NewClient(client.Config{
		Endpoints: []string{addrs[0], addrs[1]},
		Timeout:   400 * time.Millisecond,
	})
	defer minorityClient.Close()

	minCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, minErr := minorityClient.Put(minCtx, "minority-key", "minority-val")
	assert.Error(t, minErr, "Minority partition must not be able to commit writes")

	// 3. Heal partition
	chaosCtrl.HealAll()
	time.Sleep(300 * time.Millisecond)

	// 4. Verify all nodes reconcile: read "part-key" via whole cluster client
	allClient := client.NewClient(client.Config{
		Endpoints: addrs,
		Timeout:   1 * time.Second,
	})
	defer allClient.Close()

	getResp, err := allClient.Get(context.Background(), "part-key", true)
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, "majority-value", getResp.Value)
}
