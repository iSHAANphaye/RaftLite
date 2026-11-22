package chaos_test

import (
	"context"
	"net"
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

func TestChaos_NodeCrashAndRecovery(t *testing.T) {
	nodes, addrs, cleanup := startChaosCluster(t, 3, nil)
	defer cleanup()

	time.Sleep(300 * time.Millisecond)

	c := client.NewClient(client.Config{
		Endpoints: addrs,
		Timeout:   1 * time.Second,
	})
	defer c.Close()

	ctx := context.Background()

	// 1. Commit write before crash
	putResp, err := c.Put(ctx, "crash:key1", "val-before-crash")
	require.NoError(t, err)
	assert.True(t, putResp.Success)

	// 2. Abruptly kill node-1
	crashedNode := nodes[0]
	crashedDir := crashedNode.dataDir
	crashedAddr := crashedNode.addr

	crashedNode.grpcServer.Stop()
	crashedNode.peerMgr.Close()
	crashedNode.node.Stop()
	crashedNode.applier.Close()

	time.Sleep(200 * time.Millisecond)

	// 3. Commit write while node-1 is down (nodes 2 and 3 form 2/3 majority quorum)
	cSurviving := client.NewClient(client.Config{
		Endpoints: []string{addrs[1], addrs[2]},
		Timeout:   1 * time.Second,
	})
	defer cSurviving.Close()

	putResp2, err := cSurviving.Put(ctx, "crash:key2", "val-during-crash")
	require.NoError(t, err)
	assert.True(t, putResp2.Success)

	// 4. Restart node-1 from same dataDir
	lis, err := net.Listen("tcp", crashedAddr)
	require.NoError(t, err)

	peers := make(map[string]string)
	peers["node-2"] = addrs[1]
	peers["node-3"] = addrs[2]

	peerMgr := cluster.NewPeerManager("node-1", peers, "", nil)
	transport := cluster.NewGrpcTransport(peerMgr)

	applyCh := make(chan kv.ApplyMsg, 1000)
	raftCfg := raft.Config{
		NodeID:             "node-1",
		Peers:              peers,
		StorageDir:         crashedDir,
		SyncOnWrite:        true,
		HeartbeatInterval:  25 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 150 * time.Millisecond,
		ApplyCh:            applyCh,
	}

	restartedNode, err := raft.NewRaftNode(raftCfg, transport)
	require.NoError(t, err)

	applier := kv.NewApplier(nil, nil, applyCh)
	srv := server.NewServer(restartedNode, applier, peers, "")

	grpcSrv := grpc.NewServer()
	raftlitev1.RegisterRaftServiceServer(grpcSrv, srv)
	raftlitev1.RegisterKVServiceServer(grpcSrv, srv)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	restartedNode.Start()

	// Update node in slice so cleanup works cleanly
	nodes[0] = &clusterNode{
		id:         "node-1",
		addr:       crashedAddr,
		dataDir:    crashedDir,
		node:       restartedNode,
		applier:    applier,
		peerMgr:    peerMgr,
		grpcServer: grpcSrv,
	}

	// 5. Allow node-1 to rejoin and replicate missed entries
	time.Sleep(400 * time.Millisecond)

	// 6. Verify linearizable read of both keys across whole cluster
	cAll := client.NewClient(client.Config{
		Endpoints: addrs,
		Timeout:   1 * time.Second,
	})
	defer cAll.Close()

	g1, err := cAll.Get(ctx, "crash:key1", true)
	require.NoError(t, err)
	assert.True(t, g1.Found)
	assert.Equal(t, "val-before-crash", g1.Value)

	g2, err := cAll.Get(ctx, "crash:key2", true)
	require.NoError(t, err)
	assert.True(t, g2.Found)
	assert.Equal(t, "val-during-crash", g2.Value)
}
