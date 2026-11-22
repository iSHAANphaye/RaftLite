package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/raftlite/pkg/cluster"
	"github.com/raftlite/pkg/kv"
	"github.com/raftlite/pkg/raft"
	"github.com/raftlite/pkg/server"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := ParseConfig()
	if err != nil {
		log.Fatalf("[FATAL] Configuration error: %v", err)
	}

	log.Printf("[INFO] Starting RaftLite daemon node=%s listen=%s client=%s peers=%d sync=%v",
		cfg.NodeID, cfg.ListenAddr, cfg.ClientAddr, len(cfg.Peers), cfg.Sync)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("[FATAL] Failed to create data-dir %s: %v", cfg.DataDir, err)
	}

	peerMgr := cluster.NewPeerManager(cfg.NodeID, cfg.Peers, cfg.ClusterToken, nil)
	transport := cluster.NewGrpcTransport(peerMgr)

	applyCh := make(chan kv.ApplyMsg, 1000)
	raftCfg := raft.Config{
		NodeID:             cfg.NodeID,
		Peers:              cfg.Peers,
		StorageDir:         cfg.DataDir,
		SyncOnWrite:        cfg.Sync,
		HeartbeatInterval:  time.Duration(cfg.HeartbeatMs) * time.Millisecond,
		ElectionTimeoutMin: time.Duration(cfg.ElectionMinMs) * time.Millisecond,
		ElectionTimeoutMax: time.Duration(cfg.ElectionMaxMs) * time.Millisecond,
		ApplyCh:            applyCh,
	}

	node, err := raft.NewRaftNode(raftCfg, transport)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Raft engine: %v", err)
	}

	applier := kv.NewApplier(nil, nil, applyCh)
	srv := server.NewServer(node, applier, cfg.Peers, cfg.ClusterToken)

	var (
		consensusSrv *grpc.Server
		clientSrv    *grpc.Server
		listenLis    net.Listener
		clientLis    net.Listener
	)

	if cfg.ListenAddr == cfg.ClientAddr {
		lis, err := net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to bind unified port %s: %v", cfg.ListenAddr, err)
		}
		consensusSrv = grpc.NewServer()
		raftlitev1.RegisterRaftServiceServer(consensusSrv, srv)
		raftlitev1.RegisterKVServiceServer(consensusSrv, srv)

		go func() {
			if err := consensusSrv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
				log.Printf("[ERROR] Unified gRPC server exited: %v", err)
			}
		}()
		log.Printf("[INFO] Unified consensus & client gRPC listening on %s", cfg.ListenAddr)
	} else {
		// Distinct ports for internal consensus and client traffic
		var err error
		listenLis, err = net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to bind consensus port %s: %v", cfg.ListenAddr, err)
		}
		consensusSrv = grpc.NewServer()
		raftlitev1.RegisterRaftServiceServer(consensusSrv, srv)

		go func() {
			if err := consensusSrv.Serve(listenLis); err != nil && err != grpc.ErrServerStopped {
				log.Printf("[ERROR] Consensus gRPC server exited: %v", err)
			}
		}()

		clientLis, err = net.Listen("tcp", cfg.ClientAddr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to bind client port %s: %v", cfg.ClientAddr, err)
		}
		clientSrv = grpc.NewServer()
		raftlitev1.RegisterKVServiceServer(clientSrv, srv)

		go func() {
			if err := clientSrv.Serve(clientLis); err != nil && err != grpc.ErrServerStopped {
				log.Printf("[ERROR] Client KV gRPC server exited: %v", err)
			}
		}()

		log.Printf("[INFO] Consensus gRPC listening on %s", cfg.ListenAddr)
		log.Printf("[INFO] Client KV gRPC listening on %s", cfg.ClientAddr)
	}

	node.Start()
	log.Printf("[INFO] RaftLite node %s operational.", cfg.NodeID)

	// Graceful shutdown handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("[INFO] Received signal %v, shutting down gracefully...", sig)

	if consensusSrv != nil {
		consensusSrv.GracefulStop()
	}
	if clientSrv != nil {
		clientSrv.GracefulStop()
	}

	peerMgr.Close()
	node.Stop()
	applier.Close()

	fmt.Println("[INFO] RaftLite shutdown complete.")
}
