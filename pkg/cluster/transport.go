package cluster

import (
	"context"

	"github.com/raftlite/pkg/chaos"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"google.golang.org/grpc/metadata"
)

// GrpcTransport implements the raft.Transport interface over gRPC.
type GrpcTransport struct {
	pm *PeerManager
}

// NewGrpcTransport creates a gRPC transport wrapping a PeerManager.
func NewGrpcTransport(pm *PeerManager) *GrpcTransport {
	return &GrpcTransport{pm: pm}
}

func (t *GrpcTransport) withMetadata(ctx context.Context, targetPeer string) context.Context {
	md := metadata.Pairs(
		chaos.TargetPeerHeader, targetPeer,
		chaos.SourcePeerHeader, t.pm.localNodeID,
	)
	if t.pm.clusterToken != "" {
		md.Set(ClusterTokenHeader, t.pm.clusterToken)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// SendRequestVote sends a RequestVote RPC to a cluster peer.
func (t *GrpcTransport) SendRequestVote(ctx context.Context, targetPeer string, req *raftlitev1.RequestVoteRequest) (*raftlitev1.RequestVoteResponse, error) {
	client, err := t.pm.GetClient(ctx, targetPeer)
	if err != nil {
		return nil, err
	}
	callCtx := t.withMetadata(ctx, targetPeer)
	return client.RequestVote(callCtx, req)
}

// SendAppendEntries sends an AppendEntries RPC to a cluster peer.
func (t *GrpcTransport) SendAppendEntries(ctx context.Context, targetPeer string, req *raftlitev1.AppendEntriesRequest) (*raftlitev1.AppendEntriesResponse, error) {
	client, err := t.pm.GetClient(ctx, targetPeer)
	if err != nil {
		return nil, err
	}
	callCtx := t.withMetadata(ctx, targetPeer)
	return client.AppendEntries(callCtx, req)
}
