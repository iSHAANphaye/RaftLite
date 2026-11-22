package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/raftlite/pkg/cluster"
	"github.com/raftlite/pkg/kv"
	"github.com/raftlite/pkg/raft"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server implements both RaftServiceServer (internal consensus) and KVServiceServer (client API).
type Server struct {
	raftlitev1.UnimplementedRaftServiceServer
	raftlitev1.UnimplementedKVServiceServer

	node         *raft.RaftNode
	applier      *kv.Applier
	peerAddrs    map[string]string
	clusterToken string
}

// NewServer creates a new unified consensus and key-value gRPC server instance.
func NewServer(
	node *raft.RaftNode,
	applier *kv.Applier,
	peerAddrs map[string]string,
	clusterToken string,
) *Server {
	return &Server{
		node:         node,
		applier:      applier,
		peerAddrs:    peerAddrs,
		clusterToken: clusterToken,
	}
}

func (s *Server) verifyToken(ctx context.Context) error {
	if s.clusterToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get(cluster.ClusterTokenHeader)
	if len(tokens) == 0 || tokens[0] != s.clusterToken {
		return status.Errorf(codes.Unauthenticated, "invalid cluster token")
	}
	return nil
}

// ==========================================
// RaftService Consensus RPCs
// ==========================================

func (s *Server) RequestVote(ctx context.Context, req *raftlitev1.RequestVoteRequest) (*raftlitev1.RequestVoteResponse, error) {
	if err := s.verifyToken(ctx); err != nil {
		return nil, err
	}
	return s.node.HandleRequestVote(req)
}

func (s *Server) AppendEntries(ctx context.Context, req *raftlitev1.AppendEntriesRequest) (*raftlitev1.AppendEntriesResponse, error) {
	if err := s.verifyToken(ctx); err != nil {
		return nil, err
	}
	return s.node.HandleAppendEntries(req)
}

// ==========================================
// KVService Client RPCs
// ==========================================

func (s *Server) ClientPut(ctx context.Context, req *raftlitev1.ClientPutRequest) (*raftlitev1.ClientPutResponse, error) {
	if !s.node.IsLeader() {
		return &raftlitev1.ClientPutResponse{
			Success:    false,
			Error:      "not leader",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "not leader (hint: %s)", s.node.GetLeaderHint())
	}

	cmd := &kv.Command{
		Op:          kv.OpPut,
		Key:         req.Key,
		Value:       req.Value,
		ClientID:    req.ClientId,
		SequenceNum: req.SequenceNum,
	}

	data, err := kv.EncodeCommand(cmd)
	if err != nil {
		return &raftlitev1.ClientPutResponse{Success: false, Error: err.Error()}, status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
	}

	index, _, isLeader := s.node.Propose(data, raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND)
	if !isLeader {
		return &raftlitev1.ClientPutResponse{
			Success:    false,
			Error:      "lost leadership",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "lost leadership")
	}

	waitCh := s.applier.Register(index)
	defer s.applier.Deregister(index)

	select {
	case <-ctx.Done():
		return &raftlitev1.ClientPutResponse{Success: false, Error: ctx.Err().Error()}, status.Errorf(codes.DeadlineExceeded, "operation timed out")
	case respBytes, ok := <-waitCh:
		if !ok {
			return &raftlitev1.ClientPutResponse{Success: false, Error: "server closed"}, status.Errorf(codes.Unavailable, "server closed")
		}
		var resp kv.ClientPutResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return &raftlitev1.ClientPutResponse{Success: false, Error: err.Error()}, nil
		}
		return &raftlitev1.ClientPutResponse{
			Success:    resp.Success,
			Error:      resp.Error,
			LeaderHint: resp.LeaderHint,
		}, nil
	}
}

func (s *Server) ClientGet(ctx context.Context, req *raftlitev1.ClientGetRequest) (*raftlitev1.ClientGetResponse, error) {
	if req.Linearizable {
		if !s.node.IsLeader() {
			return &raftlitev1.ClientGetResponse{
				Found:      false,
				Error:      "not leader",
				LeaderHint: s.node.GetLeaderHint(),
			}, status.Errorf(codes.Unavailable, "not leader (hint: %s)", s.node.GetLeaderHint())
		}

		readIndex, err := s.node.ReadIndex(ctx)
		if err != nil {
			return &raftlitev1.ClientGetResponse{
				Found:      false,
				Error:      err.Error(),
				LeaderHint: s.node.GetLeaderHint(),
			}, status.Errorf(codes.Unavailable, "read index failed: %v", err)
		}

		// Await state machine application up to readIndex
		for {
			if s.applier.LastApplied() >= readIndex {
				break
			}
			select {
			case <-ctx.Done():
				return &raftlitev1.ClientGetResponse{Found: false, Error: ctx.Err().Error()}, status.Errorf(codes.DeadlineExceeded, "timed out awaiting read index")
			case <-time.After(2 * time.Millisecond):
			}
		}
	}

	val, found := s.applier.Engine().Get(req.Key)
	return &raftlitev1.ClientGetResponse{
		Found: found,
		Value: val,
	}, nil
}

func (s *Server) ClientCAS(ctx context.Context, req *raftlitev1.ClientCASRequest) (*raftlitev1.ClientCASResponse, error) {
	if !s.node.IsLeader() {
		return &raftlitev1.ClientCASResponse{
			Success:    false,
			Error:      "not leader",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "not leader (hint: %s)", s.node.GetLeaderHint())
	}

	cmd := &kv.Command{
		Op:            kv.OpCAS,
		Key:           req.Key,
		ExpectedValue: req.ExpectedValue,
		Value:         req.NewValue,
		ClientID:      req.ClientId,
		SequenceNum:   req.SequenceNum,
	}

	data, err := kv.EncodeCommand(cmd)
	if err != nil {
		return &raftlitev1.ClientCASResponse{Success: false, Error: err.Error()}, status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
	}

	index, _, isLeader := s.node.Propose(data, raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND)
	if !isLeader {
		return &raftlitev1.ClientCASResponse{
			Success:    false,
			Error:      "lost leadership",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "lost leadership")
	}

	waitCh := s.applier.Register(index)
	defer s.applier.Deregister(index)

	select {
	case <-ctx.Done():
		return &raftlitev1.ClientCASResponse{Success: false, Error: ctx.Err().Error()}, status.Errorf(codes.DeadlineExceeded, "operation timed out")
	case respBytes, ok := <-waitCh:
		if !ok {
			return &raftlitev1.ClientCASResponse{Success: false, Error: "server closed"}, status.Errorf(codes.Unavailable, "server closed")
		}
		var resp kv.ClientCASResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return &raftlitev1.ClientCASResponse{Success: false, Error: err.Error()}, nil
		}
		return &raftlitev1.ClientCASResponse{
			Success:      resp.Success,
			Swapped:      resp.Swapped,
			CurrentValue: resp.CurrentValue,
			Error:        resp.Error,
			LeaderHint:   resp.LeaderHint,
		}, nil
	}
}

func (s *Server) ClientDelete(ctx context.Context, req *raftlitev1.ClientDeleteRequest) (*raftlitev1.ClientDeleteResponse, error) {
	if !s.node.IsLeader() {
		return &raftlitev1.ClientDeleteResponse{
			Success:    false,
			Error:      "not leader",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "not leader (hint: %s)", s.node.GetLeaderHint())
	}

	cmd := &kv.Command{
		Op:          kv.OpDelete,
		Key:         req.Key,
		ClientID:    req.ClientId,
		SequenceNum: req.SequenceNum,
	}

	data, err := kv.EncodeCommand(cmd)
	if err != nil {
		return &raftlitev1.ClientDeleteResponse{Success: false, Error: err.Error()}, status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
	}

	index, _, isLeader := s.node.Propose(data, raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND)
	if !isLeader {
		return &raftlitev1.ClientDeleteResponse{
			Success:    false,
			Error:      "lost leadership",
			LeaderHint: s.node.GetLeaderHint(),
		}, status.Errorf(codes.Unavailable, "lost leadership")
	}

	waitCh := s.applier.Register(index)
	defer s.applier.Deregister(index)

	select {
	case <-ctx.Done():
		return &raftlitev1.ClientDeleteResponse{Success: false, Error: ctx.Err().Error()}, status.Errorf(codes.DeadlineExceeded, "operation timed out")
	case respBytes, ok := <-waitCh:
		if !ok {
			return &raftlitev1.ClientDeleteResponse{Success: false, Error: "server closed"}, status.Errorf(codes.Unavailable, "server closed")
		}
		var resp kv.ClientDeleteResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return &raftlitev1.ClientDeleteResponse{Success: false, Error: err.Error()}, nil
		}
		return &raftlitev1.ClientDeleteResponse{
			Success:    resp.Success,
			Found:      resp.Found,
			Error:      resp.Error,
			LeaderHint: resp.LeaderHint,
		}, nil
	}
}

func (s *Server) ClusterStatus(ctx context.Context, req *raftlitev1.ClusterStatusRequest) (*raftlitev1.ClusterStatusResponse, error) {
	nodeID, role, term, commitIndex, lastApplied, leaderID := s.node.GetStatus()
	peerMap := s.node.GetPeersStatus()

	peerStatuses := make([]*raftlitev1.PeerStatus, 0, len(peerMap))
	for pid, info := range peerMap {
		addr := ""
		if s.peerAddrs != nil {
			addr = s.peerAddrs[pid]
		}
		peerStatuses = append(peerStatuses, &raftlitev1.PeerStatus{
			PeerId:     pid,
			Address:    addr,
			MatchIndex: info.MatchIndex,
			NextIndex:  info.NextIndex,
			Reachable:  true,
		})
	}

	return &raftlitev1.ClusterStatusResponse{
		NodeId:      nodeID,
		Role:        string(role),
		CurrentTerm: term,
		CommitIndex: commitIndex,
		LastApplied: lastApplied,
		LeaderId:    leaderID,
		Peers:       peerStatuses,
	}, nil
}
