// Package server implements the gRPC server for DistKV.
//
// Architecture:
//   - GRPCServer wraps both the KVStore (for direct reads) and the Raft node (for writes).
//   - Writes go through Raft.Apply() → committed to log → FSM.Apply() → KVStore.Set()
//   - Reads go directly to KVStore on the leader (strong consistency default)
//
// The leader_hint pattern: instead of a separate load balancer or proxy,
// any node can tell a client "I'm not the leader, go talk to <addr>".
// This lets clients self-route without needing a coordinator service.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/distkv/proto"
	"github.com/distkv/store"
	"github.com/distkv/types"
	"google.golang.org/grpc"
)

// GRPCServer implements the KV gRPC service.
// It holds a reference to the Raft node (optional — nil means standalone mode)
// and to the underlying KVStore for direct reads.
type GRPCServer struct {
	pb.UnimplementedKVServer

	store    *store.KVStore
	raftNode *raft.Raft // nil in standalone (no-Raft) mode
	grpcAddr string     // our own address, for clients who need to find us
}

// NewGRPCServer creates a GRPCServer.
// Pass nil for raftNode to run in standalone mode (no consensus).
func NewGRPCServer(kv *store.KVStore, r *raft.Raft, grpcAddr string) *GRPCServer {
	return &GRPCServer{
		store:    kv,
		raftNode: r,
		grpcAddr: grpcAddr,
	}
}

// Set handles a Set RPC.
//
// Raft write path:
// 1. Check if we are the leader (only leaders can accept writes).
// 2. Serialize the command into bytes (the "log entry payload").
// 3. Call raft.Apply() — this replicates the entry to a majority of nodes.
//    Once a quorum acknowledges, the entry is "committed".
// 4. After Apply() returns successfully, the FSM has already applied it
//    to the KVStore on the leader. Followers apply it asynchronously.
func (s *GRPCServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if s.raftNode != nil {
		// --- Raft mode ---
		// Check leadership. Only the leader may propose log entries.
		// Non-leaders return a hint so smart clients can retry directly.
		if s.raftNode.State() != raft.Leader {
			leaderAddr, _ := s.raftNode.LeaderWithID()
			return &pb.SetResponse{
				Success:    false,
				LeaderHint: string(leaderAddr),
			}, nil
		}

		// Serialize command → JSON bytes for the Raft log
		cmd := types.Command{Op: "set", Key: req.Key, Value: req.Value}
		data, err := json.Marshal(cmd)
		if err != nil {
			return &pb.SetResponse{Success: false, Error: fmt.Sprintf("marshal: %v", err)}, nil
		}

		// raft.Apply() blocks until the entry is committed by a quorum.
		// Timeout prevents a stalled cluster from hanging the client forever.
		// ApplyFuture.Error() returns ErrNotLeader, ErrLeadershipLost, etc.
		future := s.raftNode.Apply(data, 5*time.Second)
		if err := future.Error(); err != nil {
			return &pb.SetResponse{Success: false, Error: fmt.Sprintf("raft apply: %v", err)}, nil
		}
		return &pb.SetResponse{Success: true}, nil
	}

	// --- Standalone mode (no Raft) ---
	s.store.Set(req.Key, req.Value)
	return &pb.SetResponse{Success: true}, nil
}

// Get handles a Get RPC.
//
// For strong consistency, we only serve reads from the leader.
// This prevents a stale follower (that hasn't applied all log entries yet)
// from returning an outdated value — the "stale read" problem.
//
// Trade-off: leader-only reads reduce read throughput because all reads
// hit one node. Alternative: "ReadIndex" protocol where the leader confirms
// it is still the leader before serving the read (allows eventually-consistent
// follower reads while preventing stale reads on the leader's reads).
func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.raftNode != nil {
		// Strong consistency: only the leader may serve reads
		if s.raftNode.State() != raft.Leader {
			leaderAddr, _ := s.raftNode.LeaderWithID()
			return &pb.GetResponse{
				Found:      false,
				LeaderHint: string(leaderAddr),
			}, nil
		}
	}

	val, ok := s.store.Get(req.Key)
	return &pb.GetResponse{Value: val, Found: ok}, nil
}

// Delete handles a Delete RPC.
// Same Raft routing logic as Set.
func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.raftNode != nil {
		if s.raftNode.State() != raft.Leader {
			leaderAddr, _ := s.raftNode.LeaderWithID()
			return &pb.DeleteResponse{
				Success:    false,
				LeaderHint: string(leaderAddr),
			}, nil
		}

		cmd := types.Command{Op: "delete", Key: req.Key}
		data, err := json.Marshal(cmd)
		if err != nil {
			return &pb.DeleteResponse{Success: false, Error: fmt.Sprintf("marshal: %v", err)}, nil
		}

		future := s.raftNode.Apply(data, 5*time.Second)
		if err := future.Error(); err != nil {
			return &pb.DeleteResponse{Success: false, Error: fmt.Sprintf("raft apply: %v", err)}, nil
		}
		return &pb.DeleteResponse{Success: true}, nil
	}

	// Standalone mode
	deleted := s.store.Delete(req.Key)
	return &pb.DeleteResponse{Success: deleted}, nil
}

// Serve starts the gRPC listener on the given address and blocks until ctx is done.
func Serve(ctx context.Context, addr string, srv *GRPCServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterKVServer(grpcSrv, srv)

	// Shutdown goroutine: when context is cancelled, stop the gRPC server.
	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()

	return grpcSrv.Serve(lis)
}
