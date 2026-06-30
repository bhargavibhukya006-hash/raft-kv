// Package raft provides Raft consensus setup for DistKV.
//
// Raft overview (simplified):
//
// Raft solves distributed consensus: how do multiple servers agree on a
// sequence of values even when some servers crash or messages are delayed?
//
// Key concepts:
//   - Leader election: nodes elect one leader per "term". Only the leader
//     can accept writes. If the leader crashes, a new election starts.
//   - Log replication: the leader appends entries to its log and replicates
//     them to followers. Once a majority (quorum) acknowledges an entry,
//     it is "committed" and applied to the state machine.
//   - Safety: a committed entry is never lost, even if the leader crashes
//     immediately after committing, because the quorum that acknowledged it
//     will include future leaders (by the Raft vote constraint).
//
// This file sets up:
//   - TCP transport for Raft RPC (leader election, log replication)
//   - BoltDB-backed stable log store (entries survive node restarts)
//   - File-based snapshot store
//   - Cluster bootstrap or join logic
package raft

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/distkv/store"
)

// Config holds everything needed to start a Raft node.
type Config struct {
	// NodeID uniquely identifies this node in the cluster.
	// Must be stable across restarts. Use a UUID or hostname.
	NodeID string

	// RaftBindAddr is the TCP address Raft listens on for peer communication.
	// Example: "0.0.0.0:7000"
	RaftBindAddr string

	// DataDir is the directory where Raft stores its log, stable store, and snapshots.
	DataDir string

	// Peers is a list of "nodeID=raftAddr" strings for the initial cluster members.
	// Example: ["node1=127.0.0.1:7001", "node2=127.0.0.1:7002"]
	// Used only for the initial bootstrap. Once bootstrapped, peers are stored in
	// Raft's own configuration store.
	Peers []string

	// Bootstrap should be true on the very first startup of a fresh cluster.
	// After the first run, Raft restores its configuration from disk,
	// so Bootstrap has no effect on restarts.
	Bootstrap bool
}

// SetupRaft initializes and starts a Raft node.
// Returns the raft.Raft instance that the gRPC server uses to apply writes.
func SetupRaft(cfg Config, kv *store.KVStore) (*raft.Raft, error) {
	// --- Raft configuration ---
	// raft.DefaultConfig() provides sensible production-ready defaults:
	//   HeartbeatTimeout: 1s, ElectionTimeout: 1s, CommitTimeout: 50ms
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	// Reduce timeouts for faster testing (NOT for production)
	raftCfg.HeartbeatTimeout = 500 * time.Millisecond
	raftCfg.ElectionTimeout = 500 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 400 * time.Millisecond
	raftCfg.CommitTimeout = 100 * time.Millisecond

	// --- Data directory setup ---
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	// --- TCP Transport ---
	// Raft nodes communicate with each other via TCP RPCs.
	// StreamLayer provides the low-level TCP connection.
	// AdvertiseAddr is what other nodes use to reach us (can differ from bind
	// if behind NAT — important in Docker/Kubernetes deployments).
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve tcp addr: %w", err)
	}

	// MaxPool controls how many idle connections to keep per peer.
	// Timeout is per connection attempt.
	transport, err := raft.NewTCPTransport(cfg.RaftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft tcp transport: %w", err)
	}

	// --- Snapshot store ---
	// Snapshots are stored on disk in DataDir/snapshots/.
	// retain=2: keep the 2 most recent snapshots.
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft snapshot store: %w", err)
	}

	// --- BoltDB log and stable store ---
	// BoltDB (via raft-boltdb) provides a durable, embedded key-value store
	// for Raft's own log and metadata (current term, last vote).
	// This ensures Raft's safety properties survive process restarts.
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("raft boltdb: %w", err)
	}
	logStore := boltDB    // stores log entries
	stableStore := boltDB // stores term, last vote

	// --- FSM ---
	// The FSM is the "application" that Raft drives.
	// It receives committed log entries in order and applies them.
	fsm := NewKVFsm(kv)

	// --- Create the Raft node ---
	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("raft new: %w", err)
	}

	// --- Bootstrap the cluster ---
	// Bootstrap only happens once: when a cluster starts from scratch.
	// It creates the initial Raft configuration listing all servers.
	//
	// After bootstrap, Raft persists its own configuration in the stable store
	// and uses that on subsequent restarts. Re-bootstrapping an existing cluster
	// is dangerous and can violate safety guarantees.
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				// This node always includes itself
				Suffrage: raft.Voter,
				ID:       raft.ServerID(cfg.NodeID),
				Address:  raft.ServerAddress(cfg.RaftBindAddr),
			},
		}

		// Add peer nodes to the initial configuration
		for _, peer := range cfg.Peers {
			parts := strings.SplitN(peer, "=", 2)
			if len(parts) != 2 {
				continue
			}
			peerID, peerAddr := parts[0], parts[1]
			if peerID == cfg.NodeID {
				continue // already added above
			}
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(peerID),
				Address:  raft.ServerAddress(peerAddr),
			})
		}

		clusterConfig := raft.Configuration{Servers: servers}
		// BootstrapCluster is idempotent: if the cluster is already
		// bootstrapped (has a log), this is a no-op.
		if fut := r.BootstrapCluster(clusterConfig); fut.Error() != nil {
			// ErrCantBootstrap means already bootstrapped — this is fine.
			if fut.Error() != raft.ErrCantBootstrap {
				return nil, fmt.Errorf("raft bootstrap: %w", fut.Error())
			}
		}
	}

	return r, nil
}
