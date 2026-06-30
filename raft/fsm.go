// Package raft provides the Finite State Machine (FSM) that bridges
// the Raft consensus layer with the underlying KVStore.
//
// How Raft + FSM work together:
//
//  1. A client sends a Set("foo","bar") to the leader's gRPC server.
//  2. The gRPC handler serializes {op:"set",key:"foo",value:"bar"} → JSON.
//  3. It calls raft.Apply(jsonBytes, timeout) — the Raft library:
//     a. Appends the entry to the leader's log.
//     b. Replicates the entry to follower nodes via AppendEntries RPCs.
//     c. Once a majority (quorum) acknowledges, the entry is "committed".
//  4. Raft calls FSM.Apply(logEntry) on every node (leader + followers).
//  5. Our Apply() decodes the JSON command and calls KVStore.Set/Delete.
//  6. Now all nodes have the same state — consistency achieved!
//
// Snapshot / Restore:
// As the Raft log grows, replaying thousands of entries to bring a new
// node up to date becomes expensive. Snapshots capture the full FSM
// state at a point in time. Raft can then send just the snapshot + any
// new entries since the snapshot, dramatically reducing catch-up time.
package raft

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/distkv/store"
	"github.com/distkv/types"
)

// KVFsm is the FSM (Finite State Machine) that Raft drives.
// Every committed log entry is delivered to Apply() in order.
// The FSM must be deterministic: given the same sequence of log entries,
// every node must reach the same state.
type KVFsm struct {
	store *store.KVStore
}

// NewKVFsm creates an FSM backed by the given KVStore.
func NewKVFsm(s *store.KVStore) *KVFsm {
	return &KVFsm{store: s}
}

// Apply is called by Raft after a log entry has been committed.
// A log entry is committed when a quorum of nodes has durably written it.
//
// CRITICAL REQUIREMENT: Apply must be deterministic and side-effect free
// beyond mutating local state. The same log entry, applied on any node in
// any order relative to network timing, must always produce the same result.
func (f *KVFsm) Apply(log *raft.Log) interface{} {
	// Only command-type logs carry our payload.
	// Raft also generates barrier, config-change, etc. log types that we ignore.
	if log.Type != raft.LogCommand {
		return nil
	}

	var cmd types.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		// A malformed entry is a programming error, not a runtime one.
		// We return the error but do NOT crash — Raft would be in a bad state.
		return fmt.Errorf("fsm apply: unmarshal: %w", err)
	}

	switch cmd.Op {
	case "set":
		f.store.Set(cmd.Key, cmd.Value)
	case "delete":
		f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("fsm apply: unknown op %q", cmd.Op)
	}

	return nil
}

// Snapshot returns a snapshot of the FSM state.
//
// Raft calls this periodically (controlled by raft.Config.SnapshotInterval
// and raft.Config.SnapshotThreshold). The returned fsmSnapshot is passed
// to Persist(), which writes it to stable storage (disk). After the snapshot
// is persisted, Raft truncates old log entries that are now covered by it.
func (f *KVFsm) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("fsm snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore is called by Raft when a node needs to catch up from a snapshot
// (e.g., a new node joining, or a crashed node coming back after missing
// many entries). It completely replaces the current FSM state.
func (f *KVFsm) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	if err := f.store.Restore(snapshot); err != nil {
		return fmt.Errorf("fsm restore: %w", err)
	}
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot. Persist() is called by Raft
// to write the snapshot bytes to the snapshot sink (disk / boltdb).
type fsmSnapshot struct {
	data []byte
}

// Persist writes the snapshot data to the provided sink.
// The sink is provided by the raft library and handles durable storage.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(s.data); err != nil {
			return fmt.Errorf("snapshot persist write: %w", err)
		}
		return sink.Close()
	}()
	if err != nil {
		// Cancel tells Raft the snapshot failed; it will try again later.
		sink.Cancel()
		return err
	}
	return nil
}

// Release is called by Raft when the snapshot is no longer needed.
// Nothing to do here since our data is just a byte slice.
func (s *fsmSnapshot) Release() {}
