// Package types contains shared data types used across DistKV packages.
// This avoids circular imports between the server and raft packages.
package types

// Command is the structure serialized into Raft log entries.
// Every write operation (Set, Delete) becomes a Command that is:
//   1. Serialized to JSON on the leader
//   2. Appended to the Raft log and replicated to followers
//   3. Deserialized and applied by FSM.Apply() on every node
//
// Only writes go through Raft. Reads are served directly from the KVStore.
type Command struct {
	Op    string `json:"op"`    // "set" or "delete"
	Key   string `json:"key"`
	Value string `json:"value"` // empty string for delete operations
}
