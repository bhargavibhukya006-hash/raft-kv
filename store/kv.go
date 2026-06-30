// Package store provides a thread-safe, in-memory key-value store.
//
// This is the foundation layer of DistKV. In a distributed system,
// every node maintains its own copy of the store. The Raft consensus
// algorithm ensures all copies stay consistent by only applying writes
// that have been committed by a quorum of nodes.
package store

import (
	"encoding/json"
	"io"
	"sync"
)

// KVStore is a thread-safe in-memory key-value store backed by a Go map.
//
// Why sync.RWMutex?
// In a distributed KV store, reads are much more frequent than writes.
// RWMutex allows multiple concurrent readers (Get operations) while
// ensuring exclusive access for writers (Set/Delete). This gives us
// better throughput than a plain sync.Mutex.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates and returns an initialized KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair in the store.
// It acquires an exclusive write lock to prevent data races.
func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

// Get retrieves the value for the given key.
// Returns (value, true) if found, ("", false) if not found.
// It acquires only a read lock, allowing concurrent reads.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

// Delete removes a key from the store.
// Returns true if the key existed and was deleted, false if it wasn't found.
// Acquires an exclusive write lock.
func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.data[key]
	if exists {
		delete(kv.data, key)
	}
	return exists
}

// Snapshot returns a snapshot of the current store state as a JSON byte slice.
//
// In Raft, snapshots allow a node to capture the current FSM state so that
// it can send it to lagging followers instead of replaying the entire log.
// This prevents log growth unbounded over time.
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// Deep copy the map to avoid holding the lock during serialization
	snapshot := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snapshot[k] = v
	}
	return json.Marshal(snapshot)
}

// Restore replaces the store's contents with data from a snapshot.
//
// Called during Raft snapshot restoration (e.g., when a new or lagging node
// needs to catch up quickly). The entire state is replaced atomically.
func (kv *KVStore) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	newData := make(map[string]string)
	if err := json.Unmarshal(data, &newData); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = newData
	return nil
}

// Len returns the number of keys currently in the store.
// Useful for metrics and testing.
func (kv *KVStore) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}
