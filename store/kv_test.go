package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestKVStore_BasicOperations verifies Set, Get, and Delete work correctly.
func TestKVStore_BasicOperations(t *testing.T) {
	kv := NewKVStore()

	// Test Set and Get
	kv.Set("hello", "world")
	val, ok := kv.Get("hello")
	if !ok {
		t.Fatal("expected key 'hello' to exist")
	}
	if val != "world" {
		t.Fatalf("expected 'world', got '%s'", val)
	}

	// Test Get on missing key
	_, ok = kv.Get("missing")
	if ok {
		t.Fatal("expected 'missing' key to not exist")
	}

	// Test overwrite
	kv.Set("hello", "updated")
	val, _ = kv.Get("hello")
	if val != "updated" {
		t.Fatalf("expected 'updated', got '%s'", val)
	}

	// Test Delete returns true when key exists
	deleted := kv.Delete("hello")
	if !deleted {
		t.Fatal("expected Delete to return true for existing key")
	}
	_, ok = kv.Get("hello")
	if ok {
		t.Fatal("expected key 'hello' to be gone after Delete")
	}

	// Test Delete on non-existent key returns false
	deleted = kv.Delete("nope")
	if deleted {
		t.Fatal("expected Delete to return false for non-existent key")
	}
}

// TestKVStore_Len verifies the Len() method tracks size correctly.
func TestKVStore_Len(t *testing.T) {
	kv := NewKVStore()

	if kv.Len() != 0 {
		t.Fatalf("expected empty store, got len=%d", kv.Len())
	}

	kv.Set("a", "1")
	kv.Set("b", "2")
	kv.Set("c", "3")

	if kv.Len() != 3 {
		t.Fatalf("expected len=3, got len=%d", kv.Len())
	}

	kv.Delete("b")
	if kv.Len() != 2 {
		t.Fatalf("expected len=2 after delete, got len=%d", kv.Len())
	}
}

// TestKVStore_ConcurrentAccess is the race-condition stress test.
//
// Run with: go test -race ./store/...
//
// This spawns 100 goroutines each doing 100 operations (Set, Get, Delete).
// The Go race detector will catch any data race if the mutex protection
// in KVStore is missing or incorrect.
//
// Why this matters in distributed systems:
// The gRPC server handles multiple concurrent client RPCs on different
// goroutines. Without proper locking, two simultaneous Set() calls could
// corrupt the internal map, causing panics or silent data loss.
func TestKVStore_ConcurrentAccess(t *testing.T) {
	kv := NewKVStore()

	const numGoroutines = 100
	const numOpsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				kv.Set(key, fmt.Sprintf("value-%d", j))
				kv.Get(key)
				if j%3 == 0 {
					kv.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()
	// If we get here without the race detector firing, the mutex is correct.
	t.Logf("Concurrent test completed. Store has %d keys remaining.", kv.Len())
}

// TestKVStore_SnapshotRestore verifies that Snapshot/Restore round-trips correctly.
//
// In Raft, when a follower falls too far behind the leader's log, the leader
// sends a full snapshot of the FSM state. The follower calls Restore() to
// replace its local state entirely. A bug here causes data loss after restart
// or when catching up a stale node.
func TestKVStore_SnapshotRestore(t *testing.T) {
	original := NewKVStore()
	original.Set("foo", "bar")
	original.Set("baz", "qux")
	original.Set("num", "42")

	// Take a snapshot (serializes to JSON)
	snapData, err := original.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}

	// Restore into a fresh store
	restored := NewKVStore()
	// Inject a key that should be overwritten by restore
	restored.Set("orphan", "should-disappear")

	if err := restored.Restore(bytes.NewReader(snapData)); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	// Verify all original keys survived the round-trip
	cases := []struct{ k, v string }{
		{"foo", "bar"},
		{"baz", "qux"},
		{"num", "42"},
	}
	for _, tc := range cases {
		val, ok := restored.Get(tc.k)
		if !ok {
			t.Errorf("key %q missing after restore", tc.k)
			continue
		}
		if val != tc.v {
			t.Errorf("key %q: expected %q, got %q", tc.k, tc.v, val)
		}
	}

	// The key that existed before restore should be gone (full replacement)
	_, ok := restored.Get("orphan")
	if ok {
		t.Error("'orphan' key should not survive Restore() — state should be fully replaced")
	}
}
