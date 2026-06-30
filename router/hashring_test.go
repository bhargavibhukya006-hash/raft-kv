package router

import (
	"fmt"
	"testing"
)

// TestHashRing_BasicRouting verifies that a node can be added and keys route to it.
func TestHashRing_BasicRouting(t *testing.T) {
	ring := NewHashRing(10)
	ring.AddNode("node1")

	// With only one node, all keys should route there
	for _, key := range []string{"foo", "bar", "baz", "hello", "world"} {
		got := ring.GetNode(key)
		if got != "node1" {
			t.Errorf("key %q: expected node1, got %q", key, got)
		}
	}
}

// TestHashRing_EmptyRing verifies that GetNode returns "" on an empty ring.
func TestHashRing_EmptyRing(t *testing.T) {
	ring := NewHashRing(10)
	if got := ring.GetNode("any-key"); got != "" {
		t.Errorf("expected empty string for empty ring, got %q", got)
	}
}

// TestHashRing_RemapFraction is the key consistent hashing test.
//
// This test demonstrates the core property: removing one node from an N-node
// cluster should only remap ~1/N keys, not all of them.
//
// With naive modulo sharding: removing one node remaps ~(N-1)/N keys ≈ 80%
// With consistent hashing: only ~1/N keys remap ≈ 20%
//
// This is critical for live cluster operations (rolling updates, node failures)
// because it minimizes the amount of data that must be moved between shards.
func TestHashRing_RemapFraction(t *testing.T) {
	const numNodes = 5
	const numKeys = 10000
	const replicas = 150

	ring := NewHashRing(replicas)
	nodeNames := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		name := fmt.Sprintf("node%d", i+1)
		nodeNames[i] = name
		ring.AddNode(name)
	}

	// Record which node each key is assigned to before removal
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = ring.GetNode(key)
	}

	// Remove one node
	removedNode := "node1"
	ring.RemoveNode(removedNode)

	// Count how many keys remapped to a different node
	remapped := 0
	for key, oldNode := range before {
		newNode := ring.GetNode(key)
		if oldNode != removedNode && newNode != oldNode {
			// Key was NOT on the removed node but still moved — bad!
			remapped++
		}
	}

	// Keys that were on the removed node will naturally move to the next node.
	// But keys on OTHER nodes should NOT move.
	remapRate := float64(remapped) / float64(numKeys)
	t.Logf("Spurious remaps after removing 1 of %d nodes: %d/%d = %.2f%%",
		numNodes, remapped, numKeys, remapRate*100)

	// Consistent hashing guarantee: spurious remaps should be ~0%.
	// Allow 2% tolerance for hash collisions (extremely rare, but possible).
	if remapRate > 0.02 {
		t.Errorf("Too many spurious remaps: %.2f%% (expected < 2%%)", remapRate*100)
	}
}

// TestHashRing_AddNodeRemapFraction verifies that adding a node also
// only moves the expected fraction of keys.
func TestHashRing_AddNodeRemapFraction(t *testing.T) {
	const numKeys = 10000
	const replicas = 150

	ring := NewHashRing(replicas)
	ring.AddNode("nodeA")
	ring.AddNode("nodeB")
	ring.AddNode("nodeC")
	ring.AddNode("nodeD")

	// Record assignments before adding a new node
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = ring.GetNode(key)
	}

	// Add a fifth node
	ring.AddNode("nodeE")

	// Count how many keys moved
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ring.GetNode(key) != before[key] {
			moved++
		}
	}

	// Expected: ~1/5 = 20% of keys move to the new node. Allow 5–35% range.
	moveRate := float64(moved) / float64(numKeys)
	t.Logf("Keys moved after adding 5th node: %d/%d = %.1f%%", moved, numKeys, moveRate*100)

	if moveRate < 0.05 || moveRate > 0.40 {
		t.Errorf("Unexpected move rate %.1f%% (expected ~20%%)", moveRate*100)
	}
}

// TestHashRing_ConcurrentAccess verifies the ring is safe for concurrent use.
func TestHashRing_ConcurrentAccess(t *testing.T) {
	ring := NewHashRing(50)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	done := make(chan struct{})

	// Writer: add and remove nodes concurrently
	go func() {
		for i := 0; i < 100; i++ {
			ring.AddNode(fmt.Sprintf("temp-%d", i))
			ring.RemoveNode(fmt.Sprintf("temp-%d", i))
		}
		close(done)
	}()

	// Readers: lookup keys concurrently
	for r := 0; r < 10; r++ {
		go func() {
			for i := 0; i < 500; i++ {
				ring.GetNode(fmt.Sprintf("key-%d", i))
			}
		}()
	}

	<-done
}
