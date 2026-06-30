// Package router implements a consistent hash ring for key-to-node routing.
//
// Why consistent hashing?
//
// Naive sharding (key % numNodes) has a fatal flaw: when you add or remove a
// node, almost every key remaps to a different node, causing a "thundering
// herd" of data migration. This is catastrophic for a live cluster.
//
// Consistent hashing places both nodes and keys on a circular ring of hash
// values (0..2^32-1). A key is assigned to the first node clockwise from its
// hash position on the ring. When a node is added or removed, only the keys
// in the arc between it and its predecessor need to move — typically 1/N of
// all keys.
//
// Virtual nodes (vnodes) improve uniformity. Without them, the arcs for each
// physical node would be unequal, causing hotspots. Each physical node gets
// `replicas` virtual positions on the ring. With enough vnodes, the key
// distribution approaches uniform.
package router

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const defaultReplicas = 150 // virtual nodes per physical node

// HashRing is a consistent hash ring.
// It is safe for concurrent use by multiple goroutines.
type HashRing struct {
	mu       sync.RWMutex
	replicas int              // virtual nodes per physical node
	ring     map[uint32]string // hash → nodeID
	sorted   []uint32         // sorted list of hashes on the ring
}

// NewHashRing creates an empty hash ring.
// replicas controls how many virtual nodes each physical node gets.
// Higher values = more uniform key distribution but more memory.
func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = defaultReplicas
	}
	return &HashRing{
		replicas: replicas,
		ring:     make(map[uint32]string),
	}
}

// AddNode adds a physical node to the ring by creating `replicas` virtual nodes.
// Each virtual node is at a different position on the ring, identified by
// hashing "<nodeID>-<i>" for i in [0, replicas).
func (h *HashRing) AddNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		hash := h.hashKey(fmt.Sprintf("%s-%d", nodeID, i))
		h.ring[hash] = nodeID
		h.sorted = append(h.sorted, hash)
	}

	// Keep the ring sorted so we can binary-search for the next node clockwise.
	sort.Slice(h.sorted, func(i, j int) bool {
		return h.sorted[i] < h.sorted[j]
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
// Keys that were assigned to this node will now route to the next node clockwise.
func (h *HashRing) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Remove all virtual nodes for this physical node
	for i := 0; i < h.replicas; i++ {
		hash := h.hashKey(fmt.Sprintf("%s-%d", nodeID, i))
		delete(h.ring, hash)
	}

	// Rebuild sorted slice (filter out removed hashes)
	newSorted := h.sorted[:0]
	for _, v := range h.sorted {
		if _, ok := h.ring[v]; ok {
			newSorted = append(newSorted, v)
		}
	}
	h.sorted = newSorted
}

// GetNode returns the node responsible for the given key.
// It finds the first virtual node on the ring whose hash is ≥ the key's hash.
// If none is found (key hash > all ring entries), it wraps around to the first node.
// Returns "" if the ring is empty.
func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.sorted) == 0 {
		return ""
	}

	hash := h.hashKey(key)

	// Binary search for the first ring position ≥ key hash.
	// sort.Search returns the smallest i where sorted[i] >= hash.
	idx := sort.Search(len(h.sorted), func(i int) bool {
		return h.sorted[i] >= hash
	})

	// Wrap around: if hash is beyond the last ring entry, use the first entry.
	if idx == len(h.sorted) {
		idx = 0
	}

	return h.ring[h.sorted[idx]]
}

// Nodes returns a deduplicated list of all physical nodes currently in the ring.
func (h *HashRing) Nodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[string]struct{})
	var nodes []string
	for _, nodeID := range h.ring {
		if _, ok := seen[nodeID]; !ok {
			seen[nodeID] = struct{}{}
			nodes = append(nodes, nodeID)
		}
	}
	sort.Strings(nodes)
	return nodes
}

// hashKey computes a 32-bit hash of the given string using SHA-1.
// We use only the first 4 bytes of the SHA-1 digest for a uint32 ring position.
// SHA-1 gives good distribution across the ring.
func (h *HashRing) hashKey(key string) uint32 {
	hash := sha1.New()
	hash.Write([]byte(key))
	digest := hash.Sum(nil)
	// Take the first 4 bytes and interpret as a big-endian uint32.
	return binary.BigEndian.Uint32(digest[:4])
}
