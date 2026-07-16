package main

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashRing implements consistent hashing to route documents to shards.
// Each physical shard is mapped to multiple virtual nodes (replicas) on
// the ring for better distribution.
type HashRing struct {
	mu       sync.RWMutex
	replicas int            // virtual nodes per physical shard
	keys     []int          // sorted hash values on the ring
	ring     map[int]string // hash → shard address
}

// NewHashRing creates a consistent hash ring with the given number of
// virtual replicas per node. More replicas = better distribution.
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		ring:     make(map[int]string),
	}
}

// hashKey returns a deterministic uint32 hash for a given string.
func hashKey(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}

// Update rebuilds the ring from the current set of shards. This is called
// whenever the shard registry changes (shards join or leave).
func (h *HashRing) Update(shards []ShardInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.keys = nil
	h.ring = make(map[int]string, len(shards)*h.replicas)

	for _, s := range shards {
		for i := 0; i < h.replicas; i++ {
			hash := hashKey(s.Addr + "#" + strconv.Itoa(i))
			h.keys = append(h.keys, hash)
			h.ring[hash] = s.Addr
		}
	}

	sort.Ints(h.keys)
}

// GetShard returns the shard address responsible for the given document ID.
// Uses binary search to find the first ring position >= hash(docID).
// Returns empty string if the ring is empty.
func (h *HashRing) GetShard(docID string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.keys) == 0 {
		return ""
	}

	hash := hashKey(docID)
	idx := sort.SearchInts(h.keys, hash)
	if idx >= len(h.keys) {
		idx = 0 // wrap around the ring
	}

	return h.ring[h.keys[idx]]
}
