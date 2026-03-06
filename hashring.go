package queuering

import (
	"fmt"
	"sort"
)

// HashFunc is a hash function type that returns a 32-bit hash.
type HashFunc func([]byte) uint32

// xxHash32Wrapper is a wrapper for xxh32.
func xxHash32Wrapper(data []byte) uint32 {
	return xxh32(data)
}

// murmurHash32Wrapper is a wrapper for sum32.
func murmurHash32Wrapper(data []byte) uint32 {
	return sum32(data)
}

// HashRingOption is an option function for HashRing.
type HashRingOption func(*HashRing)

// WithHashFunc sets the hash function for HashRing.
func WithHashFunc(hashFunc HashFunc) HashRingOption {
	return func(h *HashRing) {
		h.hashFunc = hashFunc
	}
}

// WithXXHash uses xxHash32 (default).
func WithXXHash() HashRingOption {
	return WithHashFunc(xxHash32Wrapper)
}

// WithMurmurHash uses MurmurHash32.
func WithMurmurHash() HashRingOption {
	return WithHashFunc(murmurHash32Wrapper)
}

// HashRing implements consistent hashing for distributing queues across workers.
type HashRing struct {
	nodes        map[string]bool
	hashFunc     HashFunc
	totalQueues  int
	virtualNodes int
}

// NewHashRing creates a new ring with the specified number of queues and virtual nodes.
// xxHash32 is used by default.
// Returns an error if parameters are invalid.
func NewHashRing(totalQueues, virtualNodes int, opts ...HashRingOption) (*HashRing, error) {
	if totalQueues < 1 {
		return nil, fmt.Errorf("totalQueues must be at least 1, got %d", totalQueues)
	}
	if virtualNodes < 1 {
		return nil, fmt.Errorf("virtualNodes must be at least 1, got %d", virtualNodes)
	}

	h := &HashRing{
		totalQueues:  totalQueues,
		virtualNodes: virtualNodes,
		nodes:        make(map[string]bool),
		hashFunc:     xxHash32Wrapper, // default: xxHash32
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

// AddNode adds a node to the ring and returns the new queue distribution.
func (h *HashRing) AddNode(nodeName string) map[string][]int {
	h.nodes[nodeName] = true
	return h.computeDistribution()
}

// RemoveNode removes a node from the ring and returns the new queue distribution.
func (h *HashRing) RemoveNode(nodeName string) map[string][]int {
	delete(h.nodes, nodeName)
	return h.computeDistribution()
}

// GetDistribution returns the current queue distribution.
func (h *HashRing) GetDistribution() map[string][]int {
	return h.computeDistribution()
}

// String returns a string representation of the HashRing.
func (h *HashRing) String() string {
	return fmt.Sprintf("HashRing{nodes=%d, queues=%d, vnodes=%d}",
		len(h.nodes), h.totalQueues, h.virtualNodes)
}

// computeDistribution calculates the queue distribution across nodes using consistent hashing.
func (h *HashRing) computeDistribution() map[string][]int {
	result := make(map[string][]int)

	// If no nodes, return empty distribution
	if len(h.nodes) == 0 {
		return result
	}

	// Collect list of nodes
	nodes := make([]string, 0, len(h.nodes))
	for node := range h.nodes {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	// Build the ring: hash -> nodeName
	ring := make([]ringPoint, 0, len(nodes)*h.virtualNodes)

	for _, node := range nodes {
		for i := 0; i < h.virtualNodes; i++ {
			hashVal := h.hashFn(node, i)
			ring = append(ring, ringPoint{hash: hashVal, nodeName: node})
		}
	}

	// Sort the ring by hash
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hash < ring[j].hash
	})

	// Find owner for each queue
	for queueNum := 0; queueNum < h.totalQueues; queueNum++ {
		queueHash := h.queueHash(queueNum)
		owner := h.findOwner(ring, queueHash)
		result[owner] = append(result[owner], queueNum)
	}

	return result
}

// ringPoint is a point in the hash ring.
type ringPoint struct {
	nodeName string
	hash     uint32
}

// buildKey builds a hash key from a prefix and integer value.
// This is a helper function to eliminate code duplication.
func buildKey(prefix string, value int) []byte {
	// Estimate key size: prefix + ':' + up to 10 digits for int
	key := make([]byte, 0, len(prefix)+1+10)
	key = append(key, []byte(prefix)...)
	key = append(key, ':')
	key = append(key, []byte(fmt.Sprintf("%d", value))...)
	return key
}

// hashFn computes the hash for a virtual node using the configured hash function.
func (h *HashRing) hashFn(nodeName string, index int) uint32 {
	return h.hashFunc(buildKey(nodeName, index))
}

// queueHash computes the hash for a queue using the configured hash function.
func (h *HashRing) queueHash(queueNum int) uint32 {
	return h.hashFunc(buildKey("queue", queueNum))
}

// findOwner finds the queue owner in the sorted ring.
func (h *HashRing) findOwner(ring []ringPoint, queueHash uint32) string {
	// Binary search for first ring node with hash >= queueHash
	idx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= queueHash
	})

	if idx >= len(ring) {
		// Wrap around - take first node
		return ring[0].nodeName
	}

	return ring[idx].nodeName
}
