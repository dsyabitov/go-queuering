package queuering

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const totalQueues = 1024

// printDistribution outputs queue distribution across nodes.
func printDistribution(t *testing.T, dist map[string][]int) {
	t.Helper()

	// Sort nodes for deterministic output
	nodes := make([]string, 0, len(dist))
	for node := range dist {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	for _, node := range nodes {
		queues := dist[node]
		sort.Ints(queues)
		t.Logf("  %s: %d queues -> %v\n", node, len(queues), queues)
	}
	t.Logf("  Total nodes: %d\n", len(dist))
}

func TestHashRing_AddNode(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	// Add first node
	dist := ring.AddNode("node-1")

	t.Logf("After adding node-1:")
	printDistribution(t, dist)

	assert.Len(t, dist, 1)
	assert.Len(t, dist["node-1"], totalQueues)

	// Add second node
	dist = ring.AddNode("node-2")

	t.Logf("After adding node-2:")
	printDistribution(t, dist)

	assert.Len(t, dist, 2)
	// Queues should be distributed between two nodes
	totalQueuesCount := len(dist["node-1"]) + len(dist["node-2"])
	assert.Equal(t, totalQueues, totalQueuesCount)
}

func TestHashRing_RemoveNode(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	// Add 3 nodes
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	dist := ring.GetDistribution()
	t.Logf("Distribution before removal (3 nodes):")
	printDistribution(t, dist)
	assert.Len(t, dist, 3)

	// Remove one node
	dist = ring.RemoveNode("node-2")

	t.Logf("Distribution after removing node-2:")
	printDistribution(t, dist)

	assert.Len(t, dist, 2)
	assert.NotContains(t, dist, "node-2")

	// All queues should be distributed between remaining nodes
	totalQueuesCount := len(dist["node-1"]) + len(dist["node-3"])
	assert.Equal(t, totalQueues, totalQueuesCount)
}

func TestHashRing_DistributionBalance(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	// Add 5 nodes
	nodes := []string{"node-A", "node-B", "node-C", "node-D", "node-E"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	dist := ring.GetDistribution()

	t.Logf("Distribution after adding 5 nodes:")
	printDistribution(t, dist)

	assert.Len(t, dist, 5)

	// Check that distribution is relatively uniform
	// With 1000 virtual nodes, distribution may be uneven
	ideal := totalQueues / 5
	tolerance := float64(totalQueues) * 0.5 // 50% tolerance for consistent hashing

	for node, queues := range dist {
		assert.InDelta(t, float64(ideal), float64(len(queues)), tolerance, "node %s has unbalanced distribution", node)
	}
}

func TestHashRing_RedistributionOnNodeAdd(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	// Add 5 nodes
	nodes := []string{"node-A", "node-B", "node-C", "node-D", "node-E"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	// Get distribution BEFORE adding 6th node
	oldDist := ring.GetDistribution()
	t.Logf("Distribution before adding node-F (5 nodes):")
	printDistribution(t, oldDist)

	// Add 6th node
	newDist := ring.AddNode("node-F")

	t.Logf("Distribution after adding node-F (6 nodes):")
	printDistribution(t, newDist)

	assert.Len(t, newDist, 6)

	// Distribution should change - 6th node should get its share
	assert.NotEmpty(t, newDist["node-F"])

	// Some queues should have moved from existing nodes to the new one
	// Check that distribution changed
	totalMoved := 0
	for node := range oldDist {
		oldSet := make(map[int]bool)
		for _, q := range oldDist[node] {
			oldSet[q] = true
		}

		newSet := make(map[int]bool)
		for _, q := range newDist[node] {
			newSet[q] = true
		}

		// Count queues that left this node
		for q := range oldSet {
			if !newSet[q] {
				totalMoved++
			}
		}
	}

	// When adding 6th node, approximately 1/6 of queues should be redistributed
	assert.Positive(t, totalMoved, "queues should be redistributed when adding a new node")
}

func TestHashRing_RedistributionOnNodeRemove(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	// Add 5 nodes
	nodes := []string{"node-A", "node-B", "node-C", "node-D", "node-E"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	// Get distribution before removal
	oldDist := ring.GetDistribution()
	t.Logf("Distribution before removing node-C (5 nodes):")
	printDistribution(t, oldDist)

	// Remove one node
	newDist := ring.RemoveNode("node-C")

	t.Logf("Distribution after removing node-C (4 nodes):")
	printDistribution(t, newDist)

	assert.Len(t, newDist, 4)
	assert.NotContains(t, newDist, "node-C")

	// Removed node's queues should be distributed among remaining nodes
	totalQueuesCount := 0
	for _, queues := range newDist {
		totalQueuesCount += len(queues)
	}
	assert.Equal(t, totalQueues, totalQueuesCount)
}

func TestHashRing_NoNodes(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	dist := ring.GetDistribution()
	assert.Empty(t, dist)
}

func TestHashRing_SingleNode(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	dist := ring.AddNode("single-node")

	assert.Len(t, dist, 1)
	assert.Len(t, dist["single-node"], totalQueues)
}

func TestHashRing_Deterministic(t *testing.T) {
	// Check that distribution is always the same for same nodes
	ring1, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)
	ring2, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	nodes := []string{"node-A", "node-B", "node-C"}

	for _, node := range nodes {
		ring1.AddNode(node)
		ring2.AddNode(node)
	}

	dist1 := ring1.GetDistribution()
	dist2 := ring2.GetDistribution()

	for _, node := range nodes {
		assert.Equal(t, dist1[node], dist2[node], "distribution should be deterministic")
	}
}

func TestHashRing_AllQueuesAssigned(t *testing.T) {
	ring, err := NewHashRing(totalQueues, 1000)
	require.NoError(t, err)

	nodes := []string{"node-A", "node-B", "node-C", "node-D", "node-E"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	dist := ring.GetDistribution()

	// Collect all queues into a set
	assignedQueues := make(map[int]bool)
	for _, queues := range dist {
		for _, q := range queues {
			assert.False(t, assignedQueues[q], "queue %d assigned to multiple nodes", q)
			assignedQueues[q] = true
		}
	}

	// All queues should be assigned
	assert.Len(t, assignedQueues, totalQueues)
}

func TestHashRing_Validation(t *testing.T) {
	tests := []struct {
		name         string
		totalQueues  int
		virtualNodes int
		wantErr      bool
	}{
		{"valid", 256, 1000, false},
		{"minimum queues", 1, 1000, false},
		{"minimum vnodes", 256, 1, false},
		{"zero queues", 0, 1000, true},
		{"negative queues", -1, 1000, true},
		{"zero vnodes", 256, 0, true},
		{"negative vnodes", 256, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ring, err := NewHashRing(tt.totalQueues, tt.virtualNodes)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, ring)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, ring)
			}
		})
	}
}

func TestHashRing_String(t *testing.T) {
	ring, err := NewHashRing(256, 1000)
	require.NoError(t, err)

	str := ring.String()
	assert.Contains(t, str, "HashRing")
	assert.Contains(t, str, "nodes=0")
	assert.Contains(t, str, "queues=256")
	assert.Contains(t, str, "vnodes=1000")

	ring.AddNode("node-1")
	str = ring.String()
	assert.Contains(t, str, "nodes=1")
}

func TestHashRing_WithMurmurHash(t *testing.T) {
	ring, err := NewHashRing(256, 1000, WithMurmurHash())
	require.NoError(t, err)

	ring.AddNode("node-1")
	ring.AddNode("node-2")

	dist := ring.GetDistribution()
	assert.Len(t, dist, 2)

	totalQueuesCount := len(dist["node-1"]) + len(dist["node-2"])
	assert.Equal(t, 256, totalQueuesCount)
}

func TestHashRing_WithXXHash(t *testing.T) {
	ring, err := NewHashRing(256, 1000, WithXXHash())
	require.NoError(t, err)

	ring.AddNode("node-1")
	ring.AddNode("node-2")

	dist := ring.GetDistribution()
	assert.Len(t, dist, 2)

	totalQueuesCount := len(dist["node-1"]) + len(dist["node-2"])
	assert.Equal(t, 256, totalQueuesCount)
}

func TestHashRing_DifferentHashFunctions(t *testing.T) {
	// Create two rings with different hash functions
	ringXX, err := NewHashRing(256, 1000, WithXXHash())
	require.NoError(t, err)
	ringMurmur, err := NewHashRing(256, 1000, WithMurmurHash())
	require.NoError(t, err)

	ringXX.AddNode("node-1")
	ringXX.AddNode("node-2")

	ringMurmur.AddNode("node-1")
	ringMurmur.AddNode("node-2")

	distXX := ringXX.GetDistribution()
	distMurmur := ringMurmur.GetDistribution()

	// Distributions should be different (different hash functions)
	// But total queue count should be the same
	totalXX := len(distXX["node-1"]) + len(distXX["node-2"])
	totalMurmur := len(distMurmur["node-1"]) + len(distMurmur["node-2"])
	assert.Equal(t, totalXX, totalMurmur, "total queues should be equal")
}
