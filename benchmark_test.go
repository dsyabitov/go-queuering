package queuering

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Generate test data.
var (
	testInt64Keys    []int64
	testShortStrings []string
	testLongStrings  []string
	testQueueKeys    []int
)

func init() {
	rnd := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility.

	testInt64Keys = make([]int64, 10000)
	for i := 0; i < 10000; i++ {
		testInt64Keys[i] = rnd.Int63()
	}

	testShortStrings = make([]string, 10000)
	for i := 0; i < 10000; i++ {
		length := rnd.Intn(10) + 1
		testShortStrings[i] = randomString(length)
	}

	testLongStrings = make([]string, 10000)
	for i := 0; i < 10000; i++ {
		testLongStrings[i] = randomString(64)
	}

	testQueueKeys = make([]int, 10000)
	for i := 0; i < 10000; i++ {
		testQueueKeys[i] = rnd.Intn(1000000)
	}
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// ============== Benchmarks for xxHash32 ==============

func BenchmarkXXHash_Int64(b *testing.B) {
	var buf [8]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testInt64Keys {
			for j := 0; j < 8; j++ {
				buf[j] = byte(key >> (j * 8))
			}
			_ = xxh32(buf[:])
		}
	}
}

func BenchmarkXXHash_ShortString(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testShortStrings {
			_ = xxh32([]byte(key))
		}
	}
}

func BenchmarkXXHash_LongString(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testLongStrings {
			_ = xxh32([]byte(key))
		}
	}
}

// ============== Benchmarks for MurmurHash32 ==============

func BenchmarkMurmurHash_Int64(b *testing.B) {
	var buf [8]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testInt64Keys {
			for j := 0; j < 8; j++ {
				buf[j] = byte(key >> (j * 8))
			}
			_ = sum32(buf[:])
		}
	}
}

func BenchmarkMurmurHash_ShortString(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testShortStrings {
			_ = sum32([]byte(key))
		}
	}
}

func BenchmarkMurmurHash_LongString(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testLongStrings {
			_ = sum32([]byte(key))
		}
	}
}

// ============== Benchmarks for QueueMapper ==============

func BenchmarkQueueMapper_Int64_xxHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperXXHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testInt64Keys {
			_ = mapper.MapInt64(key)
		}
	}
}

func BenchmarkQueueMapper_Int64_MurmurHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperMurmurHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testInt64Keys {
			_ = mapper.MapInt64(key)
		}
	}
}

func BenchmarkQueueMapper_ShortString_xxHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperXXHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testShortStrings {
			_ = mapper.MapString(key)
		}
	}
}

func BenchmarkQueueMapper_ShortString_MurmurHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperMurmurHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testShortStrings {
			_ = mapper.MapString(key)
		}
	}
}

func BenchmarkQueueMapper_LongString_xxHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperXXHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testLongStrings {
			_ = mapper.MapString(key)
		}
	}
}

func BenchmarkQueueMapper_LongString_MurmurHash(b *testing.B) {
	mapper, _ := NewQueueMapper(256, WithMapperMurmurHash())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range testLongStrings {
			_ = mapper.MapString(key)
		}
	}
}

// ============== Benchmarks for HashRing ==============

func BenchmarkHashRing_AddNode_xxHash(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring, _ := NewHashRing(256, 1000, WithXXHash())
		for j := 0; j < 10; j++ {
			ring.AddNode(fmt.Sprintf("node-%d", j))
		}
	}
}

func BenchmarkHashRing_AddNode_MurmurHash(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring, _ := NewHashRing(256, 1000, WithMurmurHash())
		for j := 0; j < 10; j++ {
			ring.AddNode(fmt.Sprintf("node-%d", j))
		}
	}
}

func BenchmarkHashRing_RemoveNode_xxHash(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring, _ := NewHashRing(256, 1000, WithXXHash())
		for j := 0; j < 10; j++ {
			ring.AddNode(fmt.Sprintf("node-%d", j))
		}
		ring.RemoveNode("node-5")
	}
}

func BenchmarkHashRing_RemoveNode_MurmurHash(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring, _ := NewHashRing(256, 1000, WithMurmurHash())
		for j := 0; j < 10; j++ {
			ring.AddNode(fmt.Sprintf("node-%d", j))
		}
		ring.RemoveNode("node-5")
	}
}

// ============== Test keys distribution ==============

func TestKeyDistribution_Int64(t *testing.T) {
	mapper, err := NewQueueMapper(256, WithMapperXXHash())
	require.NoError(t, err)

	queueCounts := make(map[int]int)
	for _, key := range testInt64Keys {
		queue := mapper.MapInt64(key)
		queueCounts[queue]++
	}

	assertDistribution(t, queueCounts, 256, 10000, "int64 keys (xxHash)")

	// MurmurHash
	mapperMurmur, err := NewQueueMapper(256, WithMapperMurmurHash())
	require.NoError(t, err)
	queueCountsMurmur := make(map[int]int)
	for _, key := range testInt64Keys {
		queue := mapperMurmur.MapInt64(key)
		queueCountsMurmur[queue]++
	}

	assertDistribution(t, queueCountsMurmur, 256, 10000, "int64 keys (MurmurHash)")
}

func TestKeyDistribution_ShortStrings(t *testing.T) {
	mapper, err := NewQueueMapper(256, WithMapperXXHash())
	require.NoError(t, err)

	queueCounts := make(map[int]int)
	for _, key := range testShortStrings {
		queue := mapper.MapString(key)
		queueCounts[queue]++
	}

	assertDistribution(t, queueCounts, 256, 10000, "short strings (xxHash)")

	// MurmurHash
	mapperMurmur, err := NewQueueMapper(256, WithMapperMurmurHash())
	require.NoError(t, err)
	queueCountsMurmur := make(map[int]int)
	for _, key := range testShortStrings {
		queue := mapperMurmur.MapString(key)
		queueCountsMurmur[queue]++
	}

	assertDistribution(t, queueCountsMurmur, 256, 10000, "short strings (MurmurHash)")
}

func TestKeyDistribution_LongStrings(t *testing.T) {
	mapper, err := NewQueueMapper(256, WithMapperXXHash())
	require.NoError(t, err)

	queueCounts := make(map[int]int)
	for _, key := range testLongStrings {
		queue := mapper.MapString(key)
		queueCounts[queue]++
	}

	assertDistribution(t, queueCounts, 256, 10000, "long strings (xxHash)")

	// MurmurHash
	mapperMurmur, err := NewQueueMapper(256, WithMapperMurmurHash())
	require.NoError(t, err)
	queueCountsMurmur := make(map[int]int)
	for _, key := range testLongStrings {
		queue := mapperMurmur.MapString(key)
		queueCountsMurmur[queue]++
	}

	assertDistribution(t, queueCountsMurmur, 256, 10000, "long strings (MurmurHash)")
}

func TestKeyDistribution_HashRing(t *testing.T) {
	ring, err := NewHashRing(256, 1000, WithXXHash())
	require.NoError(t, err)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")
	ring.AddNode("node-4")

	dist := ring.GetDistribution()

	// Checking all queues are distributed.
	totalQueues := 0
	for _, count := range dist {
		totalQueues += len(count)
	}
	assert.Equal(t, 256, totalQueues, "all queues should be distributed")

	// Checking distribution.
	ideal := 256 / 4 // 64 queues per node
	for node, queues := range dist {
		count := len(queues)
		minExpected := ideal / 2
		maxExpected := ideal * 3 / 2
		if count < minExpected || count > maxExpected {
			t.Errorf("node %s has %d queues, expected between %d and %d", node, count, minExpected, maxExpected)
		}
	}
}

func assertDistribution(t *testing.T, queueCounts map[int]int, totalQueues, totalKeys int, testName string) { //nolint:unparam // can be changed in the future
	t.Helper()

	if len(queueCounts) != totalQueues {
		t.Errorf("%s: only %d out of %d queues used", testName, len(queueCounts), totalQueues)
	}

	ideal := float64(totalKeys) / float64(totalQueues)
	minCount, maxCount := totalKeys, 0
	var sum float64 = 0

	for _, count := range queueCounts {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
		sum += float64(count)
	}

	mean := sum / float64(len(queueCounts))
	stdDev := 0.0
	for _, count := range queueCounts {
		diff := float64(count) - mean
		stdDev += diff * diff
	}
	stdDev = sqrt(stdDev / float64(len(queueCounts)))

	t.Logf("%s: ideal=%.1f, min=%d, max=%d, mean=%.1f, stddev=%.1f",
		testName, ideal, minCount, maxCount, mean, stdDev)

	maxAllowedDeviation := ideal * 1.5 // 150% from ideal
	if float64(maxCount)-ideal > maxAllowedDeviation {
		t.Errorf("%s: max count %d deviates too much from ideal %.1f", testName, maxCount, ideal)
	}
	if ideal-float64(minCount) > maxAllowedDeviation {
		t.Errorf("%s: min count %d deviates too much from ideal %.1f", testName, minCount, ideal)
	}
}

func sqrt(x float64) float64 {
	if x == 0 {
		return 0
	}
	z := x
	for i := 0; i < 20; i++ {
		z = (z + x/z) / 2
	}
	return z
}
