package queuering

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueMapper_MapString(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	// Same key always gives same result
	queue1 := mapper.MapString("user:123")
	queue2 := mapper.MapString("user:123")
	assert.Equal(t, queue1, queue2, "same key should map to same queue")

	// Different keys may give different queues
	queue3 := mapper.MapString("user:456")
	assert.NotEqual(t, queue1, queue3, "different keys should likely map to different queues")

	// Queue number is within range
	assert.GreaterOrEqual(t, queue1, 0)
	assert.Less(t, queue1, 256)
}

func TestQueueMapper_MapInt64(t *testing.T) {
	tests := []struct {
		name string
		key  int64
	}{
		{"positive", 12345},
		{"zero", 0},
		{"negative", -1},
		{"max int64", 9223372036854775807},
		{"min int64", -9223372036854775808},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := NewQueueMapper(256)
			require.NoError(t, err)

			queue1 := mapper.MapInt64(tt.key)
			queue2 := mapper.MapInt64(tt.key)
			assert.Equal(t, queue1, queue2, "same key should map to same queue")
			assert.GreaterOrEqual(t, queue1, 0)
			assert.Less(t, queue1, 256)
		})
	}
}

func TestQueueMapper_MapUint64(t *testing.T) {
	tests := []struct {
		name string
		key  uint64
	}{
		{"positive", 12345},
		{"zero", 0},
		{"max uint64", 18446744073709551615},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := NewQueueMapper(256)
			require.NoError(t, err)

			queue := mapper.MapUint64(tt.key)
			assert.GreaterOrEqual(t, queue, 0)
			assert.Less(t, queue, 256)
		})
	}
}

func TestQueueMapper_MapInt(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	queue1 := mapper.MapInt(12345)
	queue2 := mapper.MapInt(12345)
	assert.Equal(t, queue1, queue2)
	assert.GreaterOrEqual(t, queue1, 0)
	assert.Less(t, queue1, 256)
}

func TestQueueMapper_Distribution(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	// Distribute 10000 keys across queues
	queueCounts := make(map[int]int)
	for i := 0; i < 10000; i++ {
		queue := mapper.MapInt64(int64(i))
		queueCounts[queue]++
	}

	// Check that all queues are used
	assert.Len(t, queueCounts, 256, "all queues should be used")

	// Check distribution uniformity
	// Average: 10000/256 ≈ 39 queues
	// Allow deviation up to 100% (natural variation for hashing)
	ideal := 10000 / 256
	tolerance := float64(ideal) * 1.0

	for queue, count := range queueCounts {
		assert.InDelta(t, float64(ideal), float64(count), tolerance, "queue %d has unbalanced distribution", queue)
	}
}

func TestQueueMapper_DistributionStrings(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	// Distribute 10000 string keys across queues
	queueCounts := make(map[int]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("user:%08d-%04d-%04d-%04d-%012d", i, i%10000, i%10000, i%10000, i)
		queue := mapper.MapString(key)
		queueCounts[queue]++
	}

	// Check that all queues are used
	assert.Len(t, queueCounts, 256, "all queues should be used")

	// Check distribution uniformity
	ideal := 10000 / 256
	tolerance := float64(ideal) * 1.5 // 150% for string keys

	for queue, count := range queueCounts {
		assert.InDelta(t, float64(ideal), float64(count), tolerance, "queue %d has unbalanced distribution", queue)
	}
}

func TestQueueMapper_TotalQueues(t *testing.T) {
	mapper, err := NewQueueMapper(512)
	require.NoError(t, err)
	assert.Equal(t, 512, mapper.TotalQueues())
}

func TestQueueMapper_EdgeCases(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	// Empty string
	queue := mapper.MapString("")
	assert.GreaterOrEqual(t, queue, 0)
	assert.Less(t, queue, 256)
}

func TestQueueMapper_WithMurmurHash(t *testing.T) {
	mapper, err := NewQueueMapper(256, WithMapperMurmurHash())
	require.NoError(t, err)

	queue1 := mapper.MapInt64(12345)
	queue2 := mapper.MapInt64(12345)
	assert.Equal(t, queue1, queue2, "same key should map to same queue")
	assert.GreaterOrEqual(t, queue1, 0)
	assert.Less(t, queue1, 256)
}

func TestQueueMapper_WithXXHash(t *testing.T) {
	mapper, err := NewQueueMapper(256, WithMapperXXHash())
	require.NoError(t, err)

	queue1 := mapper.MapString("test-key")
	queue2 := mapper.MapString("test-key")
	assert.Equal(t, queue1, queue2, "same key should map to same queue")
	assert.GreaterOrEqual(t, queue1, 0)
	assert.Less(t, queue1, 256)
}

func TestQueueMapper_Validation(t *testing.T) {
	tests := []struct {
		name        string
		totalQueues int
		wantErr     bool
	}{
		{"valid", 256, false},
		{"minimum", 1, false},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, err := NewQueueMapper(tt.totalQueues)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, mapper)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, mapper)
			}
		})
	}
}

func TestQueueMapper_String(t *testing.T) {
	mapper, err := NewQueueMapper(256)
	require.NoError(t, err)

	str := mapper.String()
	assert.Contains(t, str, "QueueMapper")
	assert.Contains(t, str, "queues=256")
}
