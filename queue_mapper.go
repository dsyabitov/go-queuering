package queuering

import "fmt"

// QueueMapperOption is an option function for QueueMapper.
type QueueMapperOption func(*QueueMapper)

// WithMapperHashFunc sets the hash function for QueueMapper.
func WithMapperHashFunc(hashFunc HashFunc) QueueMapperOption {
	return func(m *QueueMapper) {
		m.hashFunc = hashFunc
	}
}

// WithMapperXXHash uses xxHash32 (default).
func WithMapperXXHash() QueueMapperOption {
	return WithMapperHashFunc(xxHash32Wrapper)
}

// WithMapperMurmurHash uses MurmurHash32.
func WithMapperMurmurHash() QueueMapperOption {
	return WithMapperHashFunc(murmurHash32Wrapper)
}

// QueueMapper provides methods for mapping keys to queue numbers.
type QueueMapper struct {
	hashFunc    HashFunc
	totalQueues int
}

// NewQueueMapper creates a new QueueMapper with the specified number of queues.
// xxHash32 is used by default.
// Returns an error if parameters are invalid.
func NewQueueMapper(totalQueues int, opts ...QueueMapperOption) (*QueueMapper, error) {
	if totalQueues < 1 {
		return nil, fmt.Errorf("totalQueues must be at least 1, got %d", totalQueues)
	}

	m := &QueueMapper{
		totalQueues: totalQueues,
		hashFunc:    xxHash32Wrapper, // default: xxHash32
	}

	for _, opt := range opts {
		opt(m)
	}

	return m, nil
}

// MapString maps a string key to a queue number.
// Returns a queue number from 0 to totalQueues-1.
func (m *QueueMapper) MapString(key string) int {
	hash := m.hashFunc([]byte(key))
	return int(hash % uint32(m.totalQueues))
}

// MapInt64 maps an int64 key to a queue number.
// Returns a queue number from 0 to totalQueues-1.
func (m *QueueMapper) MapInt64(key int64) int {
	// Convert int64 to bytes (little-endian)
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(key >> (i * 8))
	}
	hash := m.hashFunc(buf[:])
	return int(hash % uint32(m.totalQueues))
}

// MapUint64 maps a uint64 key to a queue number.
// Returns a queue number from 0 to totalQueues-1.
func (m *QueueMapper) MapUint64(key uint64) int {
	// Convert uint64 to bytes (little-endian)
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(key >> (i * 8))
	}
	hash := m.hashFunc(buf[:])
	return int(hash % uint32(m.totalQueues))
}

// MapInt maps an int key to a queue number.
// Returns a queue number from 0 to totalQueues-1.
func (m *QueueMapper) MapInt(key int) int {
	return m.MapInt64(int64(key))
}

// TotalQueues returns the total number of queues.
func (m *QueueMapper) TotalQueues() int {
	return m.totalQueues
}

// String returns a string representation of the QueueMapper.
func (m *QueueMapper) String() string {
	return fmt.Sprintf("QueueMapper{queues=%d}", m.totalQueues)
}
