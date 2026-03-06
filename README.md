# queuering

[![Go Reference](https://pkg.go.dev/badge/github.com/dsyabitov/queuering.svg)](https://pkg.go.dev/github.com/dsyabitov/queuering)
[![Go Report Card](https://goreportcard.com/badge/github.com/dsyabitov/queuering)](https://goreportcard.com/report/github.com/dsyabitov/queuering)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Consistent hashing library for queue distribution in Go.**

Distribute queues across worker nodes with minimal rebalancing when nodes join or leave the cluster.

## Features

- ✅ **Consistent Hashing** — minimal queue redistribution when nodes change
- ✅ **Multiple Hash Functions** — choose between xxHash32 (default) or MurmurHash32
- ✅ **QueueMapper** — simple API for mapping keys to queue numbers
- ✅ **HashRing** — full-featured consistent hashing ring for worker distribution
- ✅ **Zero Dependencies** — pure Go implementation
- ✅ **Benchmarked** — performance tested with various key types

## Installation

```bash
go get github.com/dsyabitov/queuering
```

## Quick Start

### QueueMapper — Map Keys to Queues

```go
package main

import (
    "fmt"
    "github.com/dsyabitov/queuering"
)

func main() {
    // Create mapper for 256 queues (default: xxHash32)
    mapper := queuering.NewQueueMapper(256)

    // Map different key types to queues
    queue1 := mapper.MapInt64(12345)
    queue2 := mapper.MapString("user:42")
    queue3 := mapper.MapUint64(9876543210)

    fmt.Printf("int64 key -> queue %d\n", queue1)
    fmt.Printf("string key -> queue %d\n", queue2)
    fmt.Printf("uint64 key -> queue %d\n", queue3)
}
```

### HashRing — Distribute Queues Across Workers

```go
package main

import (
    "fmt"
    "github.com/dsyabitov/queuering"
)

func main() {
    // Create hash ring with 256 queues and 1000 virtual nodes per worker
    ring := queuering.NewHashRing(256, 1000)

    // Add worker nodes
    ring.AddNode("worker-1")
    ring.AddNode("worker-2")
    ring.AddNode("worker-3")

    // Get queue distribution
    distribution := ring.GetDistribution()

    for worker, queues := range distribution {
        fmt.Printf("%s handles %d queues\n", worker, len(queues))
    }

    // Add a new worker — queues will be rebalanced
    ring.AddNode("worker-4")

    // Remove a worker — queues will be redistributed
    ring.RemoveNode("worker-2")
}
```

## Hash Function Selection

Choose the hash function that best fits your needs:

### xxHash32 (Default) — Fastest

```go
// Explicitly use xxHash32
mapper := queuering.NewQueueMapper(256, queuering.WithMapperXXHash())
ring := queuering.NewHashRing(256, 1000, queuering.WithXXHash())
```

### MurmurHash32 — Alternative

```go
// Use MurmurHash32
mapper := queuering.NewQueueMapper(256, queuering.WithMapperMurmurHash())
ring := queuering.NewHashRing(256, 1000, queuering.WithMurmurHash())
```

### Custom Hash Function

```go
// Use your own hash function
func myHash(data []byte) uint32 {
    // Your implementation
    return 42
}

mapper := queuering.NewQueueMapper(256, queuering.WithMapperHashFunc(myHash))
ring := queuering.NewHashRing(256, 1000, queuering.WithHashFunc(myHash))
```

## Use Cases

### 1. Kafka/RabbitMQ Consumer Distribution

```go
// Distribute 256 Kafka partitions across consumer pods
ring := queuering.NewHashRing(256, 1000)

// Each pod adds itself to the ring
podName := os.Getenv("POD_NAME")
ring.AddNode(podName)

// Get partitions this pod should consume
distribution := ring.GetDistribution()
myPartitions := distribution[podName]

// Consume from assigned partitions
for _, partition := range myPartitions {
    go consumePartition(partition)
}
```

### 2. Task Queue Routing

```go
// Route tasks to queues based on task ID
mapper := queuering.NewQueueMapper(256)

func routeTask(taskID int64) int {
    return mapper.MapInt64(taskID)
}

// Same task ID always goes to the same queue
queue1 := routeTask(12345)
queue2 := routeTask(12345) // queue1 == queue2
```

### 3. Multi-Tenant Queue Assignment

```go
// Assign tenants to queues
mapper := queuering.NewQueueMapper(1024)

func getTenantQueue(tenantID string) int {
    return mapper.MapString(fmt.Sprintf("tenant:%s", tenantID))
}

// All tenant's tasks go to the same queue
queue := getTenantQueue("acme-corp")
```

## Performance Benchmarks

### Hash Functions (10,000 keys)

| Test | xxHash32 | MurmurHash32 |
|------|----------|--------------|
| Int64 | 127,121 ns/op | 133,463 ns/op |
| Short Strings (<10 chars) | 134,762 ns/op | 137,731 ns/op |
| Long Strings (64 chars) | 407,115 ns/op | 590,413 ns/op |

**xxHash32 is ~5-31% faster** depending on key type.

### QueueMapper (10,000 keys, 256 queues)

| Test | xxHash | MurmurHash |
|------|--------|------------|
| Int64 | 227,429 ns/op | 239,350 ns/op |
| Short Strings | 294,936 ns/op | 298,369 ns/op |
| Long Strings | 439,078 ns/op | 666,595 ns/op |

### Key Distribution Quality

| Key Type | Hash Function | Ideal | Min | Max | StdDev |
|----------|---------------|-------|-----|-----|--------|
| Int64 | xxHash | 39.1 | 21 | 55 | 6.3 |
| Int64 | MurmurHash | 39.1 | 23 | 56 | 6.6 |
| Short Strings | xxHash | 39.1 | 20 | 88 | 10.8 |
| Long Strings | xxHash | 39.1 | 23 | 59 | 5.7 |

Run benchmarks:

```bash
go test -bench=. -benchmem
```

## API Reference

### QueueMapper

```go
// Create new mapper
func NewQueueMapper(totalQueues int, opts ...QueueMapperOption) *QueueMapper

// Map keys to queues
func (m *QueueMapper) MapString(key string) int
func (m *QueueMapper) MapInt64(key int64) int
func (m *QueueMapper) MapUint64(key uint64) int
func (m *QueueMapper) MapInt(key int) int

// Get total queues
func (m *QueueMapper) TotalQueues() int
```

### HashRing

```go
// Create new ring
func NewHashRing(totalQueues, virtualNodes int, opts ...HashRingOption) *HashRing

// Manage nodes
func (h *HashRing) AddNode(nodeName string) map[string][]int
func (h *HashRing) RemoveNode(nodeName string) map[string][]int
func (h *HashRing) GetDistribution() map[string][]int
```

### Options

```go
// Hash function options
func WithXXHash() HashRingOption
func WithMurmurHash() HashRingOption
func WithHashFunc(fn HashFunc) HashRingOption

// Mapper options
func WithMapperXXHash() QueueMapperOption
func WithMapperMurmurHash() QueueMapperOption
func WithMapperHashFunc(fn HashFunc) QueueMapperOption
```

## Why queuering?

| Feature | queuering | Other Libraries |
|---------|-----------|-----------------|
| Hash function choice | ✅ xxHash + MurmurHash | ❌ Usually fixed |
| Queue-focused API | ✅ Built for queues | ❌ Generic consistent hashing |
| Zero dependencies | ✅ Pure Go | ⚠️ Often has deps |
| Benchmark included | ✅ Comprehensive | ⚠️ Varies |

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
