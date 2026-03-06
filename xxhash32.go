package queuering

import "math/bits"

const (
	xxhPrime32_1 uint32 = 0x9E3779B1
	xxhPrime32_2 uint32 = 0x85EBCA77
	xxhPrime32_3 uint32 = 0xC2B2AE3D
	xxhPrime32_4 uint32 = 0x27D4EB2F
	xxhPrime32_5 uint32 = 0x165667B1
)

// xxh32Round performs one round of xxHash32 hashing.
func xxh32Round(acc, input uint32) uint32 {
	acc += input * xxhPrime32_2
	acc = bits.RotateLeft32(acc, 13)
	acc *= xxhPrime32_1
	return acc
}

// xxh32Avalanche performs the final avalanche mixing.
func xxh32Avalanche(h uint32) uint32 {
	h ^= h >> 15
	h *= xxhPrime32_2
	h ^= h >> 13
	h *= xxhPrime32_3
	h ^= h >> 16
	return h
}

// xxh32 computes a 32-bit xxHash for the given data.
func xxh32(data []byte) uint32 {
	var h32 uint32
	length := uint32(len(data))

	if length >= 16 {
		// Process in 16-byte blocks (4 x uint32)
		// v1 = seed + prime1 + prime2 (seed=0 by default)
		v1 := xxhPrime32_1
		v1 += xxhPrime32_2
		v2 := xxhPrime32_2
		var v3 uint32 = 0
		// v4 = seed - prime1 = 0 - prime1 (for uint32 this is ~prime1 + 1)
		v4 := ^xxhPrime32_1 + 1

		limit := len(data) - 16
		i := 0
		for i <= limit {
			v1 = xxh32Round(v1, readLE32(data[i:i+4]))
			v2 = xxh32Round(v2, readLE32(data[i+4:i+8]))
			v3 = xxh32Round(v3, readLE32(data[i+8:i+12]))
			v4 = xxh32Round(v4, readLE32(data[i+12:i+16]))
			i += 16
		}

		h32 = bits.RotateLeft32(v1, 1) +
			bits.RotateLeft32(v2, 7) +
			bits.RotateLeft32(v3, 12) +
			bits.RotateLeft32(v4, 18)
	} else {
		h32 = xxhPrime32_5
	}

	h32 += length

	// Process remaining 4-byte blocks
	for i := len(data) - int(length%16); i+4 <= len(data); i += 4 {
		h32 += readLE32(data[i:i+4]) * xxhPrime32_3
		h32 = bits.RotateLeft32(h32, 17) * xxhPrime32_4
	}

	// Process remaining bytes
	for i := len(data) - int(length%4); i < len(data); i++ {
		h32 += uint32(data[i]) * xxhPrime32_5
		h32 = bits.RotateLeft32(h32, 11) * xxhPrime32_1
	}

	return xxh32Avalanche(h32)
}

// readLE32 reads a 32-bit number in little-endian format.
func readLE32(b []byte) uint32 {
	return uint32(b[0]) |
		uint32(b[1])<<8 |
		uint32(b[2])<<16 |
		uint32(b[3])<<24
}
