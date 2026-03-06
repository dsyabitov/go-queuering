package queuering

import "math/bits"

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// sum32WithSeed returns the MurmurHash3 hash of data with the given seed.
func sum32WithSeed(data []byte, seed uint32) uint32 {
	h1 := seed
	nblocks := len(data) / 4

	for i := 0; i < nblocks; i++ {
		// Read 4 bytes as uint32 in Little Endian order
		k1 := uint32(data[i*4]) |
			uint32(data[i*4+1])<<8 |
			uint32(data[i*4+2])<<16 |
			uint32(data[i*4+3])<<24

		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32

	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(len(data))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

// sum32 returns the MurmurHash3 hash of data.
func sum32(data []byte) uint32 {
	return sum32WithSeed(data, 0)
}
