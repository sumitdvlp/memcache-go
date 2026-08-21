// Package cluster provides consistent hashing for distributing keys across
// multiple memcache nodes.
package cluster

import "hash/crc32"

// HashFunc maps a byte slice to a point on the 32-bit ring.
type HashFunc func(data []byte) uint32

// defaultHash uses CRC32 (IEEE) which is fast and well-distributed enough for
// key placement.
func defaultHash(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
