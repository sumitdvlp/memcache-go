package cluster

import (
	"sort"
	"strconv"
	"sync"
)

// DefaultReplicas is the number of virtual nodes created per physical node.
// More replicas yield a smoother key distribution at the cost of memory.
const DefaultReplicas = 160

// Ring is a thread-safe consistent hash ring. Each physical node is placed at
// multiple points ("virtual nodes") on the ring to balance the key space.
type Ring struct {
	mu       sync.RWMutex
	hash     HashFunc
	replicas int
	keys     []uint32          // sorted hash values of all virtual nodes
	hashMap  map[uint32]string // virtual node hash -> physical node
	members  map[string]bool   // set of physical nodes
}

// NewRing creates a ring with the given replica count. If replicas <= 0,
// DefaultReplicas is used. If fn is nil, a CRC32 hash is used.
func NewRing(replicas int, fn HashFunc) *Ring {
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	if fn == nil {
		fn = defaultHash
	}
	return &Ring{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[uint32]string),
		members:  make(map[string]bool),
	}
}

// virtualKey builds the string hashed for the i-th virtual node of a node.
func virtualKey(node string, i int) string {
	return strconv.Itoa(i) + "#" + node
}

// Add inserts one or more physical nodes into the ring.
func (r *Ring) Add(nodes ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, node := range nodes {
		if node == "" || r.members[node] {
			continue
		}
		r.members[node] = true
		for i := 0; i < r.replicas; i++ {
			h := r.hash([]byte(virtualKey(node, i)))
			r.keys = append(r.keys, h)
			r.hashMap[h] = node
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// Remove deletes a physical node and all its virtual nodes from the ring.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.members[node] {
		return
	}
	delete(r.members, node)
	for i := 0; i < r.replicas; i++ {
		h := r.hash([]byte(virtualKey(node, i)))
		delete(r.hashMap, h)
	}
	// Rebuild the sorted key slice from the remaining virtual nodes.
	r.keys = r.keys[:0]
	for h := range r.hashMap {
		r.keys = append(r.keys, h)
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// Get returns the node responsible for key. It returns "" if the ring is empty.
func (r *Ring) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 {
		return ""
	}
	h := r.hash([]byte(key))
	// Binary search for the first virtual node >= h, wrapping around the ring.
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx == len(r.keys) {
		idx = 0
	}
	return r.hashMap[r.keys[idx]]
}

// GetN returns up to n distinct physical nodes responsible for key, walking
// clockwise around the ring. Useful for replication or failover.
func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	h := r.hash([]byte(key))
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx == len(r.keys) {
		idx = 0
	}
	result := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for i := 0; i < len(r.keys) && len(result) < n; i++ {
		node := r.hashMap[r.keys[(idx+i)%len(r.keys)]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

// Members returns the current set of physical nodes.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for m := range r.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// IsEmpty reports whether the ring has no members.
func (r *Ring) IsEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.keys) == 0
}
