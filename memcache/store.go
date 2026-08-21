// Package memcache implements a single-node in-memory key-value store with
// LRU eviction and TTL expiration, plus a Memcache text-protocol server.
package memcache

import (
	"container/list"
	"sync"
	"time"
)

// item is the value stored in the LRU list.
type item struct {
	key       string
	value     []byte
	flags     uint32
	expiresAt time.Time // zero means no expiration
	size      int       // approximate memory footprint (key + value)
}

// isExpired reports whether the item has passed its TTL.
func (it *item) isExpired(now time.Time) bool {
	return !it.expiresAt.IsZero() && now.After(it.expiresAt)
}

// Store is a thread-safe, size-bounded LRU cache with TTL support.
type Store struct {
	mu       sync.RWMutex
	maxBytes int64
	curBytes int64
	ll       *list.List               // front = most recently used
	items    map[string]*list.Element // key -> element in ll

	// stats (protected by mu unless noted)
	getHits   uint64
	getMisses uint64
	cmdSet    uint64
	evictions uint64
	expired   uint64

	janitorStop chan struct{}
	janitorOnce sync.Once
}

// NewStore creates a store bounded to maxBytes of key+value data. A background
// janitor removes expired keys every cleanupInterval. If maxBytes <= 0 the
// store is unbounded (no LRU eviction).
func NewStore(maxBytes int64, cleanupInterval time.Duration) *Store {
	s := &Store{
		maxBytes:    maxBytes,
		ll:          list.New(),
		items:       make(map[string]*list.Element),
		janitorStop: make(chan struct{}),
	}
	if cleanupInterval > 0 {
		go s.janitor(cleanupInterval)
	}
	return s
}

// Set inserts or replaces a key with the given value, flags and TTL.
// A ttl of 0 means the key never expires.
func (s *Store) Set(key string, value []byte, flags uint32, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// Copy the value so callers cannot mutate stored data.
	v := make([]byte, len(value))
	copy(v, value)
	newSize := len(key) + len(v)

	if el, ok := s.items[key]; ok {
		it := el.Value.(*item)
		s.curBytes += int64(newSize - it.size)
		it.value = v
		it.flags = flags
		it.expiresAt = expiresAt
		it.size = newSize
		s.ll.MoveToFront(el)
	} else {
		it := &item{key: key, value: v, flags: flags, expiresAt: expiresAt, size: newSize}
		el := s.ll.PushFront(it)
		s.items[key] = el
		s.curBytes += int64(newSize)
	}
	s.cmdSet++
	s.evictIfNeeded()
}

// Get returns the value and flags for a key. The bool is false on miss or if
// the key has expired.
func (s *Store) Get(key string) ([]byte, uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.items[key]
	if !ok {
		s.getMisses++
		return nil, 0, false
	}
	it := el.Value.(*item)
	if it.isExpired(time.Now()) {
		s.removeElement(el)
		s.expired++
		s.getMisses++
		return nil, 0, false
	}
	s.ll.MoveToFront(el)
	s.getHits++
	// Return a copy so callers cannot mutate stored data.
	out := make([]byte, len(it.value))
	copy(out, it.value)
	return out, it.flags, true
}

// Delete removes a key. It returns true if the key existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		return false
	}
	s.removeElement(el)
	return true
}

// Flush removes all keys.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ll.Init()
	s.items = make(map[string]*list.Element)
	s.curBytes = 0
}

// Len returns the current number of stored keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Bytes returns the approximate memory used by stored key+value data.
func (s *Store) Bytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.curBytes
}

// removeElement removes an element from both the list and the map and updates
// the byte counter. Caller must hold the write lock.
func (s *Store) removeElement(el *list.Element) {
	it := el.Value.(*item)
	s.ll.Remove(el)
	delete(s.items, it.key)
	s.curBytes -= int64(it.size)
}

// evictIfNeeded evicts least-recently-used entries until the store fits within
// maxBytes. Caller must hold the write lock.
func (s *Store) evictIfNeeded() {
	if s.maxBytes <= 0 {
		return
	}
	for s.curBytes > s.maxBytes {
		el := s.ll.Back()
		if el == nil {
			break
		}
		s.removeElement(el)
		s.evictions++
	}
}

// janitor periodically removes expired keys.
func (s *Store) janitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.cleanupExpired()
		case <-s.janitorStop:
			return
		}
	}
}

// cleanupExpired scans and removes expired entries.
func (s *Store) cleanupExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for el := s.ll.Back(); el != nil; {
		prev := el.Prev()
		it := el.Value.(*item)
		if it.isExpired(now) {
			s.removeElement(el)
			s.expired++
		}
		el = prev
	}
}

// Close stops the background janitor goroutine.
func (s *Store) Close() {
	s.janitorOnce.Do(func() {
		close(s.janitorStop)
	})
}
