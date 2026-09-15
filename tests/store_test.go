package tests

import (
	"testing"
	"time"

	"github.com/kusu/memcache-go/memcache"
)

func TestStoreSetGet(t *testing.T) {
	s := memcache.NewStore(0, 0)
	defer s.Close()

	s.Set("foo", []byte("bar"), 7, 0)
	val, flags, ok := s.Get("foo")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "bar" {
		t.Fatalf("value = %q, want bar", val)
	}
	if flags != 7 {
		t.Fatalf("flags = %d, want 7", flags)
	}
}

func TestStoreMiss(t *testing.T) {
	s := memcache.NewStore(0, 0)
	defer s.Close()
	if _, _, ok := s.Get("nope"); ok {
		t.Fatal("expected miss")
	}
}

func TestStoreDeleteFlush(t *testing.T) {
	s := memcache.NewStore(0, 0)
	defer s.Close()
	s.Set("a", []byte("1"), 0, 0)
	if !s.Delete("a") {
		t.Fatal("delete should report true for existing key")
	}
	if s.Delete("a") {
		t.Fatal("delete should report false for missing key")
	}
	s.Set("b", []byte("2"), 0, 0)
	s.Set("c", []byte("3"), 0, 0)
	s.Flush()
	if s.Len() != 0 {
		t.Fatalf("len after flush = %d, want 0", s.Len())
	}
}

func TestStoreLRUEviction(t *testing.T) {
	// Each entry is key(1)+value(9) = 10 bytes; cap at 25 bytes -> ~2 entries.
	s := memcache.NewStore(25, 0)
	defer s.Close()

	s.Set("a", []byte("123456789"), 0, 0)
	s.Set("b", []byte("123456789"), 0, 0)
	// Touch "a" so "b" becomes least recently used.
	if _, _, ok := s.Get("a"); !ok {
		t.Fatal("a should still be present")
	}
	s.Set("c", []byte("123456789"), 0, 0) // should evict "b"

	if _, _, ok := s.Get("b"); ok {
		t.Fatal("b should have been evicted (LRU)")
	}
	if _, _, ok := s.Get("a"); !ok {
		t.Fatal("a should be retained")
	}
	if _, _, ok := s.Get("c"); !ok {
		t.Fatal("c should be present")
	}
}

func TestStoreTTLExpiration(t *testing.T) {
	s := memcache.NewStore(0, 0)
	defer s.Close()
	s.Set("temp", []byte("x"), 0, 20*time.Millisecond)
	if _, _, ok := s.Get("temp"); !ok {
		t.Fatal("should be present before expiry")
	}
	time.Sleep(40 * time.Millisecond)
	if _, _, ok := s.Get("temp"); ok {
		t.Fatal("should be expired")
	}
}

func TestStoreJanitorCleansExpired(t *testing.T) {
	s := memcache.NewStore(0, 10*time.Millisecond)
	defer s.Close()
	s.Set("temp", []byte("x"), 0, 15*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if s.Len() != 0 {
		t.Fatalf("janitor should have removed expired key, len = %d", s.Len())
	}
}
