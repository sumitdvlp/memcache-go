package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/kusu/memcache-go/client"
	"github.com/kusu/memcache-go/memcache"
)

// startNode starts a memcache server on a random local port and returns its
// address plus a shutdown function.
func startNode(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := memcache.NewServer(memcache.Config{
		MaxBytes:        8 * 1024 * 1024,
		CleanupInterval: 50 * time.Millisecond,
		Logger:          log.New(io.Discard, "", 0),
	})
	go srv.Serve(ln)
	return ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Close(ctx)
	}
}

func TestClusterSetGetDelete(t *testing.T) {
	var addrs []string
	var stops []func()
	for i := 0; i < 3; i++ {
		addr, stop := startNode(t)
		addrs = append(addrs, addr)
		stops = append(stops, stop)
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	c := client.New(addrs...)
	defer c.Close()

	// Set a batch of keys, then read them all back.
	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := c.Set(&client.Item{Key: key, Value: []byte(fmt.Sprintf("val-%d", i))}); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		item, err := c.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		want := fmt.Sprintf("val-%d", i)
		if string(item.Value) != want {
			t.Fatalf("get %s = %q, want %q", key, item.Value, want)
		}
	}

	// Delete and confirm miss.
	if err := c.Delete("key-0"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.Get("key-0"); !errors.Is(err, client.ErrCacheMiss) {
		t.Fatalf("expected cache miss after delete, got %v", err)
	}
}

func TestClusterKeysDistributedAcrossNodes(t *testing.T) {
	var addrs []string
	var stops []func()
	for i := 0; i < 3; i++ {
		addr, stop := startNode(t)
		addrs = append(addrs, addr)
		stops = append(stops, stop)
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	c := client.New(addrs...)
	defer c.Close()

	const n = 300
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("dist-%d", i)
		if err := c.Set(&client.Item{Key: key, Value: []byte("x")}); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	// Query each node's item count via a direct stats connection; ensure more
	// than one node received keys (i.e. distribution actually happened).
	nonEmpty := 0
	for _, addr := range addrs {
		if statsItems(t, addr) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty < 2 {
		t.Fatalf("expected keys spread across >=2 nodes, only %d non-empty", nonEmpty)
	}
}

func TestCacheMiss(t *testing.T) {
	addr, stop := startNode(t)
	defer stop()
	c := client.New(addr)
	defer c.Close()
	if _, err := c.Get("does-not-exist"); !errors.Is(err, client.ErrCacheMiss) {
		t.Fatalf("expected ErrCacheMiss, got %v", err)
	}
}

// statsItems opens a raw connection and parses "curr_items" from STATS.
func statsItems(t *testing.T, addr string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "stats\r\n")
	conn.SetReadDeadline(time.Now().Add(time.Second))

	buf := make([]byte, 4096)
	nread, _ := conn.Read(buf)
	out := string(buf[:nread])
	var items int
	for _, line := range splitLines(out) {
		var name string
		var val int
		if _, err := fmt.Sscanf(line, "STAT %s %d", &name, &val); err == nil && name == "curr_items" {
			items = val
		}
	}
	return items
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	return lines
}
