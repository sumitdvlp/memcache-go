// Package client implements a multi-node memcache client that routes keys to
// servers using a consistent hash ring.
package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kusu/memcache-go/cluster"
)

// ErrCacheMiss is returned by Get when the key is not present.
var ErrCacheMiss = errors.New("memcache: cache miss")

// ErrNoNodes is returned when the client has no configured nodes.
var ErrNoNodes = errors.New("memcache: no nodes configured")

// Item is a value stored/retrieved from the cluster.
type Item struct {
	Key        string
	Value      []byte
	Flags      uint32
	Expiration int32 // seconds; see memcached exptime semantics
}

// Client is a consistent-hashing memcache client. It is safe for concurrent
// use by multiple goroutines.
type Client struct {
	ring        *cluster.Ring
	dialTimeout time.Duration
	ioTimeout   time.Duration

	mu    sync.Mutex
	pools map[string]*connPool // addr -> pool
}

// New creates a client for the given node addresses (host:port).
func New(nodes ...string) *Client {
	ring := cluster.NewRing(cluster.DefaultReplicas, nil)
	ring.Add(nodes...)
	return &Client{
		ring:        ring,
		dialTimeout: 2 * time.Second,
		ioTimeout:   2 * time.Second,
		pools:       make(map[string]*connPool),
	}
}

// AddNode adds a node to the ring at runtime.
func (c *Client) AddNode(addr string) { c.ring.Add(addr) }

// RemoveNode removes a node from the ring at runtime.
func (c *Client) RemoveNode(addr string) { c.ring.Remove(addr) }

// pool returns (creating if needed) the connection pool for an address.
func (c *Client) pool(addr string) *connPool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pools[addr]
	if !ok {
		p = newConnPool(addr, c.dialTimeout)
		c.pools[addr] = p
	}
	return p
}

// nodeFor returns the address responsible for key.
func (c *Client) nodeFor(key string) (string, error) {
	if c.ring.IsEmpty() {
		return "", ErrNoNodes
	}
	return c.ring.Get(key), nil
}

// withConn runs fn against a pooled connection for key's node, handling
// checkout/return and discarding the connection on error.
func (c *Client) withConn(key string, fn func(*bufio.ReadWriter) error) error {
	addr, err := c.nodeFor(key)
	if err != nil {
		return err
	}
	p := c.pool(addr)
	conn, err := p.get()
	if err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(c.ioTimeout))
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if err := fn(rw); err != nil {
		conn.Close() // don't reuse a connection that errored
		return err
	}
	p.put(conn)
	return nil
}

// Set stores an item in the cluster.
func (c *Client) Set(item *Item) error {
	return c.withConn(item.Key, func(rw *bufio.ReadWriter) error {
		fmt.Fprintf(rw, "set %s %d %d %d\r\n", item.Key, item.Flags, item.Expiration, len(item.Value))
		rw.Write(item.Value)
		rw.WriteString("\r\n")
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line != "STORED" {
			return fmt.Errorf("memcache: unexpected set reply %q", line)
		}
		return nil
	})
}

// Get retrieves an item by key. It returns ErrCacheMiss if not found.
func (c *Client) Get(key string) (*Item, error) {
	var out *Item
	err := c.withConn(key, func(rw *bufio.ReadWriter) error {
		fmt.Fprintf(rw, "get %s\r\n", key)
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "END" {
			return ErrCacheMiss
		}
		if !strings.HasPrefix(line, "VALUE ") {
			return fmt.Errorf("memcache: unexpected get reply %q", line)
		}
		// VALUE <key> <flags> <bytes>
		parts := strings.Fields(line)
		if len(parts) < 4 {
			return fmt.Errorf("memcache: malformed VALUE line %q", line)
		}
		flags, _ := strconv.ParseUint(parts[2], 10, 32)
		nbytes, err := strconv.Atoi(parts[3])
		if err != nil {
			return fmt.Errorf("memcache: bad byte count %q", parts[3])
		}
		buf := make([]byte, nbytes+2) // include trailing \r\n
		if _, err := readFull(rw, buf); err != nil {
			return err
		}
		out = &Item{Key: key, Value: buf[:nbytes], Flags: uint32(flags)}
		// Consume the trailing END line.
		if _, err := rw.ReadString('\n'); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes a key from the cluster. Missing keys are not an error.
func (c *Client) Delete(key string) error {
	return c.withConn(key, func(rw *bufio.ReadWriter) error {
		fmt.Fprintf(rw, "delete %s\r\n", key)
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line != "DELETED" && line != "NOT_FOUND" {
			return fmt.Errorf("memcache: unexpected delete reply %q", line)
		}
		return nil
	})
}

// Close closes all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.closeAll()
	}
}

// readFull reads len(buf) bytes from the buffered reader.
func readFull(rw *bufio.ReadWriter, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := rw.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// connPool is a minimal connection pool for a single address.
type connPool struct {
	addr        string
	dialTimeout time.Duration
	mu          sync.Mutex
	idle        []net.Conn
	maxIdle     int
}

func newConnPool(addr string, dialTimeout time.Duration) *connPool {
	return &connPool{addr: addr, dialTimeout: dialTimeout, maxIdle: 8}
}

func (p *connPool) get() (net.Conn, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()
	return net.DialTimeout("tcp", p.addr, p.dialTimeout)
}

func (p *connPool) put(conn net.Conn) {
	// Clear any deadline before returning to the pool.
	conn.SetDeadline(time.Time{})
	p.mu.Lock()
	if len(p.idle) < p.maxIdle {
		p.idle = append(p.idle, conn)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	conn.Close()
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idle {
		c.Close()
	}
	p.idle = nil
}
