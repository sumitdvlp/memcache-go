package memcache

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Server.
type Config struct {
	// Addr is the TCP address to listen on, e.g. ":11211".
	Addr string
	// MaxBytes bounds the key+value data before LRU eviction kicks in.
	// Zero or negative means unbounded.
	MaxBytes int64
	// CleanupInterval controls how often the janitor removes expired keys.
	CleanupInterval time.Duration
	// Logger is optional; a default is used if nil.
	Logger *log.Logger
}

// Server is a single-node Memcache text-protocol server.
type Server struct {
	cfg      Config
	store    *Store
	stats    *serverStats
	version  string
	logger   *log.Logger
	listener net.Listener

	wg       sync.WaitGroup
	shutdown chan struct{}
	closed   int32
}

// NewServer creates a server from cfg. The store is created immediately so it
// can be used directly in tests without a network listener.
func NewServer(cfg Config) *Server {
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Server{
		cfg:      cfg,
		store:    NewStore(cfg.MaxBytes, cfg.CleanupInterval),
		stats:    &serverStats{startTime: time.Now()},
		version:  "memcache-go 0.1.0",
		logger:   logger,
		shutdown: make(chan struct{}),
	}
}

// Store exposes the underlying store (useful for tests and embedding).
func (s *Server) Store() *Store { return s.store }

// ListenAndServe binds the configured address and serves until Close is called.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until the server is closed.
func (s *Server) Serve(ln net.Listener) error {
	s.listener = ln
	s.logger.Printf("memcache-go listening on %s", ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
				return err
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(conn)
		}()
	}
}

// serveConn wraps handleConn with connection bookkeeping.
func (s *Server) serveConn(conn net.Conn) {
	s.stats.connOpened()
	defer func() {
		s.stats.connClosed()
		conn.Close()
	}()
	s.handleConn(conn)
}

// Stats returns a snapshot of current server statistics.
func (s *Server) Stats() Stats {
	hits, misses, cmdSet, evictions, expired, items, bytes := s.store.statsSnapshot()
	return Stats{
		CurrConnections:  atomic.LoadInt64(&s.stats.currConnections),
		TotalConnections: atomic.LoadInt64(&s.stats.totalConnections),
		CmdGet:           atomic.LoadUint64(&s.stats.cmdGet),
		CmdSet:           cmdSet,
		GetHits:          hits,
		GetMisses:        misses,
		Evictions:        evictions,
		Expired:          expired,
		CurrItems:        items,
		Bytes:            bytes,
		UptimeSeconds:    int64(time.Since(s.stats.startTime).Seconds()),
	}
}

// Close stops accepting new connections and waits for in-flight connections to
// finish, then stops the store janitor.
func (s *Server) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	close(s.shutdown)
	if s.listener != nil {
		s.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
	s.store.Close()
	return nil
}
