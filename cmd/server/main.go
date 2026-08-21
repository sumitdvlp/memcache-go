// Command server runs a single memcache-go node.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kusu/memcache-go/memcache"
)

func main() {
	addr := flag.String("addr", envOr("MEMCACHE_ADDR", ":11211"), "TCP listen address")
	maxMB := flag.Int64("max-mb", envInt64("MEMCACHE_MAX_MB", 64), "max cache size in MiB (0 = unbounded)")
	cleanup := flag.Duration("cleanup", time.Second, "expired-key cleanup interval")
	flag.Parse()

	cfg := memcache.Config{
		Addr:            *addr,
		MaxBytes:        *maxMB * 1024 * 1024,
		CleanupInterval: *cleanup,
		Logger:          log.New(os.Stdout, "[memcache] ", log.LstdFlags),
	}

	srv := memcache.NewServer(cfg)

	// Handle SIGINT/SIGTERM for graceful shutdown.
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errc:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	case s := <-sig:
		log.Printf("received %s, shutting down", s)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Close(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
