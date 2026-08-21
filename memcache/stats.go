package memcache

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of server-level counters.
type Stats struct {
	CurrConnections  int64
	TotalConnections int64
	CmdGet           uint64
	CmdSet           uint64
	GetHits          uint64
	GetMisses        uint64
	Evictions        uint64
	Expired          uint64
	CurrItems        int
	Bytes            int64
	UptimeSeconds    int64
}

// serverStats holds atomic connection counters at the server layer.
type serverStats struct {
	currConnections  int64
	totalConnections int64
	cmdGet           uint64
	startTime        time.Time
}

func (ss *serverStats) connOpened() {
	atomic.AddInt64(&ss.currConnections, 1)
	atomic.AddInt64(&ss.totalConnections, 1)
}

func (ss *serverStats) connClosed() {
	atomic.AddInt64(&ss.currConnections, -1)
}

func (ss *serverStats) incrGet() {
	atomic.AddUint64(&ss.cmdGet, 1)
}

// snapshot merges server-level and store-level counters into a Stats value.
func (s *Store) statsSnapshot() (getHits, getMisses, cmdSet, evictions, expired uint64, items int, bytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getHits, s.getMisses, s.cmdSet, s.evictions, s.expired, len(s.items), s.curBytes
}
