package memcache

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Memcache text protocol replies.
const (
	respStored    = "STORED\r\n"
	respDeleted   = "DELETED\r\n"
	respNotFound  = "NOT_FOUND\r\n"
	respEnd       = "END\r\n"
	respOK        = "OK\r\n"
	respErr       = "ERROR\r\n"
	maxValueBytes = 1 << 20 // 1 MiB, same default cap as memcached
)

// handleConn serves a single client connection using the text protocol until
// the client disconnects or sends "quit".
func (s *Server) handleConn(rw io.ReadWriter) {
	reader := bufio.NewReader(rw)
	writer := bufio.NewWriter(rw)
	defer writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		cmd := strings.ToLower(fields[0])

		switch cmd {
		case "get", "gets":
			s.cmdGetHandler(writer, fields[1:])
		case "set":
			if !s.cmdSetHandler(reader, writer, fields[1:]) {
				return
			}
		case "delete":
			s.cmdDeleteHandler(writer, fields[1:])
		case "flush_all":
			s.store.Flush()
			writeString(writer, respOK)
		case "stats":
			s.cmdStatsHandler(writer)
		case "version":
			writeString(writer, "VERSION "+s.version+"\r\n")
		case "quit":
			return
		default:
			writeString(writer, respErr)
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// cmdGetHandler handles "get <key>*".
func (s *Server) cmdGetHandler(w *bufio.Writer, keys []string) {
	if len(keys) == 0 {
		writeString(w, respErr)
		return
	}
	for _, key := range keys {
		s.stats.incrGet()
		val, flags, ok := s.store.Get(key)
		if !ok {
			continue
		}
		// VALUE <key> <flags> <bytes>\r\n<data>\r\n
		fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, flags, len(val))
		w.Write(val)
		writeString(w, "\r\n")
	}
	writeString(w, respEnd)
}

// cmdSetHandler handles "set <key> <flags> <exptime> <bytes>\r\n<data>\r\n".
// It returns false if the connection should be closed (fatal read error).
func (s *Server) cmdSetHandler(r *bufio.Reader, w *bufio.Writer, args []string) bool {
	if len(args) < 4 {
		writeString(w, respErr)
		return true
	}
	key := args[0]
	flags, ferr := strconv.ParseUint(args[1], 10, 32)
	exptime, eerr := strconv.ParseInt(args[2], 10, 64)
	nbytes, berr := strconv.Atoi(args[3])
	if ferr != nil || eerr != nil || berr != nil || nbytes < 0 || nbytes > maxValueBytes {
		writeString(w, "CLIENT_ERROR bad command line format\r\n")
		return true
	}

	// Read the data block plus trailing \r\n.
	body := make([]byte, nbytes+2)
	if _, err := io.ReadFull(r, body); err != nil {
		return false
	}
	data := body[:nbytes]

	s.store.Set(key, data, uint32(flags), expTimeToTTL(exptime))
	writeString(w, respStored)
	return true
}

// cmdDeleteHandler handles "delete <key>".
func (s *Server) cmdDeleteHandler(w *bufio.Writer, args []string) {
	if len(args) < 1 {
		writeString(w, respErr)
		return
	}
	if s.store.Delete(args[0]) {
		writeString(w, respDeleted)
	} else {
		writeString(w, respNotFound)
	}
}

// cmdStatsHandler writes the STAT lines followed by END.
func (s *Server) cmdStatsHandler(w *bufio.Writer) {
	st := s.Stats()
	lines := []struct {
		name string
		val  interface{}
	}{
		{"pid", 0},
		{"uptime", st.UptimeSeconds},
		{"curr_connections", st.CurrConnections},
		{"total_connections", st.TotalConnections},
		{"cmd_get", st.CmdGet},
		{"cmd_set", st.CmdSet},
		{"get_hits", st.GetHits},
		{"get_misses", st.GetMisses},
		{"evictions", st.Evictions},
		{"expired_unfetched", st.Expired},
		{"curr_items", st.CurrItems},
		{"bytes", st.Bytes},
	}
	for _, l := range lines {
		fmt.Fprintf(w, "STAT %s %v\r\n", l.name, l.val)
	}
	writeString(w, respEnd)
}

// expTimeToTTL converts the memcached exptime convention into a duration.
// 0 means no expiry. Values <= 60*60*24*30 are relative seconds; larger values
// are absolute Unix timestamps.
func expTimeToTTL(exptime int64) time.Duration {
	if exptime == 0 {
		return 0
	}
	const thirtyDays = 60 * 60 * 24 * 30
	if exptime <= thirtyDays {
		if exptime < 0 {
			return time.Nanosecond // already expired
		}
		return time.Duration(exptime) * time.Second
	}
	d := time.Until(time.Unix(exptime, 0))
	if d <= 0 {
		return time.Nanosecond
	}
	return d
}

func writeString(w *bufio.Writer, s string) {
	_, _ = w.WriteString(s)
}
