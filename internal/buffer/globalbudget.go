package buffer

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Global RAM budget shared across ALL Buffer instances.
//
// Each Buffer already bounds its OWN resident RAM to Config.MemorySize
// (default 32 MB). Nothing, however, bounded the SUM across many concurrent
// streams. On a busy instance a Jellyfin library scan plus an arr import storm
// open hundreds of simultaneous reads — hundreds × 32 MB drove RSS past
// 13 GiB and the process became unresponsive, after which a stale-mount
// watchdog restarted the container ("crashing and unmounting with no error").
//
// globalRAMBytes is a process-wide ceiling on bytes held in RAM across every
// buffer's block cache. When the ceiling is reached, buffers stop promoting /
// caching NEW blocks in RAM and serve from their disk-backing file instead —
// slower, but bounded. They never error and never block on it, so correctness
// is unaffected; only the RAM/throughput trade-off shifts under heavy fan-out.
//
// Default 1 GiB. Override with DECYPHARR_BUFFER_GLOBAL_MEM_MB (0 = unlimited).
var (
	globalRAMBytes atomic.Int64
	globalRAMLimit = defaultGlobalRAMLimit()
)

const defaultGlobalRAMLimitBytes int64 = 1 << 30 // 1 GiB

func defaultGlobalRAMLimit() int64 {
	if v := os.Getenv("DECYPHARR_BUFFER_GLOBAL_MEM_MB"); v != "" {
		if mb, err := strconv.ParseInt(v, 10, 64); err == nil && mb >= 0 {
			return mb << 20
		}
	}
	return defaultGlobalRAMLimitBytes
}

// globalRAMHasRoom reports whether one more block fits under the global
// ceiling. A limit <= 0 means unlimited.
func globalRAMHasRoom() bool {
	return globalRAMLimit <= 0 || globalRAMBytes.Load()+blockSize <= globalRAMLimit
}

// globalRAMAdd accounts one newly RAM-resident block.
func globalRAMAdd() { globalRAMBytes.Add(blockSize) }

// globalRAMSub releases n bytes from the global accounting.
func globalRAMSub(n int64) {
	if n > 0 {
		globalRAMBytes.Add(-n)
	}
}

// GlobalRAMBytes returns the bytes currently held in RAM across all buffers.
// Exposed for observability (e.g. the /api/debug/state endpoint).
func GlobalRAMBytes() int64 { return globalRAMBytes.Load() }

// GlobalRAMLimit returns the configured global RAM ceiling in bytes
// (0 = unlimited).
func GlobalRAMLimit() int64 { return globalRAMLimit }
