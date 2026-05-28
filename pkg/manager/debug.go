package manager

import (
	"runtime"

	"github.com/sirrobot01/decypharr/pkg/version"
)

// DebugState returns a lightweight, read-only snapshot of internal runtime
// state for the GET /api/debug/state endpoint. It's cheap to call (no disk
// walk — DiskSize is O(1)) and safe at any time, so it can be polled during a
// soak test to watch the fork's caches, queues and memory without log-grepping.
func (m *Manager) DebugState() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	out := map[string]any{
		"version":      version.GetInfo().String(),
		"goroutines":   runtime.NumGoroutine(),
		"mem_alloc_mb": ms.Alloc / (1024 * 1024),
		"mem_sys_mb":   ms.Sys / (1024 * 1024),
		"gc_cycles":    ms.NumGC,
	}
	if m.nzbFailCache != nil {
		out["nzb_fail_cache_entries"] = m.nzbFailCache.Len()
	}
	if m.torrentFailCache != nil {
		out["torrent_fail_cache_entries"] = m.torrentFailCache.Len()
	}
	if m.nzbContentHash != nil {
		out["nzb_content_bridge_entries"] = m.nzbContentHash.Size()
	}
	if m.activeStreams != nil {
		out["active_streams"] = m.activeStreams.Size()
	}
	if m.nzbQueue != nil {
		out["nzb_queue_depth"] = m.nzbQueue.Len()
	}
	if m.storage != nil {
		out["storage_disk_bytes"] = m.storage.DiskSize()
	}
	return out
}
