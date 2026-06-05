package manager

import (
	"runtime"

	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/pkg/version"
)

// DebugState returns a lightweight, read-only snapshot of runtime memory plus
// the global stream-buffer RAM accounting, served at GET /api/debug/state.
//
// It is cheap (no disk walk, no locks beyond runtime's own) and safe to poll
// continuously, so a soak collector can watch the process's memory, goroutine
// count, GC activity and buffer RAM over days without grepping logs. The field
// names match the keys soak-collect.py already reads.
func (m *Manager) DebugState() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	const mb = 1024 * 1024
	return map[string]any{
		"version":                version.GetInfo().String(),
		"goroutines":             runtime.NumGoroutine(),
		"mem_alloc_mb":           ms.Alloc / mb,
		"mem_sys_mb":             ms.Sys / mb,
		"gc_cycles":              ms.NumGC,
		"buffer_global_ram_mb":   buffer.GlobalRAMBytes() / mb,
		"buffer_global_limit_mb": buffer.GlobalRAMLimit() / mb,
	}
}
