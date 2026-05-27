// Package warden ports the queue-hygiene loop from the standalone Warden
// project into Decypharr. The headline win over the external version is
// DFS-awareness: when an arr reports "path does not exist" we check whether
// DFS is currently mounted before taking any destructive action. During a
// DFS restart, items get a retry instead of being removed and re-downloaded.
//
// Two responsibilities:
//
//  1. Startup reconciliation: once DFS reports ready, sweep every arr's
//     queue and trigger a fresh import for items that failed during downtime.
//     Fixes the cascade where Decypharr restarts, arr import fails, queue
//     item sits in "warning" forever.
//
//  2. Defence loop: periodic scan of arr queues, classify warning items
//     using the same category map as external Warden, and execute the
//     user's configured action per category.
package warden

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// MountChecker is the subset of MountManager that Warden actually needs.
// Defined locally to keep pkg/warden from importing pkg/manager, which would
// create an import cycle (manager imports warden to wire it up).
type MountChecker interface {
	IsReady() bool
}

// ArrSource is anything that can enumerate the configured arrs. Storage
// in pkg/arr satisfies this with no further work.
type ArrSource interface {
	GetAll() []*arr.Arr
}

// Warden orchestrates queue cleanup and startup reconciliation.
type Warden struct {
	arrs   ArrSource
	mount  MountChecker
	logger zerolog.Logger
	cfg    WardenConfig

	mu      sync.Mutex
	running bool
}

// WardenConfig is the public knob set for both startup reconciliation and the
// Defence loop. Loaded from config.json via internal/config.
type WardenConfig struct {
	Enabled            bool
	DryRun             bool
	StartupReconcile   bool
	DefenceEnabled     bool
	DefenceInterval    string // duration string, default "30m"
	BatchSize          int    // max removals per cycle, default 10 (-1 = unlimited)
	StaggerSeconds     int    // seconds between removals, default 5
	SearchAfterCleanup bool   // re-search after retry/blocklist, default true
	Actions            map[string]string
	ReadyTimeout       string // how long startup reconcile waits for DFS, default "5m"
}

// defaultActions is the action map used when the user supplies none. Tuned
// for "do something safe by default" — most categories default to ignore so
// Warden never silently destroys data without opt-in.
func defaultActions() map[string]Action {
	return map[string]Action{
		string(StallPathMissing):   ActionRefresh, // DFS-aware: only when DFS is ready
		string(StallManualImport):  ActionIgnore,
		string(StallNoFiles):       ActionBlocklist,
		string(StallNoUpgrade):     ActionRemove,
		string(StallStalled):       ActionRetry,
		string(StallMissingItems):  ActionBlocklist,
		string(StallDangerousFile): ActionBlocklist,
		string(StallTBATitle):      ActionIgnore,
		string(StallNoMessages):    ActionIgnore,
		string(StallUnknown):       ActionIgnore,
	}
}

// resolveActions merges defaultActions with the user's overrides, validating
// each. Invalid actions are dropped with a warn log so a typo doesn't
// silently disable Warden for a whole category.
func (w *Warden) resolveActions() map[StallCategory]Action {
	merged := defaultActions()
	for cat, raw := range w.cfg.Actions {
		if !IsValidAction(raw) {
			w.logger.Warn().Str("category", cat).Str("action", raw).Msg("Warden: invalid action, keeping default")
			continue
		}
		merged[cat] = Action(raw)
	}
	out := make(map[StallCategory]Action, len(merged))
	for k, v := range merged {
		out[StallCategory(k)] = v
	}
	return out
}

// New constructs a Warden ready to be started.
func New(arrs ArrSource, mount MountChecker, cfg WardenConfig) *Warden {
	return &Warden{
		arrs:   arrs,
		mount:  mount,
		logger: logger.New("warden"),
		cfg:    cfg,
	}
}

// Start launches both Warden goroutines. Idempotent — second call is a no-op
// so we can safely call this on every manager start without leaking workers.
func (w *Warden) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	if !w.cfg.Enabled {
		w.logger.Info().Msg("Warden: disabled in config")
		return
	}

	if w.cfg.StartupReconcile {
		go w.runStartupReconciliation(ctx)
	}
	if w.cfg.DefenceEnabled {
		go w.runDefenceLoop(ctx)
	}
}

// runStartupReconciliation waits for DFS to report ready (or times out),
// then iterates every configured arr and issues a single
// RefreshMonitoredDownloads. That command makes the arr re-evaluate every
// "completed" download in its queue — so anything that failed to import
// during the recent DFS downtime gets a fresh shot now that the mount is
// back. No removals, no re-searches.
func (w *Warden) runStartupReconciliation(ctx context.Context) {
	timeout := parseDurationOr(w.cfg.ReadyTimeout, 5*time.Minute)
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	w.logger.Info().Dur("timeout", timeout).Msg("Warden: waiting for DFS ready before reconciling arr queues")
	for {
		if w.mount != nil && w.mount.IsReady() {
			break
		}
		if time.Now().After(deadline) {
			w.logger.Warn().Msg("Warden: DFS not ready within timeout; reconciling anyway")
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}

	arrs := w.arrs.GetAll()
	if len(arrs) == 0 {
		w.logger.Info().Msg("Warden: no arrs configured, skipping reconciliation")
		return
	}
	w.logger.Info().Int("arrs", len(arrs)).Msg("Warden: reconciling arr queues post-DFS-ready")
	for _, a := range arrs {
		if a == nil || a.Host == "" || a.Token == "" {
			continue
		}
		if w.cfg.DryRun {
			w.logger.Info().Str("arr", a.Name).Msg("Warden: [DRY RUN] would refresh monitored downloads")
			continue
		}
		if err := refreshDownload(ctx, a); err != nil {
			w.logger.Warn().Err(err).Str("arr", a.Name).Msg("Warden: refresh failed")
			continue
		}
		w.logger.Info().Str("arr", a.Name).Msg("Warden: refresh sent")
	}
}

// runDefenceLoop is the cleanup-loop tick. Once per interval it sweeps each
// arr's queue, classifies warning items, and applies the configured action.
func (w *Warden) runDefenceLoop(ctx context.Context) {
	interval := parseDurationOr(w.cfg.DefenceInterval, 30*time.Minute)
	t := time.NewTicker(interval)
	defer t.Stop()
	w.logger.Info().Dur("interval", interval).Msg("Warden: defence loop active")

	first := time.NewTimer(60 * time.Second)
	defer first.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-first.C:
			w.runDefenceCycle(ctx)
		case <-t.C:
			w.runDefenceCycle(ctx)
		}
	}
}

func (w *Warden) runDefenceCycle(ctx context.Context) {
	actionMap := w.resolveActions()
	arrs := w.arrs.GetAll()
	stagger := time.Duration(w.cfg.StaggerSeconds) * time.Second
	if stagger <= 0 {
		stagger = 5 * time.Second
	}
	batchCap := w.cfg.BatchSize
	if batchCap == 0 {
		batchCap = 10
	}

	dfsReady := w.mount != nil && w.mount.IsReady()
	removed := 0

	for _, a := range arrs {
		if batchCap > 0 && removed >= batchCap {
			break
		}
		if a == nil || a.Host == "" || a.Token == "" {
			continue
		}
		queue := a.GetQueue()
		refreshedThisArr := false
		for _, item := range queue {
			if batchCap > 0 && removed >= batchCap {
				break
			}
			if !IsStalled(item) {
				continue
			}
			category := ClassifyStall(item)
			action := actionMap[category]
			if action == "" {
				action = ActionIgnore
			}

			// DFS awareness: when a path-missing stall comes through and DFS
			// is currently down, do nothing.
			if category == StallPathMissing && !dfsReady {
				w.logger.Debug().Str("arr", a.Name).Str("title", item.Title).
					Msg("Warden: skipping path_missing (DFS not ready)")
				continue
			}

			logEv := w.logger.Info().Str("arr", a.Name).
				Str("title", item.Title).Str("category", string(category)).
				Str("action", string(action))

			if w.cfg.DryRun {
				logEv.Msg("Warden: [DRY RUN] would act")
				continue
			}

			switch action {
			case ActionIgnore:
				logEv.Msg("Warden: ignored")
				continue
			case ActionRefresh:
				if refreshedThisArr {
					logEv.Msg("Warden: refresh already sent this cycle for this arr, skipping")
					continue
				}
				if err := refreshDownload(ctx, a); err != nil {
					w.logger.Warn().Err(err).Str("arr", a.Name).Msg("Warden: refresh failed")
				} else {
					logEv.Msg("Warden: refreshed download")
					refreshedThisArr = true
				}
			case ActionRemove, ActionRetry, ActionBlocklist:
				blocklist := action == ActionBlocklist
				if err := removeQueueItem(ctx, a, item.Id, blocklist); err != nil {
					w.logger.Warn().Err(err).Str("arr", a.Name).Str("title", item.Title).
						Msg("Warden: remove failed")
					continue
				}
				logEv.Msg("Warden: removed")
				removed++
				if w.cfg.SearchAfterCleanup && (action == ActionRetry || action == ActionBlocklist) {
					_ = refreshDownload(ctx, a)
					refreshedThisArr = true
				}
				if stagger > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(stagger):
					}
				}
			}
		}
	}
	if removed > 0 {
		w.logger.Info().Int("removed", removed).Msg("Warden: defence cycle complete")
	}
}

func parseDurationOr(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// FromConfig builds a WardenConfig from the application config block. Kept
// as a separate constructor so the import boundary stays one-way:
// internal/config knows nothing about Warden's runtime types.
func FromConfig(c config.Warden) WardenConfig {
	return WardenConfig{
		Enabled:            c.Enabled,
		DryRun:             c.DryRun,
		StartupReconcile:   c.StartupReconcile,
		DefenceEnabled:     c.DefenceEnabled,
		DefenceInterval:    c.DefenceInterval,
		BatchSize:          c.BatchSize,
		StaggerSeconds:     c.StaggerSeconds,
		SearchAfterCleanup: c.SearchAfterCleanup,
		Actions:            c.Actions,
		ReadyTimeout:       c.ReadyTimeout,
	}
}
