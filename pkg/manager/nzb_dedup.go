package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

// failCache remembers submissions that recently failed so re-submission loops
// (Lidarr's redownload-on-failure, Sonarr's grab-history retry, arr re-grabs)
// don't hammer the upstream provider. It is used for two distinct streams:
//
//   - NZBs: keyed by SHA-256 of NZB content. 24h TTL — dead articles don't
//     come back, so a long cooldown is safe.
//   - Torrents: keyed by InfoHash. Short TTL (~30m) — a torrent that isn't
//     cached on debrid right now may become cached later, so we only want to
//     rate-limit re-submission, not permanently blocklist it.
//
// Entries time out after TTL so transient failures (a network blip during one
// cycle) recover on their own.
type failCache struct {
	mu      sync.RWMutex
	entries map[string]failCacheEntry
	ttl     time.Duration
	max     int
}

type failCacheEntry struct {
	reason string
	at     time.Time
}

func newFailCache(ttl time.Duration, max int) *failCache {
	return &failCache{
		entries: make(map[string]failCacheEntry),
		ttl:     ttl,
		max:     max,
	}
}

// hashNZBContent returns a stable identifier for an NZB payload. We use this
// instead of filename because Lidarr may rename releases between attempts.
func hashNZBContent(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// Lookup returns (reason, true) when the key is in the cache and not expired.
func (c *failCache) Lookup(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok {
		return "", false
	}
	if time.Since(e.at) > c.ttl {
		return "", false
	}
	return e.reason, true
}

// Record stores a failure. Drops the oldest entries when the cap is hit so the
// cache can never balloon past `max` even under a sustained failure storm.
func (c *failCache) Record(key, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = failCacheEntry{reason: reason, at: time.Now()}
	if len(c.entries) <= c.max {
		return
	}
	// Evict oldest. O(n) but n is bounded by `max`, and writes are rare.
	var oldestKey string
	var oldestAt time.Time
	first := true
	for k, e := range c.entries {
		if first || e.at.Before(oldestAt) {
			oldestKey = k
			oldestAt = e.at
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

// Cleanup walks the cache and drops expired entries. Cheap enough to run on a
// timer; uses a write lock so concurrent reads pause briefly.
func (c *failCache) Cleanup() {
	cutoff := time.Now().Add(-c.ttl)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if e.at.Before(cutoff) {
			delete(c.entries, k)
		}
	}
}

// Len returns the current number of cached entries (for debug/metrics).
func (c *failCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// isPermanentNZBFailure inspects an error returned from usenet processing and
// decides whether the failure should poison the cache. We only cache failures
// unlikely to fix themselves on retry — dead articles, invalid content, no
// valid files. Transient errors (timeouts, connection problems) are NOT cached
// so a temporary glitch doesn't permanently blacklist a viable NZB.
func isPermanentNZBFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range []string{
		"no valid files found",
		"no valid file groups found", // parser.go:148 uses "groups" variant
		"no such article",            // NNTP code 430
		"article_not_found",
		"invalid nzb content",
		"nzb content is empty",
		"all files were skipped",
	} {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

// isTransientSubmitError reports whether a torrent submission error is the kind
// that is likely to recover on its own shortly (network blips, timeouts,
// rate-limit backoff). Such errors should NOT be put on cooldown — the arr's
// next re-grab should be allowed through promptly.
func isTransientSubmitError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range []string{
		"context deadline exceeded",
		"context canceled",
		"timeout",
		"timed out",
		"connection refused",
		"connection reset",
		"no such host",
		"eof",
		"temporary failure",
		"i/o timeout",
		"too many active downloads", // handled by requeue elsewhere; never cooldown
	} {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

// isPermanentTorrentFailure reports whether a torrent submission failure is
// clearly terminal (legal takedown, permanently uncached). Used to log/observe
// the difference; the cooldown itself is applied to all non-transient failures.
func isPermanentTorrentFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range []string{
		"status: 451", // RealDebrid: unavailable for legal reasons (DMCA)
		"451",         // generic legal-block
		"infringing",
		"not cached", // provider has no cached copy (with uncached disabled)
		"file unavailable",
		"dmca",
	} {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}
