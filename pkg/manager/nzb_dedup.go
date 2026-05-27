package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

// nzbFailCache remembers NZBs that recently failed to process so re-submission
// loops (Lidarr's redownload-on-failure, Sonarr's grab-history retry) don't
// hammer the usenet provider re-fetching dead articles. Entries time out
// after TTL so transient failures (network blip during one cycle) can recover.
//
// Lookup key is SHA-256 of NZB content bytes — deterministic across indexer
// hops, so the same release submitted from a different Prowlarr indexer
// matches if the bytes are identical.
type nzbFailCache struct {
	mu      sync.RWMutex
	entries map[string]nzbFailEntry
	ttl     time.Duration
	max     int
}

type nzbFailEntry struct {
	reason string
	at     time.Time
}

func newNzbFailCache(ttl time.Duration, max int) *nzbFailCache {
	return &nzbFailCache{
		entries: make(map[string]nzbFailEntry),
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

// Lookup returns (reason, true) when the content hash is in the cache and
// not yet expired. The reason text was the original failure cause and is
// returned to the caller so it can be logged or surfaced upstream.
func (c *nzbFailCache) Lookup(hash string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[hash]
	if !ok {
		return "", false
	}
	if time.Since(e.at) > c.ttl {
		return "", false
	}
	return e.reason, true
}

// Record stores a failure. Drops the oldest entries when the cap is hit so
// the cache can never balloon past `max` entries even under sustained
// failure-storm conditions.
func (c *nzbFailCache) Record(hash, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[hash] = nzbFailEntry{reason: reason, at: time.Now()}
	if len(c.entries) <= c.max {
		return
	}
	// Evict oldest. O(n) but n is bounded by `max`, and writes here are
	// rare (one per failed NZB processing). A heap would be overkill.
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

// Cleanup walks the cache and drops expired entries. Cheap enough to run
// every minute; uses a write lock so concurrent reads pause briefly.
func (c *nzbFailCache) Cleanup() {
	cutoff := time.Now().Add(-c.ttl)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if e.at.Before(cutoff) {
			delete(c.entries, k)
		}
	}
}

// isPermanentNZBFailure inspects an error returned from usenet processing
// and decides whether the failure should poison the cache. We only cache
// failures that are unlikely to fix themselves on retry — dead articles,
// invalid content, no valid files. Transient errors (timeouts, connection
// problems) are NOT cached so a temporary glitch doesn't permanently
// blacklist a viable NZB.
func isPermanentNZBFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range []string{
		"no valid files found",
		"no such article",      // NNTP code 430
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
