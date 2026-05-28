package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIsPermanentNZBFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		// Regression guard for the D1-fix: parser.go:148 emits the "groups"
		// variant. Before the fix this returned false and the NZB retried
		// every 15 minutes forever.
		{"file_groups_variant", errors.New("usenet process failed: no valid file groups found in NZB"), true},
		{"no_valid_files", errors.New("no valid files found in NZB"), true},
		{"no_such_article", errors.New("NNTP ARTICLE_NOT_FOUND (code 430): No such article"), true},
		{"empty", errors.New("nzb content is empty"), true},
		{"transient_timeout", errors.New("usenet processing timed out after 5m"), false},
		{"random", errors.New("some other error"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isPermanentNZBFailure(c.err); got != c.want {
				t.Fatalf("isPermanentNZBFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestIsTransientSubmitError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline", context.DeadlineExceeded, true},
		{"canceled", context.Canceled, true},
		{"conn_refused", errors.New("dial tcp: connection refused"), true},
		{"io_timeout", errors.New("read: i/o timeout"), true},
		{"too_many", errors.New("too many active downloads"), true},
		{"rd_451", errors.New("realdebrid API error: Status: 451"), false},
		{"not_cached", errors.New("torrent: foo.mkv not cached"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTransientSubmitError(c.err); got != c.want {
				t.Fatalf("isTransientSubmitError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestIsPermanentTorrentFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"rd_451", errors.New("realdebrid API error: Status: 451"), true},
		{"not_cached", errors.New("torrent: foo.mkv not cached"), true},
		{"transient", errors.New("connection refused"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isPermanentTorrentFailure(c.err); got != c.want {
				t.Fatalf("isPermanentTorrentFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestFailCache_RecordLookupExpiry(t *testing.T) {
	c := newFailCache(50*time.Millisecond, 100)
	c.Record("hash1", "boom")

	if reason, hit := c.Lookup("hash1"); !hit || reason != "boom" {
		t.Fatalf("expected hit with reason 'boom', got hit=%v reason=%q", hit, reason)
	}
	if _, hit := c.Lookup("missing"); hit {
		t.Fatal("expected miss for unknown key")
	}

	time.Sleep(70 * time.Millisecond)
	if _, hit := c.Lookup("hash1"); hit {
		t.Fatal("expected entry to be expired after TTL")
	}
}

func TestFailCache_EvictsOldestAtCap(t *testing.T) {
	c := newFailCache(time.Hour, 3)
	for i := 0; i < 3; i++ {
		c.Record(fmt.Sprintf("h%d", i), "r")
		time.Sleep(2 * time.Millisecond) // ensure distinct timestamps
	}
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}
	// Adding a 4th should evict the oldest (h0).
	c.Record("h3", "r")
	if c.Len() != 3 {
		t.Fatalf("expected cap to hold at 3, got %d", c.Len())
	}
	if _, hit := c.Lookup("h0"); hit {
		t.Fatal("expected oldest entry h0 to be evicted")
	}
	if _, hit := c.Lookup("h3"); !hit {
		t.Fatal("expected newest entry h3 to be present")
	}
}
