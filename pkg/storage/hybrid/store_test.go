package hybrid

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestMain points the global config path at a temp dir so logger.New()
// (called inside the store) can initialise without failing on a missing
// config directory in the test sandbox.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "hybrid-test-cfg")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// newTestStore returns a store backed by a temp .db file. SyncInterval=0 means
// fsync on every write, giving deterministic durability for recovery tests.
func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := New(Config{
		DataPath:            path,
		CacheSize:           16,
		SyncInterval:        0,
		CompactionThreshold: 0.5,
		AutoCompact:         false,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, path
}

func mustPut(t *testing.T, s *Store, k, v string, meta *EntryMeta) {
	t.Helper()
	if err := s.Put(k, []byte(v), meta); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, s *Store, k, want string) {
	t.Helper()
	got, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	mustPut(t, s, "a", "alpha", nil)
	mustPut(t, s, "b", "bravo", nil)
	mustGet(t, s, "a", "alpha")
	mustGet(t, s, "b", "bravo")

	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	if !s.Exists("a") || s.Exists("missing") {
		t.Fatal("Exists wrong")
	}
}

func TestGetMissingErrors(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	if _, err := s.Get("nope"); err == nil {
		t.Fatal("expected error getting missing key")
	}
}

func TestOverwriteReturnsLatest(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	mustPut(t, s, "k", "v1", nil)
	mustPut(t, s, "k", "v2", nil)
	mustPut(t, s, "k", "v3", nil)
	mustGet(t, s, "k", "v3")
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after overwrites", s.Len())
	}
}

func TestDeleteSemantics(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	mustPut(t, s, "k", "v", nil)
	if err := s.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get("k"); err == nil {
		t.Fatal("expected miss after delete")
	}
	if s.Exists("k") {
		t.Fatal("Exists should be false after delete")
	}
	if err := s.Delete("k"); err == nil {
		t.Fatal("expected error deleting missing key")
	}
}

func TestCategorySecondaryIndex(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	mustPut(t, s, "1", "x", &EntryMeta{Category: "sonarr", Provider: "torbox"})
	mustPut(t, s, "2", "y", &EntryMeta{Category: "sonarr", Provider: "realdebrid"})
	mustPut(t, s, "3", "z", &EntryMeta{Category: "lidarr", Provider: "torbox"})

	if got := s.CountByCategory("sonarr"); got != 2 {
		t.Fatalf("CountByCategory(sonarr) = %d, want 2", got)
	}
	if got := len(s.FilterByCategory("lidarr")); got != 1 {
		t.Fatalf("FilterByCategory(lidarr) = %d, want 1", got)
	}
	cats := s.Categories()
	if len(cats) != 2 {
		t.Fatalf("Categories = %v, want 2 distinct", cats)
	}
	// Deleting updates the secondary index.
	if err := s.Delete("3"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := s.CountByCategory("lidarr"); got != 0 {
		t.Fatalf("CountByCategory(lidarr) after delete = %d, want 0", got)
	}
}

func TestForEachAndKeys(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		mustPut(t, s, k, v, nil)
	}
	seen := map[string]string{}
	if err := s.ForEach(func(k string, v []byte) error {
		seen[k] = string(v)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if len(seen) != len(want) {
		t.Fatalf("ForEach saw %d entries, want %d", len(seen), len(want))
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("ForEach[%q] = %q, want %q", k, seen[k], v)
		}
	}
	if len(s.Keys()) != 3 {
		t.Fatalf("Keys = %d, want 3", len(s.Keys()))
	}
}

// TestCrashRecovery is the headline guarantee: after Close, reopening the store
// on the same file must rebuild the full index from the log — preserving latest
// values, honouring deletes, and surviving overwrites.
func TestCrashRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recover.db")
	cfg := Config{DataPath: path, CacheSize: 8, SyncInterval: 0, CompactionThreshold: 0.5}

	s1, err := New(cfg)
	if err != nil {
		t.Fatalf("New#1: %v", err)
	}
	mustPut(t, s1, "keep", "final", &EntryMeta{Category: "radarr"})
	mustPut(t, s1, "keep", "OVERWRITTEN", &EntryMeta{Category: "radarr"}) // overwrite
	mustPut(t, s1, "keep", "final2", &EntryMeta{Category: "radarr"})      // latest
	mustPut(t, s1, "gone", "doomed", nil)
	if err := s1.Delete("gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	// Reopen — simulates a restart / crash recovery.
	s2, err := New(cfg)
	if err != nil {
		t.Fatalf("New#2 (recovery): %v", err)
	}
	defer s2.Close()

	mustGet(t, s2, "keep", "final2") // latest overwrite survived
	if _, err := s2.Get("gone"); err == nil {
		t.Fatal("deleted key resurrected after recovery")
	}
	if s2.Len() != 1 {
		t.Fatalf("Len after recovery = %d, want 1", s2.Len())
	}
	if got := s2.CountByCategory("radarr"); got != 1 {
		t.Fatalf("category index not rebuilt: CountByCategory(radarr) = %d, want 1", got)
	}
}

func TestCompactionPreservesLiveData(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), nil)
	}
	// Delete half to create reclaimable space.
	for i := 0; i < 20; i += 2 {
		if err := s.Delete(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Delete k%d: %v", i, err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Survivors intact, deleted gone.
	for i := 1; i < 20; i += 2 {
		mustGet(t, s, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	for i := 0; i < 20; i += 2 {
		if s.Exists(fmt.Sprintf("k%d", i)) {
			t.Fatalf("k%d should be gone after compaction", i)
		}
	}
	if s.Len() != 10 {
		t.Fatalf("Len after compaction = %d, want 10", s.Len())
	}
}

// TestCompactionThenRecovery ensures the compacted log is itself recoverable.
func TestCompactionThenRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compact-recover.db")
	cfg := Config{DataPath: path, CacheSize: 8, SyncInterval: 0, CompactionThreshold: 0.5}
	s1, err := New(cfg)
	if err != nil {
		t.Fatalf("New#1: %v", err)
	}
	for i := 0; i < 10; i++ {
		mustPut(t, s1, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), nil)
	}
	for i := 0; i < 10; i += 2 {
		_ = s1.Delete(fmt.Sprintf("k%d", i))
	}
	if err := s1.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := New(cfg)
	if err != nil {
		t.Fatalf("New#2: %v", err)
	}
	defer s2.Close()
	if s2.Len() != 5 {
		t.Fatalf("Len after compact+recovery = %d, want 5", s2.Len())
	}
	mustGet(t, s2, "k1", "v1")
	if s2.Exists("k0") {
		t.Fatal("k0 should stay deleted across compact+recovery")
	}
}

// TestConcurrentAccess exercises the RWMutex. Run with -race to catch data
// races. Each goroutine owns a disjoint key range so assertions stay clean.
func TestConcurrentAccess(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	const workers = 8
	const perWorker = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i)
				if err := s.Put(k, []byte("v"), nil); err != nil {
					t.Errorf("Put(%q): %v", k, err)
					return
				}
				if _, err := s.Get(k); err != nil {
					t.Errorf("Get(%q): %v", k, err)
					return
				}
			}
		}(w)
	}
	// Concurrent readers hammering the index/stats.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_ = s.Len()
				_ = s.Keys()
				_ = s.GetStats()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if s.Len() != workers*perWorker {
		t.Fatalf("Len = %d, want %d", s.Len(), workers*perWorker)
	}
}

// TestRecoveryTruncatesTornTail proves a torn/partial final write does NOT
// brick the store: recovery truncates the garbage and recovers all complete
// records, and the store stays writable afterwards.
func TestRecoveryTruncatesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.db")
	cfg := Config{DataPath: path, CacheSize: 8, SyncInterval: 0}

	s1, err := New(cfg)
	if err != nil {
		t.Fatalf("New#1: %v", err)
	}
	for i := 0; i < 5; i++ {
		mustPut(t, s1, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), nil)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a torn final write: a record header claiming 1000 key bytes,
	// but only a few bytes actually follow (crash mid-append).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0xE8, 0x03, 0x00, 0x00, 0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close corrupt file: %v", err)
	}

	// Reopen: must recover, not brick.
	s2, err := New(cfg)
	if err != nil {
		t.Fatalf("New#2 bricked on torn tail (should have recovered): %v", err)
	}
	defer s2.Close()

	if s2.Len() != 5 {
		t.Fatalf("Len after torn-tail recovery = %d, want 5", s2.Len())
	}
	for i := 0; i < 5; i++ {
		mustGet(t, s2, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// Writable after recovery (writePos was repaired by truncation).
	mustPut(t, s2, "after", "ok", nil)
	mustGet(t, s2, "after", "ok")
}

// TestRecoveryTruncatesPartialHeader covers the other torn case: a crash that
// left only a couple of bytes of the next record's length prefix.
func TestRecoveryTruncatesPartialHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.db")
	cfg := Config{DataPath: path, CacheSize: 8, SyncInterval: 0}

	s1, err := New(cfg)
	if err != nil {
		t.Fatalf("New#1: %v", err)
	}
	mustPut(t, s1, "only", "value", nil)
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0x05, 0x00}); err != nil { // 2 of 4 length bytes
		t.Fatalf("write partial header: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := New(cfg)
	if err != nil {
		t.Fatalf("New#2 bricked on partial header: %v", err)
	}
	defer s2.Close()
	if s2.Len() != 1 {
		t.Fatalf("Len after partial-header recovery = %d, want 1", s2.Len())
	}
	mustGet(t, s2, "only", "value")
}
