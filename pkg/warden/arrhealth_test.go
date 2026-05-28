package warden

import (
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTrackerWithClock() (*arrHealthTracker, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	tr := newArrHealthTracker()
	tr.now = clk.now
	return tr, clk
}

func arrHost(h string) *arr.Arr { return &arr.Arr{Host: h} }

func TestArrHealth_AvailableUntilThreshold(t *testing.T) {
	tr, _ := newTrackerWithClock()
	a := arrHost("http://radarr:7878")

	if !tr.available(a) {
		t.Fatal("should be available initially")
	}
	// threshold is 3: first two failures keep it available.
	for i := 0; i < tr.threshold-1; i++ {
		if entered := tr.recordFailure(a); entered {
			t.Fatalf("failure %d should not enter cooldown", i+1)
		}
		if !tr.available(a) {
			t.Fatalf("should still be available after %d failures", i+1)
		}
	}
	// the threshold-th failure enters cooldown.
	if entered := tr.recordFailure(a); !entered {
		t.Fatal("threshold failure should enter cooldown (return true once)")
	}
	if tr.available(a) {
		t.Fatal("should be on cooldown after threshold failures")
	}
}

func TestArrHealth_RecoversAfterCooldown(t *testing.T) {
	tr, clk := newTrackerWithClock()
	a := arrHost("http://lidarr:8686")
	for i := 0; i < tr.threshold; i++ {
		tr.recordFailure(a)
	}
	if tr.available(a) {
		t.Fatal("should be on cooldown")
	}
	clk.advance(tr.baseCooldown - time.Second)
	if tr.available(a) {
		t.Fatal("should still be cooling down just before expiry")
	}
	clk.advance(2 * time.Second)
	if !tr.available(a) {
		t.Fatal("should be re-probeable after cooldown lapses")
	}
}

func TestArrHealth_SuccessClearsPenalty(t *testing.T) {
	tr, _ := newTrackerWithClock()
	a := arrHost("http://sonarr:8989")
	for i := 0; i < tr.threshold; i++ {
		tr.recordFailure(a)
	}
	tr.recordSuccess(a)
	if !tr.available(a) {
		t.Fatal("success should clear the cooldown")
	}
	// counter reset: it takes a full threshold again to re-enter cooldown.
	for i := 0; i < tr.threshold-1; i++ {
		if tr.recordFailure(a) {
			t.Fatal("failure counter should have reset after success")
		}
	}
}

func TestArrHealth_BackoffDoublesAndCaps(t *testing.T) {
	tr, clk := newTrackerWithClock()
	a := arrHost("http://radarr:7878")
	key := arrKey(a)

	// Drive failures while the clock stays fixed so skipUntil deltas are exact.
	expect := []time.Duration{
		tr.baseCooldown,     // fails == threshold (extra 0)
		tr.baseCooldown * 2, // extra 1
		tr.baseCooldown * 4, // extra 2
	}
	// first threshold-1 failures don't set a cooldown
	for i := 0; i < tr.threshold-1; i++ {
		tr.recordFailure(a)
	}
	for i, want := range expect {
		tr.recordFailure(a)
		got := tr.skipUntil[key].Sub(clk.t)
		if got != want {
			t.Fatalf("failure %d cooldown = %s, want %s", i, got, want)
		}
	}
	// keep failing until capped at maxCooldown
	for i := 0; i < 10; i++ {
		tr.recordFailure(a)
	}
	if got := tr.skipUntil[key].Sub(clk.t); got != tr.maxCooldown {
		t.Fatalf("cooldown should cap at %s, got %s", tr.maxCooldown, got)
	}
}

func TestArrHealth_RecordFailureLogsOncePerWindow(t *testing.T) {
	tr, clk := newTrackerWithClock()
	a := arrHost("http://lidarr:8686")
	for i := 0; i < tr.threshold-1; i++ {
		tr.recordFailure(a)
	}
	if !tr.recordFailure(a) {
		t.Fatal("entering cooldown should return true")
	}
	if tr.recordFailure(a) {
		t.Fatal("further failures within the same window should return false")
	}
	// after the window lapses, a new failure opens a new window -> true again.
	clk.advance(2 * tr.maxCooldown)
	if !tr.recordFailure(a) {
		t.Fatal("entering a new cooldown window should return true again")
	}
}

func TestArrHealth_EmptyKey(t *testing.T) {
	tr, _ := newTrackerWithClock()
	if tr.available(nil) {
		t.Fatal("nil arr must not be available")
	}
	if tr.recordFailure(nil) {
		t.Fatal("nil arr failure is a no-op")
	}
	tr.recordSuccess(nil) // must not panic
}

func TestArrHealth_ConcurrentAccess(t *testing.T) {
	tr, _ := newTrackerWithClock()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			a := arrHost("http://arr" + string(rune('A'+w)) + ":8989")
			for i := 0; i < 100; i++ {
				tr.recordFailure(a)
				_ = tr.available(a)
				if i%10 == 0 {
					tr.recordSuccess(a)
				}
			}
		}(w)
	}
	wg.Wait()
}
