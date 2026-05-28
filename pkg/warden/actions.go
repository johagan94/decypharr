package warden

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	gourl "net/url"
	"sync"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// Action is what Warden does about a stall.
type Action string

const (
	ActionIgnore    Action = "ignore"
	ActionRemove    Action = "remove"    // remove from queue + client, no re-search, no blocklist
	ActionRetry     Action = "retry"     // remove from queue + client, then re-search same item
	ActionBlocklist Action = "blocklist" // remove + blocklist (so arr won't grab same release) + re-search
	ActionRefresh   Action = "refresh"   // ask arr to re-scan the download (use when DFS just came back up)
)

// IsValidAction reports whether the supplied string is a known action.
func IsValidAction(a string) bool {
	switch Action(a) {
	case ActionIgnore, ActionRemove, ActionRetry, ActionBlocklist, ActionRefresh:
		return true
	}
	return false
}

// fastClient is Warden's HTTP client. Unlike the shared client used elsewhere
// in arr.go (which retries 5x with backoff over ~30 seconds), this one fails
// fast: no retries, 3-second total timeout. The defence loop polls regularly,
// so transient blips will be picked up next cycle — no need to hammer a
// possibly-dead arr for half a minute on every call.
var fastClient = &http.Client{
	Timeout: 3 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 1500 * time.Millisecond,
		}).DialContext,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 2 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Don't keep idle connections around — most arrs are pinged once per
		// cycle and would just consume a slot for 30 minutes.
		MaxIdleConns:      4,
		IdleConnTimeout:   30 * time.Second,
		DisableKeepAlives: false,
		MaxConnsPerHost:   2,
	},
}

// arrUnavailable tracks how many consecutive failures Warden has seen per arr
// host. After N failures we stop trying that arr until the next process
// restart, so a permanently-down arr doesn't waste 3 seconds × every cycle.
// Cleared on first success.
// arrKey identifies an arr by host for health tracking.
func arrKey(a *arr.Arr) string {
	if a == nil {
		return ""
	}
	return a.Host
}

// arrHealthTracker tracks per-arr reachability so the defence loop doesn't
// hammer an unreachable arr every cycle, while still recovering automatically
// once it comes back. Unlike a permanent skip, a failing arr is placed on a
// cooldown with exponential backoff; when the cooldown lapses it is re-probed
// on the next cycle, and a single success clears the penalty.
//
// All state is instance-scoped (no package globals) and guarded by mu. The
// clock is injectable so the backoff is deterministically testable.
type arrHealthTracker struct {
	mu        sync.Mutex
	fails     map[string]int
	skipUntil map[string]time.Time

	threshold    int           // consecutive failures before the first cooldown
	baseCooldown time.Duration // cooldown applied at the threshold
	maxCooldown  time.Duration // cap for the exponential backoff
	now          func() time.Time
}

func newArrHealthTracker() *arrHealthTracker {
	return &arrHealthTracker{
		fails:        make(map[string]int),
		skipUntil:    make(map[string]time.Time),
		threshold:    3,
		baseCooldown: 15 * time.Minute,
		maxCooldown:  time.Hour,
		now:          time.Now,
	}
}

// available reports whether the arr should be contacted this cycle. An arr on
// cooldown becomes available again once the cooldown lapses (a re-probe). An
// empty key (nil/unconfigured arr) is never available.
func (t *arrHealthTracker) available(a *arr.Arr) bool {
	k := arrKey(a)
	if k == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	until, ok := t.skipUntil[k]
	if !ok {
		return true
	}
	return !t.now().Before(until)
}

// recordSuccess clears any accumulated penalty for the arr.
func (t *arrHealthTracker) recordSuccess(a *arr.Arr) {
	k := arrKey(a)
	if k == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.fails, k)
	delete(t.skipUntil, k)
}

// recordFailure registers a failed contact and returns true only when the arr
// newly enters a cooldown window, so the caller can log it once rather than
// every cycle. The cooldown grows exponentially (baseCooldown << extra) up to
// maxCooldown for an arr that stays down.
func (t *arrHealthTracker) recordFailure(a *arr.Arr) bool {
	k := arrKey(a)
	if k == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	alreadyCoolingDown := false
	if until, ok := t.skipUntil[k]; ok && now.Before(until) {
		alreadyCoolingDown = true
	}

	t.fails[k]++
	if t.fails[k] < t.threshold {
		return false
	}

	cooldown := t.baseCooldown
	for i := 0; i < t.fails[k]-t.threshold && cooldown < t.maxCooldown; i++ {
		cooldown *= 2
	}
	if cooldown > t.maxCooldown {
		cooldown = t.maxCooldown
	}
	t.skipUntil[k] = now.Add(cooldown)

	// Log once when entering a fresh cooldown window.
	return !alreadyCoolingDown
}

// arrFastRequest issues an HTTP request to an arr using fastClient.
// We do this in pkg/warden instead of going through arr.RequestCtx so the
// 5-retry shared client doesn't turn a DNS failure into a 30-second wait.
func arrFastRequest(ctx context.Context, a *arr.Arr, method, endpoint string, payload any) (*http.Response, error) {
	if a == nil || a.Host == "" || a.Token == "" {
		return nil, fmt.Errorf("arr not configured")
	}
	url, err := utils.JoinURL(a.Host, endpoint)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", a.Token)
	return fastClient.Do(req)
}

// removeQueueItem deletes one queue entry. Mirrors what Warden's
// execute_removal does (DELETE /queue/{id} with removeFromClient + optional
// blocklist). We do this one-at-a-time because the bulk endpoint requires
// all items to share the same arr.
func removeQueueItem(ctx context.Context, a *arr.Arr, queueID int, blocklist bool) error {
	q := gourl.Values{}
	q.Add("removeFromClient", "true")
	if blocklist {
		q.Add("blocklist", "true")
		q.Add("skipRedownload", "false")
	}
	url := fmt.Sprintf("%s/queue/%d?%s", a.APIBase(), queueID, q.Encode())
	resp, err := arrFastRequest(ctx, a, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s returned %s", url, resp.Status)
	}
	return nil
}

// refreshDownload asks the arr to re-scan the queue and re-import anything
// it now considers ready. This is the cheap "things just came back up, try
// again" trigger — it does NOT remove or re-search.
func refreshDownload(ctx context.Context, a *arr.Arr) error {
	payload := struct {
		Name string `json:"name"`
	}{Name: "RefreshMonitoredDownloads"}
	resp, err := arrFastRequest(ctx, a, http.MethodPost, a.APIBase()+"/command", payload)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("RefreshMonitoredDownloads returned %s", resp.Status)
	}
	return nil
}
