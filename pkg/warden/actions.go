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
		MaxIdleConns:        4,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
		MaxConnsPerHost:     2,
	},
}

// arrUnavailable tracks how many consecutive failures Warden has seen per arr
// host. After N failures we stop trying that arr until the next process
// restart, so a permanently-down arr doesn't waste 3 seconds × every cycle.
// Cleared on first success.
var (
	arrFailMu     sync.Mutex
	arrFails      = map[string]int{}
	arrUnreached  = map[string]bool{}
	maxArrFails   = 3 // 3 strikes and we skip that host
)

func arrKey(a *arr.Arr) string {
	if a == nil {
		return ""
	}
	return a.Host
}

func isArrAvailable(a *arr.Arr) bool {
	arrFailMu.Lock()
	defer arrFailMu.Unlock()
	return !arrUnreached[arrKey(a)]
}

func recordArrSuccess(a *arr.Arr) {
	arrFailMu.Lock()
	defer arrFailMu.Unlock()
	k := arrKey(a)
	delete(arrFails, k)
	delete(arrUnreached, k)
}

// recordArrFailure increments the per-arr failure counter and returns true
// if this is the failure that crossed the threshold (so the caller can log
// a one-time "giving up on this arr" notice).
func recordArrFailure(a *arr.Arr) bool {
	arrFailMu.Lock()
	defer arrFailMu.Unlock()
	k := arrKey(a)
	if k == "" || arrUnreached[k] {
		return false
	}
	arrFails[k]++
	if arrFails[k] >= maxArrFails {
		arrUnreached[k] = true
		return true
	}
	return false
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
