package warden

import (
	"context"
	"fmt"
	"net/http"
	gourl "net/url"

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
	resp, err := a.RequestCtx(ctx, http.MethodDelete, url, nil, nil)
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
// again" trigger — it does NOT remove or re-search. Equivalent to clicking
// "Refresh Monitored Downloads" in the UI.
func refreshDownload(ctx context.Context, a *arr.Arr) error {
	payload := struct {
		Name string `json:"name"`
	}{Name: "RefreshMonitoredDownloads"}
	resp, err := a.RequestCtx(ctx, http.MethodPost, a.APIBase()+"/command", payload, nil)
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
