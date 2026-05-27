package warden

import (
	"strings"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

// StallCategory classifies *arr queue items in trackedDownloadStatus=warning
// state. Mirrors Warden's category map so users can carry over their
// existing intuition about which messages mean what.
type StallCategory string

const (
	StallDangerousFile StallCategory = "dangerous_file"
	StallManualImport  StallCategory = "manual_import"
	StallNoFiles       StallCategory = "no_files"
	StallNoUpgrade     StallCategory = "no_upgrade"
	StallStalled       StallCategory = "stalled"
	StallMissingItems  StallCategory = "missing_items"
	StallTBATitle      StallCategory = "tba_title"
	StallNoMessages    StallCategory = "no_messages"
	StallPathMissing   StallCategory = "path_missing"
	StallUnknown       StallCategory = "unknown"
)

// stallPatterns maps each category to lowercase substring patterns. Order matters
// only weakly — path_missing is checked before manual_import because every
// path-not-exist message also matches the broader manual_import set.
var stallPatterns = []struct {
	category StallCategory
	patterns []string
}{
	{StallPathMissing, []string{
		"import failed, path does not exist",
		"path does not exist",
	}},
	{StallDangerousFile, []string{
		"potentially dangerous file extension",
	}},
	{StallNoFiles, []string{
		"no audio files found",
		"no files found are eligible for import",
		"no video files found",
		"downloaded file is empty",
	}},
	{StallNoUpgrade, []string{
		"already meets cutoff",
		"custom format upgrade",
		"do not improve on existing",
		"not a custom format upgrade",
		"not an upgrade for existing",
	}},
	{StallStalled, []string{
		"is locked by another process",
		"qbittorrent is downloading metadata",
		"the download is stalled with no",
	}},
	{StallMissingItems, []string{
		"not imported or missing from the release",
		"not found in the grabbed release",
	}},
	{StallTBATitle, []string{"tba title"}},
	{StallManualImport, []string{
		"non-sample file detected",
		"not enough space",
		"sample file detected",
		"unable to parse file",
		"found matching movie via grab history",
		"release was matched to movie by id",
		"matched to movie by id",
		"unable to determine if file is a sample",
		"automatic import is not possible",
		"release title doesn't match series title",
		"release was matched to series by id",
		"matched to series by id",
		"single episode file contains all episodes",
		"single episode file contains",
		"matched to album by id",
		"track does not belong to album",
		"manual import required",
		"album match is not close enough",
		"couldn't find similar album",
		"failed to import track",
		"has missing tracks",
		"permissions error",
		"worst track match",
		"found matching series via grab history, but release was matched to series by id",
	}},
}

// ClassifyStall examines an arr queue item's status messages and returns
// the best-matching category. Returns StallNoMessages when there are no
// messages, StallUnknown when there are messages but none match.
func ClassifyStall(item arr.QueueSchema) StallCategory {
	if len(item.StatusMessages) == 0 {
		return StallNoMessages
	}
	var combined strings.Builder
	for _, m := range item.StatusMessages {
		combined.WriteString(strings.ToLower(m.Title))
		combined.WriteString(" ")
		for _, line := range m.Messages {
			combined.WriteString(strings.ToLower(line))
			combined.WriteString(" ")
		}
	}
	haystack := combined.String()
	for _, entry := range stallPatterns {
		for _, p := range entry.patterns {
			if strings.Contains(haystack, p) {
				return entry.category
			}
		}
	}
	return StallUnknown
}

// IsStalled returns true when the item is in a tracked-warning state worth
// investigating. Matches Warden's _is_stalled() check.
func IsStalled(item arr.QueueSchema) bool {
	return item.TrackedDownloadStatus == "warning"
}
