package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// recordNZBContentFailure records a permanent NZB failure under the stable
// content hash mapped from its (random) meta.ID, falling back to the id itself
// if the bridge entry is gone. The bridge entry is cleared once recorded.
func (m *Manager) recordNZBContentFailure(infohash, reason string) {
	if m.nzbFailCache == nil {
		return
	}
	key := infohash
	if m.nzbContentHash != nil {
		if ch, ok := m.nzbContentHash.Load(infohash); ok {
			key = ch
			m.nzbContentHash.Delete(infohash)
		}
	}
	m.nzbFailCache.Record(key, reason)
}

// AddNewNZB processes an NZB file and stores it as a storage.Entry
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}

	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB to usenet")

	// Fast-fail: if this exact NZB content recently failed with a
	// permanent error (dead articles, no valid files), refuse without
	// re-fetching from the usenet provider. Otherwise Lidarr's
	// redownload-on-failure can loop us forever on a single dead release.
	contentHash := hashNZBContent(req.NZBContent)
	if m.nzbFailCache != nil {
		if reason, hit := m.nzbFailCache.Lookup(contentHash); hit {
			m.logger.Info().
				Str("name", req.Name).
				Str("cached_reason", reason).
				Msg("NZB previously failed with permanent error — refusing without re-fetch")
			return "", fmt.Errorf("nzb previously failed: %s", reason)
		}
	}

	// Parse NZB through usenet client
	meta, groups, err := m.usenet.Parse(ctx, req.Name, req.NZBContent, req.Arr.Name)
	if err != nil {
		if m.nzbFailCache != nil && isPermanentNZBFailure(err) {
			m.nzbFailCache.Record(contentHash, err.Error())
		}
		return "", fmt.Errorf("usenet process failed: %w", err)
	}

	// Bridge this parse's random meta.ID to the stable content hash, so a
	// download-time failure (dead articles, only discovered when fetching) can
	// be recorded under the content hash that the lookup at the top of this
	// function matches on a re-grab. meta.ID is a fresh UUID per parse, so it
	// cannot be used as the dedup key directly.
	if m.nzbContentHash != nil {
		m.nzbContentHash.Store(meta.ID, contentHash)
	}

	// Create storage.Entry
	entry := &storage.Entry{
		InfoHash:         meta.ID,
		Name:             meta.Name,
		OriginalFilename: meta.Name,
		Size:             meta.TotalSize,
		Protocol:         config.ProtocolNZB,
		Bytes:            meta.TotalSize,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}

	entry.ContentPath = entry.DownloadPath()
	_ = entry.AddUsenetProvider(meta)
	entry.ActiveProvider = "usenet"
	entry.UpdatedAt = time.Now()
	entry.State = storage.EntryStateDownloading
	entry.Status = debridTypes.TorrentStatusDownloading
	if err := m.queue.Add(entry); err != nil {
		return "", fmt.Errorf("failed to add nzb to queue: %w", err)
	}

	// Submit job to unbounded worker pool queue (never blocks)
	m.nzbQueue.Push(&nzbJob{entry: entry, meta: meta, groups: groups})
	m.logger.Debug().Str("name", entry.Name).Int("queued", m.nzbQueue.Len()).Msg("NZB added to processing queue")

	return meta.ID, nil
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	// Add files using logical streamable files
	for _, file := range metadata.Files {
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		entry.Files[file.Name] = tFile
	}
	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)

	for _, file := range metadata.Files {
		go func(f storage.NZBFile) {
			cacheCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_ = m.usenet.PreCache(cacheCtx, metadata.ID, f.Name) // This will fetch head and tail of the file
		}(file)
	}

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	go m.processAction(entry)
	return nil
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(context.Background(), m.usenetTimeout)
	defer cancel()

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
		}
		if isPermanentNZBFailure(err) {
			// Record under the stable content hash (meta.ID is a random UUID per
			// parse and would never match a re-grab); AddNewNZB's content-hash
			// lookup then short-circuits the re-grab.
			m.recordNZBContentFailure(metadata.ID, err.Error())
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]interface{} {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	newNZBs, err := m.usenet.ProcessNewNZBs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get new NZBs from usenet client: %w", err)
	}

	for _, meta := range newNZBs {
		// Skip if already in storage or queue to avoid overwriting in-progress entries
		if _, err := m.GetEntry(meta.ID); err == nil {
			continue
		}
		if _, err := m.queue.GetTorrent(meta.ID); err == nil {
			continue
		}

		entry := &storage.Entry{
			InfoHash:         meta.ID,
			Name:             meta.Name,
			OriginalFilename: meta.Name,
			Size:             meta.TotalSize,
			Protocol:         config.ProtocolNZB,
			Bytes:            meta.TotalSize,
			Category:         meta.Category,
			Status:           debridTypes.TorrentStatusDownloading,
			State:            storage.EntryStateDownloading,
			Progress:         0,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
			AddedOn:          time.Now(),
			Providers:        make(map[string]*storage.ProviderEntry),
			Files:            make(map[string]*storage.File),
			Tags:             []string{},
		}
		entry.ContentPath = entry.DownloadPath()

		// AddOrUpdate placement
		_ = entry.AddUsenetProvider(meta)
		entry.ActiveProvider = "usenet"
		// AddOrUpdate files here using logical streamable files
		for _, file := range meta.Files {
			tFile := &storage.File{
				Name:     file.Name,
				Size:     file.Size,
				InfoHash: entry.InfoHash,
				AddedOn:  entry.AddedOn,
				Path:     file.Name,
			}
			entry.Files[file.Name] = tFile
		}

		// Add the entry to storage
		if err := m.storage.AddOrUpdate(entry); err != nil {
			m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to addOrUpdate synced NZB entry to storage")
			continue
		}
	}
	return nil
}
