package server

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// mediaExtensions are the file extensions for which a .strm file makes sense.
// Anything not in this set is skipped — we don't generate .strm for .rar/.par2/.nfo
// because those aren't playable streams.
var mediaExtensions = map[string]struct{}{
	".mkv":  {},
	".mp4":  {},
	".avi":  {},
	".mov":  {},
	".webm": {},
	".m4v":  {},
	".ts":   {},
	".mpg":  {},
	".mpeg": {},
	".wmv":  {},
	".flv":  {},
	".m2ts": {},
	".mp3":  {},
	".flac": {},
	".m4a":  {},
	".ogg":  {},
	".opus": {},
	".wav":  {},
	".aac":  {},
	".alac": {},
	".wma":  {},
}

type strmGenerateRequest struct {
	// Root directory to walk. Must be readable and writable by Decypharr.
	Root string `json:"root"`
	// If true, walk subdirectories. Default false.
	Recursive bool `json:"recursive"`
	// If true, generate .strm files for plain files too, not just symlinks.
	// Default false — only symlinks (broken or not) get .strm companions.
	IncludePlainFiles bool `json:"include_plain_files"`
	// If true, write the .strm file even when one already exists. Default false.
	Overwrite bool `json:"overwrite"`
	// If true, delete the original symlink/file after writing the .strm.
	// Use this when you want Jellyfin to ignore the original (which may be a
	// broken symlink) and only see the .strm. Default false.
	DeleteOriginal bool `json:"delete_original"`
	// If true, only report what would be done — write nothing. Default false.
	DryRun bool `json:"dry_run"`
	// External base URL Jellyfin will use to reach this Decypharr instance.
	// Example: "http://decypharr:8282". The .strm contents become
	// "{base}/stream/{torrent}/{file}". If empty, falls back to config.AppURL.
	BaseURL string `json:"base_url"`
}

type strmGenerateResponse struct {
	Scanned int      `json:"scanned"`
	Wrote   int      `json:"wrote"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
	Files   []string `json:"files,omitempty"`
}

// handleSTRMGenerate walks a directory and creates a .strm file for every
// media-extension entry pointing back at Decypharr's /stream endpoint. The
// caller chooses whether the original symlink stays or gets removed.
//
// Intended workflow:
//  1. Arr import places a file (real or symlinked) into the library directory.
//  2. Post-import webhook (or scheduled job) calls this endpoint with that
//     directory as `root`.
//  3. Decypharr writes a .strm next to each media file.
//  4. With `delete_original=true`, the originals are removed and Jellyfin
//     scans only the .strm files — surviving any future DFS downtime.
//
// SECURITY: this endpoint requires auth (sits behind authMiddleware in
// routes.go). Treat it as filesystem-mutating: callers can ask Decypharr
// to delete files anywhere it can write.
func (s *Server) handleSTRMGenerate(w http.ResponseWriter, r *http.Request) {
	var req strmGenerateRequest
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendJSONError(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Root == "" {
		s.sendJSONError(w, "root is required", http.StatusBadRequest)
		return
	}
	root, err := filepath.Abs(req.Root)
	if err != nil {
		s.sendJSONError(w, "invalid root path: "+err.Error(), http.StatusBadRequest)
		return
	}
	info, err := os.Stat(root)
	if err != nil {
		s.sendJSONError(w, "root not accessible: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !info.IsDir() {
		s.sendJSONError(w, "root must be a directory", http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	base := strings.TrimRight(req.BaseURL, "/")
	if base == "" {
		base = strings.TrimRight(cfg.AppURL, "/")
	}
	if base == "" {
		s.sendJSONError(w, "base_url is required (or set config.app_url)", http.StatusBadRequest)
		return
	}

	resp := &strmGenerateResponse{}
	walkFn := func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %v", path, walkErr))
			return nil
		}
		if d.IsDir() {
			if !req.Recursive && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		resp.Scanned++

		// Only act on media extensions
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if _, ok := mediaExtensions[ext]; !ok {
			resp.Skipped++
			return nil
		}

		// Skip the .strm files themselves
		if ext == ".strm" {
			resp.Skipped++
			return nil
		}

		// Decide whether to process — symlink always, plain file only if opted in
		fi, err := os.Lstat(path)
		if err != nil {
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: lstat: %v", path, err))
			return nil
		}
		isSymlink := fi.Mode()&os.ModeSymlink != 0
		if !isSymlink && !req.IncludePlainFiles {
			resp.Skipped++
			return nil
		}

		// Derive {torrent}/{file} for the URL. The convention used by
		// Decypharr's existing download endpoint is:
		//   /api/browse/download/{torrent-name}/{file-name}
		// For .strm we mirror that with the public /stream path.
		// Torrent name = parent directory name.
		// File name = basename of this file.
		torrentName := filepath.Base(filepath.Dir(path))
		fileName := d.Name()

		streamURL := fmt.Sprintf("%s/stream/%s/%s",
			base,
			url.PathEscape(torrentName),
			url.PathEscape(fileName),
		)

		strmPath := strings.TrimSuffix(path, ext) + ".strm"

		// Skip if already exists and overwrite=false
		if !req.Overwrite {
			if _, err := os.Stat(strmPath); err == nil {
				resp.Skipped++
				return nil
			}
		}

		if req.DryRun {
			resp.Wrote++
			resp.Files = append(resp.Files, fmt.Sprintf("DRY: write %s -> %s", strmPath, streamURL))
			return nil
		}

		if err := os.WriteFile(strmPath, []byte(streamURL+"\n"), 0644); err != nil {
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: write: %v", strmPath, err))
			return nil
		}
		resp.Wrote++
		resp.Files = append(resp.Files, strmPath)

		if req.DeleteOriginal {
			if err := os.Remove(path); err != nil {
				resp.Errors = append(resp.Errors, fmt.Sprintf("%s: remove original: %v", path, err))
			}
		}
		return nil
	}

	if err := filepath.WalkDir(root, walkFn); err != nil {
		resp.Errors = append(resp.Errors, "walk: "+err.Error())
	}

	// Cap Files list to keep response sane on huge libraries
	const filesCap = 500
	if len(resp.Files) > filesCap {
		resp.Files = append(resp.Files[:filesCap], fmt.Sprintf("... (%d more)", len(resp.Files)-filesCap))
	}

	utils.JSONResponse(w, resp, http.StatusOK)
}
