package server

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// handleStream serves a streaming-friendly version of /api/browse/download:
//   - no auth required (so Jellyfin / ffmpeg / VLC can hit it from .strm files)
//   - no Content-Disposition: attachment (so the browser/player streams instead of downloading)
//   - 302 redirect to the live debrid CDN URL for torrents
//   - direct stream for usenet (NZB-style downloads have no CDN URL)
//
// URL shape: /stream/{torrent}/{file}
//
// Both segments must be URL-encoded by the caller. Decypharr's .strm
// generator does this automatically.
//
// SECURITY: this endpoint is intentionally unauthenticated. The torrent and
// file names are not secret (anyone with the names can guess the URL), but
// the debrid CDN URLs returned are short-lived signed URLs that the
// debrid provider issues per-request — so leaking the path doesn't expose
// the underlying content beyond Decypharr's existing exposure model.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	torrentName := utils.PathUnescape(chi.URLParam(r, "torrent"))
	fileName := utils.PathUnescape(chi.URLParam(r, "file"))

	entry, err := s.manager.GetEntryByName(torrentName, fileName)
	if err != nil || entry == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	file, err := entry.GetFile(fileName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", utils.GetContentType(file.Name))
	// Deliberately no Content-Disposition — we want inline streaming.

	switch entry.Protocol {
	case config.ProtocolTorrent:
		link, err := s.manager.GetDownloadLink(r.Context(), entry, file.Name)
		if err != nil || link.Empty() {
			s.logger.Warn().Err(err).Str("torrent", entry.Name).Str("file", file.Name).
				Msg("stream: failed to resolve download link")
			http.Error(w, "could not resolve stream URL", http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Accel-Redirect", link.DownloadLink)
		w.Header().Set("X-Accel-Buffering", "no")
		http.Redirect(w, r, link.DownloadLink, http.StatusFound)
	case config.ProtocolNZB:
		// Usenet streams don't have a CDN URL — we proxy the bytes ourselves.
		// This means usenet streams pay the Decypharr egress cost, but they
		// also bypass any external rate-limiting on the debrid side.
		w.Header().Set("Content-Length", strconv.FormatInt(file.Size, 10))
		if err := s.manager.Usenet().Download(r.Context(), file.InfoHash, file.Name, w, nil); err != nil {
			s.logger.Warn().Err(err).Str("file", file.Name).Msg("stream: usenet proxy failed")
		}
	default:
		http.Error(w, fmt.Sprintf("unsupported protocol: %s", entry.Protocol), http.StatusPreconditionFailed)
	}
}
