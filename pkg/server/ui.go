package server

import (
	"errors"
	"net/http"
	"strings"
	"syscall"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// isClientDisconnect returns true when an error came from the client closing
// the connection (broken pipe, connection reset). Healthcheck tools like
// `wget --spider` close after reading headers, so every index render emits
// a write error — this lets us log those at debug instead of warn.
func isClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset")
}

func (s *Server) LoginHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	if cfg.NeedsAuth() {
		http.Redirect(w, r, "/register", http.StatusSeeOther)
		return
	}
	if r.Method == "GET" {
		data := map[string]interface{}{
			"URLBase": cfg.URLBase,
			"Page":    "login",
			"Title":   "Login",
		}
		err := s.templates.ExecuteTemplate(w, "layout", data)
		if err != nil {
			if isClientDisconnect(err) {
				s.logger.Debug().Err(err).Msg("error rendering /login template (client disconnected)")
			} else {
				s.logger.Warn().Err(err).Msg("error rendering /login template")
			}
		}
		return
	}

	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&credentials); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if s.verifyAuth(credentials.Username, credentials.Password) {
		session, _ := s.cookie.Get(r, "auth-session")
		session.Values["authenticated"] = true
		session.Values["username"] = credentials.Username
		if err := session.Save(r, w); err != nil {
			http.Error(w, "Error saving session", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	http.Error(w, "Invalid credentials", http.StatusUnauthorized)
}

func (s *Server) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := s.cookie.Get(r, "auth-session")
	session.Values["authenticated"] = false
	session.Options.MaxAge = -1
	err := session.Save(r, w)
	if err != nil {
		return
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	authCfg := cfg.GetAuth()

	if r.Method == "GET" {
		data := map[string]interface{}{
			"URLBase": cfg.URLBase,
			"Page":    "register",
			"Title":   "registerVolume",
		}
		err := s.templates.ExecuteTemplate(w, "layout", data)
		if err != nil {
			if isClientDisconnect(err) {
				s.logger.Debug().Err(err).Msg("error rendering /register template (client disconnected)")
			} else {
				s.logger.Warn().Err(err).Msg("error rendering /register template")
			}
		}
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")
	confirmPassword := r.FormValue("confirmPassword")

	if password != confirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Error processing password", http.StatusInternalServerError)
		return
	}

	// Set the credentials
	authCfg.Username = username
	authCfg.Password = string(hashedPassword)

	if err := cfg.SaveAuth(authCfg); err != nil {
		http.Error(w, "Error saving credentials", http.StatusInternalServerError)
		return
	}

	// Create a session
	session, _ := s.cookie.Get(r, "auth-session")
	session.Values["authenticated"] = true
	session.Values["username"] = username
	if err := session.Save(r, w); err != nil {
		http.Error(w, "Error saving session", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) IndexHandler(w http.ResponseWriter, r *http.Request) {
	// Healthcheck short-circuit: the bundled Docker healthcheck hits / with
	// `wget --spider`, which discards the body after headers. Rendering the
	// full HTML for every probe is wasted work and produces broken-pipe
	// warnings as wget closes the socket mid-write. Detect the probe via UA
	// and return a fast cheap 200 instead. Browsers (User-Agent: Mozilla/...)
	// still get the real UI.
	if isHealthcheckProbe(r) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
		return
	}
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase":    cfg.URLBase,
		"Page":       "index",
		"Title":      "Queues",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		if isClientDisconnect(err) {
			s.logger.Debug().Err(err).Msg("error rendering /index template (client disconnected)")
		} else {
			s.logger.Warn().Err(err).Msg("error rendering /index template")
		}
	}
}

// isHealthcheckProbe heuristically detects requests from container
// healthcheck tooling so we can serve them a tiny 200 OK without rendering.
// Matches: wget, curl, and any UA that mentions "health". Skipped (returns
// false) when the Accept header explicitly asks for text/html — somebody
// who really wants the UI from those tools can still get it with `wget
// --header="Accept: text/html"`.
func isHealthcheckProbe(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	accept := strings.ToLower(r.Header.Get("Accept"))
	if strings.Contains(accept, "text/html") {
		return false
	}
	ua := strings.ToLower(r.Header.Get("User-Agent"))
	if ua == "" {
		return false
	}
	return strings.HasPrefix(ua, "wget/") ||
		strings.HasPrefix(ua, "curl/") ||
		strings.Contains(ua, "healthcheck") ||
		strings.Contains(ua, "docker-healthcheck") ||
		strings.Contains(ua, "kube-probe")
}

func (s *Server) DownloadHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	debrids := make([]string, 0)
	for _, d := range cfg.Debrids {
		debrids = append(debrids, d.Name)
	}
	data := map[string]interface{}{
		"URLBase":                 cfg.URLBase,
		"Page":                    "download",
		"Title":                   "Download",
		"Debrids":                 debrids,
		"HasMultiDebrid":          len(debrids) > 1,
		"downloadFolder":          cfg.DownloadFolder,
		"alwaysRemoveTrackerURLS": cfg.AlwaysRmTrackerUrls,
		"SetupError":              cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		if isClientDisconnect(err) {
		s.logger.Debug().Err(err).Msg("error rendering /download template (client disconnected)")
		} else {
		s.logger.Warn().Err(err).Msg("error rendering /download template")
		}
	}
}

func (s *Server) RepairHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase":    cfg.URLBase,
		"Page":       "repair",
		"Title":      "Repair",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		if isClientDisconnect(err) {
		s.logger.Debug().Err(err).Msg("error rendering /repair template (client disconnected)")
		} else {
		s.logger.Warn().Err(err).Msg("error rendering /repair template")
		}
	}
}

func (s *Server) ConfigHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase":    cfg.URLBase,
		"Page":       "config",
		"Title":      "Config",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /config template")
	}
}

func (s *Server) StatsHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase": cfg.URLBase,
		"Page":    "stats",
		"Title":   "Statistics",
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /stats template")
	}
}

func (s *Server) BrowseHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase":    cfg.URLBase,
		"Page":       "browse",
		"Title":      "Browse Torrents",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /browse template")
	}
}
