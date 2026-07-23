// Package web serves the turnstile ops console and its JSON/SSE API.
//
// It depends only on internal/types (plus the standard library). The console
// HTML/CSS/JS is embedded via go:embed so the binary is self-contained.
package web

import (
	"embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

//go:embed static
var staticFS embed.FS

// Handler serves the console UI and the /api/* endpoints.
type Handler struct {
	sched    types.Scheduler
	ks       types.KeyStore
	activity types.ActivityStore
	log      *slog.Logger
	mux      *http.ServeMux
}

// New constructs a web Handler. Signature matches the wiring contract in
// internal/types/doc.go exactly.
func New(sched types.Scheduler, ks types.KeyStore, log *slog.Logger) *Handler {
	return NewObserved(sched, ks, nil, log)
}

// NewObserved constructs a console with LLM activity history and streaming.
func NewObserved(sched types.Scheduler, ks types.KeyStore, activity types.ActivityStore, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	h := &Handler{sched: sched, ks: ks, activity: activity, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.handleIndex)
	mux.HandleFunc("GET /api/queue", h.handleQueue)
	mux.HandleFunc("GET /api/queue/stream", h.handleQueueStream)
	mux.HandleFunc("GET /api/history", h.handleHistory)
	mux.HandleFunc("GET /api/activity", h.handleActivityList)
	mux.HandleFunc("GET /api/activity/stream", h.handleActivityStream)
	mux.HandleFunc("GET /api/activity/{id}", h.handleActivityGet)
	mux.HandleFunc("GET /api/keys", h.handleListKeys)
	mux.HandleFunc("POST /api/keys", h.handleMintKey)
	mux.HandleFunc("DELETE /api/keys/{nameOrKey}", h.handleRevokeKey)
	h.mux = mux
	return h
}

// handleHistory returns GPU holds for the Overview timeline.
func (h *Handler) handleHistory(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-24 * time.Hour)
	query := r.URL.Query()
	if raw := query.Get("since"); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			since = parsed
		}
	} else if duration, ok := parseWindow(query.Get("window")); ok {
		since = time.Now().Add(-duration)
	}
	records := h.sched.History(since)
	if records == nil {
		records = []types.HoldRecord{}
	}
	h.writeJSON(w, http.StatusOK, records)
}

func parseWindow(window string) (time.Duration, bool) {
	switch window {
	case "1h":
		return time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	case "7d":
		return 7 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func (h *Handler) handleActivityList(w http.ResponseWriter, r *http.Request) {
	if h.activity == nil {
		h.writeJSON(w, http.StatusOK, []types.LLMActivity{})
		return
	}
	items := h.activity.List()
	// The rail needs titles and metrics, not potentially megabytes of transcript
	// data. Full content is fetched only for the selected request.
	for i := range items {
		items[i].Output = ""
		items[i].ToolCalls = nil
		items[i].Usage = nil
	}
	h.writeJSON(w, http.StatusOK, items)
}

func (h *Handler) handleActivityGet(w http.ResponseWriter, r *http.Request) {
	if h.activity == nil {
		h.writeError(w, http.StatusNotFound, "activity not found")
		return
	}
	a, ok := h.activity.Get(r.PathValue("id"))
	if !ok {
		h.writeError(w, http.StatusNotFound, "activity not found")
		return
	}
	h.writeJSON(w, http.StatusOK, a)
}

func (h *Handler) handleActivityStream(w http.ResponseWriter, r *http.Request) {
	if h.activity == nil {
		http.Error(w, "activity unavailable", http.StatusNotFound)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	updates, cancel := h.activity.Subscribe()
	defer cancel()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case a, open := <-updates:
			if !open {
				return
			}
			payload, err := json.Marshal(a)
			if err != nil {
				continue
			}
			if _, err := w.Write(append(append([]byte("data: "), payload...), '\n', '\n')); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// Routes returns a handler pre-routed for this package's paths.
func (h *Handler) Routes() http.Handler { return h.mux }

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Only serve the console at the exact root; other unmatched GETs are 404.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	page, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		h.log.Error("read embedded index", "err", err)
		http.Error(w, "console unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page)
}

func (h *Handler) handleQueue(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, h.sched.Snapshot())
}

func (h *Handler) handleQueueStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	send := func() bool {
		payload, err := json.Marshal(h.sched.Snapshot())
		if err != nil {
			h.log.Error("marshal snapshot for stream", "err", err)
			return false
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return false
		}
		if _, err := w.Write(payload); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	// One immediately on connect.
	if !send() {
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !send() {
				return
			}
		}
	}
}

func (h *Handler) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys := h.ks.List()
	if keys == nil {
		keys = []types.KeyInfo{}
	}
	h.writeJSON(w, http.StatusOK, keys)
}

func (h *Handler) handleMintKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string         `json:"name"`
		Priority types.Priority `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	info, err := h.ks.Mint(req.Name, req.Priority)
	if err != nil {
		h.log.Error("mint key", "name", req.Name, "err", err)
		h.writeError(w, http.StatusInternalServerError, "could not mint key")
		return
	}
	// 201 with the secret included (shown once).
	h.writeJSON(w, http.StatusCreated, info)
}

func (h *Handler) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	nameOrKey := r.PathValue("nameOrKey")
	if strings.TrimSpace(nameOrKey) == "" {
		h.writeError(w, http.StatusBadRequest, "name or key is required")
		return
	}
	if err := h.ks.Revoke(nameOrKey); err != nil {
		// Any Revoke error means nothing matched -> 404.
		h.writeError(w, http.StatusNotFound, "no key matching name or secret")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.log.Error("encode json response", "err", err)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}
