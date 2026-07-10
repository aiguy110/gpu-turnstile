// Package lease implements the cooperative GPU lease HTTP API.
//
// It resolves the caller's priority tier via the KeyStore, acquires a Ticket
// from the Scheduler, and lets clients poll/heartbeat/release a lease. It owns
// no scheduling policy: reaping of dead (non-heartbeating) leases is the
// scheduler's job. See internal/types/doc.go for the frozen wiring contract.
package lease

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// Handler serves the lease API. Construct it with New.
type Handler struct {
	sched types.Scheduler
	ks    types.KeyStore
	cfg   types.LeaseCfg
	log   *slog.Logger

	mu      sync.Mutex
	tickets map[string]types.Ticket

	mux *http.ServeMux
}

// New builds a lease Handler. Signature is fixed by internal/types/doc.go.
func New(sched types.Scheduler, ks types.KeyStore, cfg types.LeaseCfg, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	h := &Handler{
		sched:   sched,
		ks:      ks,
		cfg:     cfg,
		log:     log,
		tickets: make(map[string]types.Ticket),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /lease", h.handleCreate)
	mux.HandleFunc("GET /lease/{id}", h.handleGet)
	mux.HandleFunc("POST /lease/{id}/heartbeat", h.handleHeartbeat)
	mux.HandleFunc("POST /lease/{id}/release", h.handleRelease)
	h.mux = mux
	return h
}

// ServeHTTP delegates to the internal, path-routed mux.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// Routes returns a handler pre-routed for this package's paths (/lease, /lease/...).
func (h *Handler) Routes() http.Handler {
	return h.mux
}

// createRequest is the POST /lease body. All fields are optional.
type createRequest struct {
	APIKey     string `json:"api_key"`
	EstSeconds int    `json:"est_seconds"`
}

func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if r.Body != nil {
		// Ignore decode errors: an empty/garbage body is treated as anonymous.
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	token := req.APIKey
	if token == "" {
		token = tokenFromHeaders(r)
	}

	name := "anon"
	priority := h.ks.AnonymousPriority()
	if token != "" {
		if info, found := h.ks.Lookup(token); found {
			name = info.Name
			priority = info.Priority
		}
	}

	leaseID, err := newLeaseID()
	if err != nil {
		h.log.Error("lease: failed to generate lease id", "err", err)
		writeError(w, http.StatusInternalServerError, "could not generate lease id")
		return
	}

	ticket := h.sched.Acquire(types.AcquireSpec{
		ID:         leaseID,
		Name:       name,
		Priority:   priority,
		Kind:       types.KindLease,
		EstSeconds: req.EstSeconds,
	})

	h.mu.Lock()
	h.tickets[leaseID] = ticket
	h.mu.Unlock()

	if granted(ticket) {
		writeJSON(w, http.StatusOK, map[string]any{
			"lease_id": leaseID,
			"status":   "granted",
		})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"lease_id":      leaseID,
		"status":        "queued",
		"poll_after_ms": h.cfg.PollAfterMs,
	})
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ticket, ok := h.lookup(id)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown lease")
		return
	}

	// A poll counts as liveness.
	revoke := ticket.Heartbeat()
	st := ticket.Status()

	status := "queued"
	if granted(ticket) {
		status = "granted"
	}

	resp := map[string]any{
		"status":           status,
		"position":         st.Position,
		"revoke_requested": st.RevokeRequested || revoke,
		"ahead":            st.Position,
	}
	if status == "queued" {
		resp["poll_after_ms"] = h.cfg.PollAfterMs
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ticket, ok := h.lookup(id)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown lease")
		return
	}
	revoke := ticket.Heartbeat()
	writeJSON(w, http.StatusOK, map[string]any{
		"revoke_requested": revoke,
	})
}

func (h *Handler) handleRelease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.mu.Lock()
	ticket, ok := h.tickets[id]
	if ok {
		delete(h.tickets, id)
	}
	h.mu.Unlock()

	if ok {
		ticket.Release()
	}
	// Release is idempotent: always 200, even for an unknown id.
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "released",
	})
}

func (h *Handler) lookup(id string) (types.Ticket, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.tickets[id]
	return t, ok
}

// granted reports whether the ticket's Granted channel is already closed,
// without blocking.
func granted(t types.Ticket) bool {
	select {
	case <-t.Granted():
		return true
	default:
		return false
	}
}

// tokenFromHeaders extracts a bearer token from Authorization or X-Api-Key.
func tokenFromHeaders(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if len(h) > len(prefix) && (h[:len(prefix)] == prefix || h[:len(prefix)] == "bearer ") {
			return h[len(prefix):]
		}
		return h
	}
	return r.Header.Get("X-Api-Key")
}

func newLeaseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
