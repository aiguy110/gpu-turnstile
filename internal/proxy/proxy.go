// Package proxy implements the OpenAI-compatible reverse proxy that fronts
// llama-swap. Each inbound /v1/* request acquires the GPU via the Scheduler,
// waits for the grant, then streams the request/response to/from the upstream
// llama-swap server, releasing the GPU exactly once when the exchange is done.
//
// It depends solely on internal/types (plus the standard library).
package proxy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// maxModelPeekBytes caps how much of the request body we buffer to sniff the
// top-level "model" field. Bodies larger than this are still proxied verbatim,
// but the model is left empty (we do not want to buffer unbounded uploads).
const maxModelPeekBytes = 4 << 20 // 4 MiB

// anonymousName is the display name used for unknown/missing API keys.
const anonymousName = "anon"

// Handler reverse-proxies OpenAI-compatible requests through the GPU scheduler.
type Handler struct {
	sched types.Scheduler
	ks    types.KeyStore
	cfg   types.LlamaSwapCfg
	log   *slog.Logger

	proxy *httputil.ReverseProxy
}

// New constructs a proxy Handler. The signature matches the frozen wiring
// contract in internal/types/doc.go exactly.
func New(sched types.Scheduler, ks types.KeyStore, cfg types.LlamaSwapCfg, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}

	h := &Handler{
		sched: sched,
		ks:    ks,
		cfg:   cfg,
		log:   log,
	}

	// Build the reverse proxy targeting the upstream base URL. httputil's
	// ReverseProxy already streams and flushes response bodies (including SSE
	// token streams), so we do not buffer.
	target, err := url.Parse(cfg.BaseURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		// A malformed BaseURL is a configuration error we cannot fail-fast on
		// (no os.Exit). Log it; the ErrorHandler below will turn every request
		// into a 502 because the director produces an unusable URL.
		log.Error("proxy: invalid llama-swap BaseURL", "baseURL", cfg.BaseURL, "err", err)
	}

	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Fires on upstream dial/stream errors and on client disconnects. We do
		// not write a body if the client is gone; otherwise emit a 502.
		h.log.Warn("proxy: upstream error", "path", r.URL.Path, "err", err)
		if r.Context().Err() == nil {
			w.WriteHeader(http.StatusBadGateway)
		}
	}
	h.proxy = rp

	return h
}

// Routes returns an http.Handler that mounts this handler under "/v1/".
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", h)
	return mux
}

// ServeHTTP handles a single OpenAI-compatible request: authenticate, sniff the
// model, acquire the GPU, wait for the grant (or client cancel), then reverse
// proxy and release exactly once.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Auth / priority.
	token := bearerToken(r)
	name := anonymousName
	prio := h.ks.AnonymousPriority()
	if info, found := h.ks.Lookup(token); found {
		name = info.Name
		prio = info.Priority
	}

	// 2. Model peek — buffer the body, decode just the "model" field, then
	// restore r.Body so the upstream receives the identical bytes.
	model := h.peekModelAndRestore(r)

	// 3. Acquire a GPU ticket and wait for the grant.
	ticket := h.sched.Acquire(types.AcquireSpec{
		ID:       newRequestID(),
		Name:     name,
		Priority: prio,
		Kind:     types.KindLLM,
		Model:    model,
	})

	// Guarantee release exactly once, no matter which path we exit through.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(ticket.Release) }

	start := time.Now()
	select {
	case <-ticket.Granted():
		// Proceed below.
	case <-r.Context().Done():
		// Client gave up before we ever got the GPU. Release the queued ticket
		// and abandon; the upstream was never contacted.
		release()
		h.log.Info("proxy: client canceled before grant", "name", name, "model", string(model))
		w.WriteHeader(http.StatusRequestTimeout)
		return
	}

	waited := time.Since(start)
	w.Header().Set("X-Turnstile-Wait", strconv.FormatInt(waited.Milliseconds(), 10))

	// 4/5. Reverse-proxy the request. httputil.ReverseProxy streams the
	// response and only returns after the body is fully written (or errored, or
	// the client disconnected mid-stream). Releasing after it returns therefore
	// never releases early while llama-swap is still streaming.
	defer release()
	h.proxy.ServeHTTP(w, r)
}

// peekModelAndRestore reads up to maxModelPeekBytes of the request body to find
// the top-level "model" string, then restores r.Body (and Content-Length) so
// the upstream receives the identical, complete body. Bodies larger than the
// cap, or non-JSON bodies, yield types.Model("") but are still proxied intact.
func (h *Handler) peekModelAndRestore(r *http.Request) types.Model {
	if r.Body == nil {
		return types.Model("")
	}

	limited := io.LimitReader(r.Body, maxModelPeekBytes+1)
	buf, err := io.ReadAll(limited)
	if err != nil {
		// Best-effort: restore whatever we managed to read so the proxy still
		// forwards it, and skip model parsing.
		h.restoreBody(r, buf)
		return types.Model("")
	}

	if len(buf) > maxModelPeekBytes {
		// Oversized body: we truncated the peek. Stitch the buffered prefix back
		// in front of the unread remainder so the upstream gets every byte, and
		// do NOT attempt to parse a partial JSON document.
		combined := io.MultiReader(bytes.NewReader(buf), r.Body)
		r.Body = io.NopCloser(combined)
		r.ContentLength = -1
		r.Header.Del("Content-Length")
		return types.Model("")
	}

	// We hold the whole body; close the original and restore an exact copy.
	_ = r.Body.Close()
	h.restoreBody(r, buf)

	return parseModel(buf)
}

// restoreBody sets r.Body to a fresh reader over buf and syncs the length
// headers so the upstream sees the identical body.
func (h *Handler) restoreBody(r *http.Request, buf []byte) {
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	r.Header.Set("Content-Length", strconv.Itoa(len(buf)))
}

// parseModel decodes only the top-level "model" field from a JSON object.
func parseModel(buf []byte) types.Model {
	if len(bytes.TrimSpace(buf)) == 0 {
		return types.Model("")
	}
	var probe struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(buf, &probe); err != nil {
		return types.Model("")
	}
	return types.Model(probe.Model)
}

// bearerToken extracts the API key from "Authorization: Bearer X"
// (case-insensitive scheme), falling back to the "X-Api-Key" header.
func bearerToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		const scheme = "bearer "
		if len(auth) >= len(scheme) && strings.EqualFold(auth[:len(scheme)], scheme) {
			return strings.TrimSpace(auth[len(scheme):])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

// newRequestID returns a random hex id for an AcquireSpec.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is effectively fatal for uniqueness; fall back to
		// a timestamp so we never panic in a request path.
		return "req-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "req-" + hex.EncodeToString(b[:])
}
