// Package proxy implements the OpenAI-compatible reverse proxy that fronts
// llama-swap. Each inbound /v1/* request acquires the GPU via the Scheduler,
// waits for the grant, then streams the request/response to/from the upstream
// llama-swap server, releasing the GPU exactly once when the exchange is done.
//
// It depends solely on internal/types (plus the standard library).
package proxy

import (
	"bytes"
	"context"
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
	sched    types.Scheduler
	ks       types.KeyStore
	cfg      types.LlamaSwapCfg
	log      *slog.Logger
	activity types.ActivityStore
	routing  types.RoutingCfg

	proxy *httputil.ReverseProxy
}

// New constructs a proxy Handler. The signature matches the frozen wiring
// contract in internal/types/doc.go exactly.
func New(sched types.Scheduler, ks types.KeyStore, cfg types.LlamaSwapCfg, log *slog.Logger) *Handler {
	return NewObserved(sched, ks, cfg, types.RoutingCfg{}, nil, log)
}

// NewObserved constructs a proxy that also records normalized LLM activity and
// applies virtual-model routing and context estimates from routing.
func NewObserved(sched types.Scheduler, ks types.KeyStore, cfg types.LlamaSwapCfg, routing types.RoutingCfg, activity types.ActivityStore, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}

	h := &Handler{
		sched:    sched,
		ks:       ks,
		cfg:      cfg,
		log:      log,
		activity: activity,
		routing:  routing,
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
		if c := captureFromContext(r.Context()); c != nil {
			c.fail(err.Error(), r.Context().Err() != nil)
		}
		if r.Context().Err() == nil {
			w.WriteHeader(http.StatusBadGateway)
		}
	}
	rp.ModifyResponse = func(resp *http.Response) error {
		c := captureFromContext(resp.Request.Context())
		if c != nil {
			c.status = resp.StatusCode
			c.noBody = resp.Request.Method == http.MethodHead || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified
			c.parser = newResponseParser(resp.Header.Get("Content-Type"), c.emit)
			resp.Body = &observedBody{ReadCloser: resp.Body, capture: c}
		}
		return nil
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

	// 2. Safely inspect display metadata, then restore the exact request body
	// (rewritten when it targets a virtual model).
	model, prompt, inputTools, rt := h.peekRequestAndRestore(r)
	if rt.reject != "" {
		h.log.Info("proxy: rejected", "name", name, "model", string(rt.alias), "reason", rt.reject)
		writeContextError(w, rt.reject)
		return
	}
	requestID := newRequestID()
	var capture *responseCapture
	if h.activity != nil {
		h.activity.Begin(types.LLMActivity{
			ID: requestID, Name: name, Priority: prio, Model: model, Alias: rt.alias,
			Endpoint: r.URL.Path, Prompt: prompt, ToolCalls: inputTools,
			EnqueuedAt: time.Now(),
		})
		capture = &responseCapture{id: requestID, store: h.activity, ctx: r.Context()}
		r = r.WithContext(context.WithValue(r.Context(), captureContextKey{}, capture))
	}

	// 3. Acquire a GPU ticket and wait for the grant.
	ticket := h.sched.Acquire(types.AcquireSpec{
		ID:        requestID,
		Name:      name,
		Priority:  prio,
		Kind:      types.KindLLM,
		Model:     model,
		Alias:     rt.alias,
		CtxTokens: rt.ctxTokens,
	})

	// Guarantee release exactly once, no matter which path we exit through.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(ticket.Release) }

	start := time.Now()
	select {
	case <-ticket.Granted():
		if h.activity != nil {
			h.activity.Granted(requestID, time.Now())
		}
	case <-r.Context().Done():
		// Client gave up before we ever got the GPU. Release the queued ticket
		// and abandon; the upstream was never contacted.
		release()
		h.log.Info("proxy: client canceled before grant", "name", name, "model", string(model))
		if capture != nil {
			capture.fail("client canceled before GPU grant", true)
		}
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

// peekRequestAndRestore reads bounded, safe display metadata and restores the
// body for the upstream: byte-identical, unless the request names a virtual
// model, in which case the rewritten body is forwarded. Headers and API keys
// are never observed.
func (h *Handler) peekRequestAndRestore(r *http.Request) (types.Model, string, []types.ToolCall, routeResult) {
	if r.Body == nil {
		return "", "", nil, routeResult{}
	}

	limited := io.LimitReader(r.Body, maxModelPeekBytes+1)
	buf, err := io.ReadAll(limited)
	if err != nil {
		// Best-effort: restore whatever we managed to read so the proxy still
		// forwards it, and skip model parsing.
		h.restoreBody(r, buf)
		return "", "", nil, routeResult{}
	}

	if len(buf) > maxModelPeekBytes {
		// Oversized body: we truncated the peek. Stitch the buffered prefix back
		// in front of the unread remainder so the upstream gets every byte, and
		// do NOT attempt to parse a partial JSON document.
		combined := io.MultiReader(bytes.NewReader(buf), r.Body)
		r.Body = io.NopCloser(combined)
		r.ContentLength = -1
		r.Header.Del("Content-Length")
		// Unparsed, so unsized: the scheduler budgets it as the whole pool.
		return "", "", nil, routeResult{}
	}

	// We hold the whole body; close the original and restore it (rewritten for
	// virtual models).
	_ = r.Body.Close()
	buf, rt := h.route(buf)
	h.restoreBody(r, buf)

	model, prompt, tools := parseRequest(buf)
	return model, prompt, tools, rt
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
	model, _, _ := parseRequest(buf)
	return model
}

func parseRequest(buf []byte) (types.Model, string, []types.ToolCall) {
	if len(bytes.TrimSpace(buf)) == 0 {
		return "", "", nil
	}
	var probe struct {
		Model    string `json:"model"`
		Input    any    `json:"input"`
		Messages []struct {
			Role       string `json:"role"`
			Content    any    `json:"content"`
			ToolCallID string `json:"tool_call_id"`
			ToolCalls  []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(buf, &probe); err != nil {
		return "", "", nil
	}
	var prompt string
	toolByID := make(map[string]int)
	var tools []types.ToolCall
	for _, message := range probe.Messages {
		if message.Role == "user" {
			prompt = contentText(message.Content)
		}
		for _, call := range message.ToolCalls {
			toolByID[call.ID] = len(tools)
			tools = append(tools, types.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments})
		}
		if message.Role == "tool" {
			result := contentText(message.Content)
			if i, ok := toolByID[message.ToolCallID]; ok {
				tools[i].Result = result
			} else {
				tools = append(tools, types.ToolCall{ID: message.ToolCallID, Result: result})
			}
		}
	}
	if prompt == "" && probe.Input != nil {
		prompt = contentText(probe.Input)
	}
	return types.Model(probe.Model), prompt, tools
}

func contentText(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		var out strings.Builder
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				if text, ok := m["text"].(string); ok {
					out.WriteString(text)
				}
			}
		}
		return out.String()
	default:
		raw, _ := json.Marshal(value)
		return string(raw)
	}
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
