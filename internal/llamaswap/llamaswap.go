// Package llamaswap implements types.Evictor by talking to an upstream
// llama-swap server. It is the only turnstile package that speaks the
// llama-swap HTTP API; it depends solely on internal/types.
package llamaswap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

const (
	// defaultUnloadPath matches config.example.yaml and llama-swap's route
	// POST /api/models/unload/{model...}.
	defaultUnloadPath = "/api/models/unload/{model}"
	// unloadAllPath is llama-swap's endpoint to unload every resident model.
	unloadAllPath = "/api/models/unload"
	// runningPath returns the currently-loaded models.
	runningPath = "/running"

	// modelPlaceholder is the literal substring replaced with the escaped model id.
	modelPlaceholder = "{model}"

	// unloadDefaultTimeout bounds a synchronous unload when the caller's ctx has
	// no deadline. Unload blocks until the upstream process exits and VRAM frees.
	unloadDefaultTimeout = 120 * time.Second
	// runningTimeout bounds the /running GET when the caller's ctx has no deadline.
	runningTimeout = 10 * time.Second

	// bodySnippetLimit caps how much of an error response body we quote.
	bodySnippetLimit = 512
)

// evictor is the concrete types.Evictor.
type evictor struct {
	baseURL    string
	unloadPath string
	client     *http.Client
	log        *slog.Logger
}

// New constructs a types.Evictor for the given llama-swap config.
func New(cfg types.LlamaSwapCfg, log *slog.Logger) types.Evictor {
	if log == nil {
		log = slog.Default()
	}
	unloadPath := cfg.UnloadPath
	if unloadPath == "" {
		unloadPath = defaultUnloadPath
	}
	return &evictor{
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		unloadPath: unloadPath,
		// Shared client (never http.DefaultClient). Per-call timeouts are
		// enforced via context so a single client is safe to reuse.
		client: &http.Client{},
		log:    log,
	}
}

// Unload evicts the named model, or all resident models when model == "".
// It is synchronous: llama-swap's unload endpoint blocks until VRAM frees.
func (e *evictor) Unload(ctx context.Context, model types.Model) error {
	var path string
	if model == "" {
		path = unloadAllPath
	} else {
		// Replace the literal "{model}" with the path-escaped model id.
		escaped := url.PathEscape(string(model))
		path = strings.ReplaceAll(e.unloadPath, modelPlaceholder, escaped)
	}

	// Apply a generous default timeout only when the caller supplied no deadline.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, unloadDefaultTimeout)
		defer cancel()
	}

	fullURL := e.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, nil)
	if err != nil {
		return fmt.Errorf("llamaswap: build unload request for %q: %w", fullURL, err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("llamaswap: unload %q: %w", fullURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet := readSnippet(resp.Body)
		e.log.Warn("llamaswap unload failed",
			"url", fullURL, "status", resp.StatusCode, "body", snippet)
		return fmt.Errorf("llamaswap: unload %q: status %d: %s", fullURL, resp.StatusCode, snippet)
	}

	e.log.Debug("llamaswap unload ok", "url", fullURL, "model", string(model))
	return nil
}

// runningResponse mirrors llama-swap's GET /running body.
type runningResponse struct {
	Running []struct {
		Model string `json:"model"`
	} `json:"running"`
}

// Running reports the models currently loaded upstream. It is display/sanity
// only and must never hard-fail the caller: any parse/shape problem yields an
// empty slice and nil error. Genuine transport errors are still returned.
func (e *evictor) Running(ctx context.Context) ([]types.Model, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, runningTimeout)
		defer cancel()
	}

	fullURL := e.baseURL + runningPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("llamaswap: build running request for %q: %w", fullURL, err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llamaswap: running %q: %w", fullURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		e.log.Warn("llamaswap running non-2xx", "url", fullURL, "status", resp.StatusCode)
		return nil, fmt.Errorf("llamaswap: running %q: status %d", fullURL, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("llamaswap: running %q: read body: %w", fullURL, err)
	}

	var parsed runningResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		// Defensive: malformed body is not a hard failure for a display call.
		e.log.Debug("llamaswap running body did not parse", "url", fullURL, "err", err)
		return []types.Model{}, nil
	}

	models := make([]types.Model, 0, len(parsed.Running))
	for _, r := range parsed.Running {
		models = append(models, types.Model(r.Model))
	}
	return models, nil
}

// readSnippet reads a bounded, single-line snippet of an error response body.
func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, bodySnippetLimit))
	s := strings.TrimSpace(string(b))
	s = strings.ReplaceAll(s, "\n", " ")
	if s == "" {
		return "(empty body)"
	}
	return s
}
