package llamaswap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

func newTestEvictor(t *testing.T, baseURL, unloadPath string) types.Evictor {
	t.Helper()
	return New(types.LlamaSwapCfg{BaseURL: baseURL, UnloadPath: unloadPath}, nil)
}

func TestUnloadModelPathAndMethod(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	if err := ev.Unload(context.Background(), "qwen3.6-27b"); err != nil {
		t.Fatalf("Unload returned error: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if want := "/api/models/unload/qwen3.6-27b"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
}

func TestUnloadAll(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	if err := ev.Unload(context.Background(), ""); err != nil {
		t.Fatalf("Unload(\"\") returned error: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if want := "/api/models/unload"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
}

func TestUnloadPathEscaping(t *testing.T) {
	// The raw request-target is what the client actually sent on the wire; use
	// it to verify the model id was PathEscaped before substitution.
	var rawPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawPath = r.URL.EscapedPath()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	// A model id containing characters that require escaping in a path segment.
	if err := ev.Unload(context.Background(), "foo/bar baz"); err != nil {
		t.Fatalf("Unload returned error: %v", err)
	}
	want := "/api/models/unload/" + "foo%2Fbar%20baz"
	if rawPath != want {
		t.Errorf("escaped path = %q, want %q", rawPath, want)
	}
}

func TestUnloadNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "boom: model not found")
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	err := ev.Unload(context.Background(), "qwen3.6-27b")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error %q should mention status 500", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q should include a body snippet", err)
	}
}

func TestUnloadContextCancellation(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // block so the client-side context can cancel first
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := ev.Unload(ctx, "qwen3.6-27b")
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestRunningParses(t *testing.T) {
	const body = `{"running":[` +
		`{"model":"qwen3.6-27b","state":"ready","cmd":"x","proxy":"y","ttl":0,"name":"n","description":"d"},` +
		`{"model":"llama3-8b","state":"loading"}` +
		`]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != "/running" {
			t.Errorf("path = %q, want /running", r.URL.Path)
		}
		io.WriteString(w, body)
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	got, err := ev.Running(context.Background())
	if err != nil {
		t.Fatalf("Running returned error: %v", err)
	}
	want := []types.Model{"qwen3.6-27b", "llama3-8b"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("model[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRunningMalformedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "this is not json {[")
	}))
	defer srv.Close()

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	got, err := ev.Running(context.Background())
	if err != nil {
		t.Fatalf("malformed body should not error, got: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("malformed body should yield empty slice, got %v", got)
	}
}

func TestRunningDefaultUnloadPath(t *testing.T) {
	// New with empty UnloadPath should fall back to the documented default.
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ev := New(types.LlamaSwapCfg{BaseURL: srv.URL}, nil)
	if err := ev.Unload(context.Background(), "m1"); err != nil {
		t.Fatalf("Unload returned error: %v", err)
	}
	if want := "/api/models/unload/m1"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
}

// Ensure per-call default timeout is applied when ctx has no deadline; a slow
// server past the deadline should surface an error, but we keep this fast by
// verifying the deadline plumbing indirectly rather than waiting 120s.
func TestUnloadHonorsCallerDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	ev := newTestEvictor(t, srv.URL, "/api/models/unload/{model}")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := ev.Unload(ctx, "qwen3.6-27b")
	if err == nil {
		t.Fatal("expected deadline-exceeded error, got nil")
	}
}
