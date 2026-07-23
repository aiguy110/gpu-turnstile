package web

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// ---- fakes ----

type fakeScheduler struct {
	snap types.QueueSnapshot
	hist []types.HoldRecord
}

func (f *fakeScheduler) Acquire(spec types.AcquireSpec) types.Ticket { return nil }
func (f *fakeScheduler) Snapshot() types.QueueSnapshot               { return f.snap }
func (f *fakeScheduler) History(time.Time) []types.HoldRecord        { return f.hist }
func (f *fakeScheduler) Shutdown(ctx context.Context) error          { return nil }

type fakeKeyStore struct {
	list        []types.KeyInfo
	minted      types.KeyInfo
	mintName    string
	mintPrio    types.Priority
	mintErr     error
	revokedWith string
	revokeErr   error
}

func (f *fakeKeyStore) Lookup(rawKey string) (types.KeyInfo, bool) { return types.KeyInfo{}, false }
func (f *fakeKeyStore) AnonymousPriority() types.Priority          { return 0 }
func (f *fakeKeyStore) List() []types.KeyInfo                      { return f.list }
func (f *fakeKeyStore) Mint(name string, priority types.Priority) (types.KeyInfo, error) {
	f.mintName = name
	f.mintPrio = priority
	if f.mintErr != nil {
		return types.KeyInfo{}, f.mintErr
	}
	f.minted = types.KeyInfo{Name: name, Priority: priority, Key: "secret-xyz", Created: time.Now()}
	return f.minted, nil
}
func (f *fakeKeyStore) Revoke(nameOrKey string) error {
	f.revokedWith = nameOrKey
	return f.revokeErr
}

func newTestHandler(sched types.Scheduler, ks types.KeyStore) *Handler {
	return New(sched, ks, nil)
}

// ---- tests ----

func TestQueueSnapshotJSON(t *testing.T) {
	enq := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	snap := types.QueueSnapshot{
		Holder: &types.TicketStatus{
			ID: "h1", Name: "pi", Priority: 5, Kind: types.KindLease,
			State: types.StateGranted, Position: 0,
		},
		Pending: []types.TicketStatus{
			{ID: "w1", Name: "bob", Priority: 3, Kind: types.KindLLM, Model: "qwen",
				State: types.StateQueued, Position: 1, EnqueuedAt: enq},
		},
		WarmModel: "qwen",
		WarmTier:  3,
		UpdatedAt: enq,
	}
	h := newTestHandler(&fakeScheduler{snap: snap}, &fakeKeyStore{})

	req := httptest.NewRequest(http.MethodGet, "/api/queue", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	var got types.QueueSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Holder == nil || got.Holder.Name != "pi" || got.Holder.Kind != types.KindLease {
		t.Fatalf("holder mismatch: %+v", got.Holder)
	}
	if len(got.Pending) != 1 || got.Pending[0].Name != "bob" || got.Pending[0].Position != 1 {
		t.Fatalf("pending mismatch: %+v", got.Pending)
	}
	if got.WarmModel != "qwen" || got.WarmTier != 3 {
		t.Fatalf("warm mismatch: model=%q tier=%d", got.WarmModel, got.WarmTier)
	}
}

func TestListKeysRedacted(t *testing.T) {
	ks := &fakeKeyStore{list: []types.KeyInfo{
		{Name: "pi", Priority: 5, Created: time.Now()}, // no secret (already redacted)
		{Name: "bob", Priority: 2, Created: time.Now()},
	}}
	h := newTestHandler(&fakeScheduler{}, ks)

	req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "secret") || strings.Contains(body, "\"key\"") {
		t.Fatalf("listing leaked a secret: %s", body)
	}
	var got []types.KeyInfo
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 2 || got[0].Name != "pi" || got[0].Priority != 5 {
		t.Fatalf("keys mismatch: %+v", got)
	}
	for _, k := range got {
		if k.Key != "" {
			t.Fatalf("key %s carried a secret", k.Name)
		}
	}
}

func TestMintKey(t *testing.T) {
	ks := &fakeKeyStore{}
	h := newTestHandler(&fakeScheduler{}, ks)

	req := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"newby","priority":7}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if ks.mintName != "newby" || ks.mintPrio != 7 {
		t.Fatalf("Mint called with (%q, %d)", ks.mintName, ks.mintPrio)
	}
	var got types.KeyInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Key != "secret-xyz" {
		t.Fatalf("mint response missing secret, got key=%q", got.Key)
	}
	if got.Name != "newby" || got.Priority != 7 {
		t.Fatalf("mint response mismatch: %+v", got)
	}
}

func TestMintKeyEmptyName(t *testing.T) {
	ks := &fakeKeyStore{}
	h := newTestHandler(&fakeScheduler{}, ks)

	req := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"   ","priority":1}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ks.mintName != "" {
		t.Fatalf("Mint should not have been called, got %q", ks.mintName)
	}
}

func TestRevokeKeySuccess(t *testing.T) {
	ks := &fakeKeyStore{}
	h := newTestHandler(&fakeScheduler{}, ks)

	req := httptest.NewRequest(http.MethodDelete, "/api/keys/pi", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if ks.revokedWith != "pi" {
		t.Fatalf("Revoke called with %q, want pi", ks.revokedWith)
	}
}

func TestRevokeKeyNotFound(t *testing.T) {
	ks := &fakeKeyStore{revokeErr: errors.New("no key matching name or secret")}
	h := newTestHandler(&fakeScheduler{}, ks)

	req := httptest.NewRequest(http.MethodDelete, "/api/keys/ghost", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestIndexHTML(t *testing.T) {
	h := newTestHandler(&fakeScheduler{}, &fakeKeyStore{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type = %q, want text/html", ct)
	}
	if !strings.Contains(rec.Body.String(), "turnstile") {
		t.Fatalf("index HTML did not contain expected content")
	}
}

func TestQueueStreamSSE(t *testing.T) {
	snap := types.QueueSnapshot{WarmModel: "qwen", WarmTier: 4, UpdatedAt: time.Now()}
	h := newTestHandler(&fakeScheduler{snap: snap}, &fakeKeyStore{})

	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/queue/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	// Read the first data event (sent immediately on connect).
	reader := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(1500 * time.Millisecond)
	var dataLine string
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if strings.HasPrefix(line, "data: ") {
			dataLine = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			break
		}
		if err == io.EOF {
			break
		}
	}
	cancel()

	if dataLine == "" {
		t.Fatalf("no data event received")
	}
	var got types.QueueSnapshot
	if err := json.Unmarshal([]byte(dataLine), &got); err != nil {
		t.Fatalf("data event was not valid snapshot JSON: %v (%q)", err, dataLine)
	}
	if got.WarmModel != "qwen" || got.WarmTier != 4 {
		t.Fatalf("stream snapshot mismatch: %+v", got)
	}
}
