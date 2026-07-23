package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// ---- fakes ----

type fakeTicket struct {
	mu         sync.Mutex
	grantedCh  chan struct{}
	revokeCh   chan struct{}
	status     types.TicketStatus
	heartbeats int
	revoke     bool
	released   int
}

func newFakeTicket() *fakeTicket {
	return &fakeTicket{
		grantedCh: make(chan struct{}),
		revokeCh:  make(chan struct{}),
	}
}

func (t *fakeTicket) grant() { close(t.grantedCh) }

func (t *fakeTicket) Granted() <-chan struct{}         { return t.grantedCh }
func (t *fakeTicket) RevokeRequested() <-chan struct{} { return t.revokeCh }

func (t *fakeTicket) Release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.released++
}

func (t *fakeTicket) Heartbeat() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heartbeats++
	return t.revoke
}

func (t *fakeTicket) Status() types.TicketStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

func (t *fakeTicket) heartbeatCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.heartbeats
}

func (t *fakeTicket) releaseCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.released
}

type schedulerAdapter struct {
	ticket   *fakeTicket
	lastSpec types.AcquireSpec
}

func (s *schedulerAdapter) Acquire(spec types.AcquireSpec) types.Ticket {
	s.lastSpec = spec
	return s.ticket
}
func (s *schedulerAdapter) Snapshot() types.QueueSnapshot        { return types.QueueSnapshot{} }
func (s *schedulerAdapter) History(time.Time) []types.HoldRecord { return nil }
func (s *schedulerAdapter) Shutdown(_ context.Context) error     { return nil }

type fakeKeyStore struct {
	info  types.KeyInfo
	found bool
	anon  types.Priority
}

func (k *fakeKeyStore) Lookup(raw string) (types.KeyInfo, bool) { return k.info, k.found }
func (k *fakeKeyStore) AnonymousPriority() types.Priority       { return k.anon }
func (k *fakeKeyStore) Mint(name string, p types.Priority) (types.KeyInfo, error) {
	return types.KeyInfo{}, nil
}
func (k *fakeKeyStore) List() []types.KeyInfo         { return nil }
func (k *fakeKeyStore) Revoke(nameOrKey string) error { return nil }

// ---- helpers ----

func newTestHandler(t *fakeTicket) (*Handler, *schedulerAdapter, *fakeKeyStore) {
	sched := &schedulerAdapter{ticket: t}
	ks := &fakeKeyStore{anon: 1}
	h := New(sched, ks, types.LeaseCfg{HeartbeatTimeoutSeconds: 30, PollAfterMs: 1000},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h, sched, ks
}

func do(h *Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode: %v (body=%q)", err, rec.Body.String())
	}
	return m
}

// ---- tests ----

func TestCreateGrantedImmediately(t *testing.T) {
	ticket := newFakeTicket()
	ticket.grant() // already granted
	h, _, _ := newTestHandler(ticket)

	rec := do(h, "POST", "/lease", []byte(`{"est_seconds":5}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	m := decode(t, rec)
	if m["status"] != "granted" {
		t.Fatalf("want granted, got %v", m["status"])
	}
	if m["lease_id"] == "" || m["lease_id"] == nil {
		t.Fatalf("missing lease_id")
	}
}

func TestCreateQueued(t *testing.T) {
	ticket := newFakeTicket()
	h, _, _ := newTestHandler(ticket)

	rec := do(h, "POST", "/lease", []byte(`{}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", rec.Code)
	}
	m := decode(t, rec)
	if m["status"] != "queued" {
		t.Fatalf("want queued, got %v", m["status"])
	}
	if m["poll_after_ms"] != float64(1000) {
		t.Fatalf("want poll_after_ms=1000, got %v", m["poll_after_ms"])
	}
}

func TestGetFlipsQueuedToGranted(t *testing.T) {
	ticket := newFakeTicket()
	ticket.status = types.TicketStatus{Position: 2}
	h, _, _ := newTestHandler(ticket)

	rec := do(h, "POST", "/lease", []byte(`{}`))
	id := decode(t, rec)["lease_id"].(string)

	// queued
	rec = do(h, "GET", "/lease/"+id, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	m := decode(t, rec)
	if m["status"] != "queued" {
		t.Fatalf("want queued, got %v", m["status"])
	}
	if m["position"] != float64(2) {
		t.Fatalf("want position 2, got %v", m["position"])
	}
	if ticket.heartbeatCount() < 1 {
		t.Fatalf("GET should call Heartbeat, got %d", ticket.heartbeatCount())
	}

	// grant, then GET flips
	ticket.grant()
	rec = do(h, "GET", "/lease/"+id, nil)
	m = decode(t, rec)
	if m["status"] != "granted" {
		t.Fatalf("want granted after grant, got %v", m["status"])
	}
	if ticket.heartbeatCount() < 2 {
		t.Fatalf("second GET should call Heartbeat again")
	}
}

func TestHeartbeatReturnsRevokeFlag(t *testing.T) {
	ticket := newFakeTicket()
	ticket.revoke = true
	h, _, _ := newTestHandler(ticket)

	rec := do(h, "POST", "/lease", []byte(`{}`))
	id := decode(t, rec)["lease_id"].(string)

	rec = do(h, "POST", "/lease/"+id+"/heartbeat", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	m := decode(t, rec)
	if m["revoke_requested"] != true {
		t.Fatalf("want revoke_requested=true, got %v", m["revoke_requested"])
	}
}

func TestReleaseIdempotent(t *testing.T) {
	ticket := newFakeTicket()
	h, _, _ := newTestHandler(ticket)

	rec := do(h, "POST", "/lease", []byte(`{}`))
	id := decode(t, rec)["lease_id"].(string)

	rec = do(h, "POST", "/lease/"+id+"/release", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("first release want 200, got %d", rec.Code)
	}
	rec = do(h, "POST", "/lease/"+id+"/release", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("second release want 200, got %d", rec.Code)
	}
	if ticket.releaseCount() != 1 {
		t.Fatalf("Release should be called exactly once, got %d", ticket.releaseCount())
	}
}

func TestReleaseUnknownID(t *testing.T) {
	ticket := newFakeTicket()
	h, _, _ := newTestHandler(ticket)
	rec := do(h, "POST", "/lease/deadbeef/release", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("unknown release want 200, got %d", rec.Code)
	}
}

func TestGetUnknownID(t *testing.T) {
	ticket := newFakeTicket()
	h, _, _ := newTestHandler(ticket)
	rec := do(h, "GET", "/lease/nope", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown GET want 404, got %d", rec.Code)
	}
}

func TestAuthResolvesPriority(t *testing.T) {
	ticket := newFakeTicket()
	sched := &schedulerAdapter{ticket: ticket}
	ks := &fakeKeyStore{
		info:  types.KeyInfo{Name: "pi", Priority: 5},
		found: true,
		anon:  1,
	}
	h := New(sched, ks, types.LeaseCfg{PollAfterMs: 100},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest("POST", "/lease", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if sched.lastSpec.Name != "pi" || sched.lastSpec.Priority != 5 {
		t.Fatalf("want name=pi priority=5, got %+v", sched.lastSpec)
	}
	if sched.lastSpec.Kind != types.KindLease {
		t.Fatalf("want KindLease, got %v", sched.lastSpec.Kind)
	}
}
