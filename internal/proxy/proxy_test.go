package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	activitystore "github.com/aiguy110/gpu-turnstile/internal/activity"
	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// ---- fakes implementing the frozen interfaces ----

type fakeTicket struct {
	granted      chan struct{}
	revoke       chan struct{}
	releaseCount int32
}

func newFakeTicket(grantedNow bool) *fakeTicket {
	t := &fakeTicket{
		granted: make(chan struct{}),
		revoke:  make(chan struct{}),
	}
	if grantedNow {
		close(t.granted)
	}
	return t
}

func (t *fakeTicket) Granted() <-chan struct{}         { return t.granted }
func (t *fakeTicket) RevokeRequested() <-chan struct{} { return t.revoke }
func (t *fakeTicket) Release()                         { atomic.AddInt32(&t.releaseCount, 1) }
func (t *fakeTicket) Heartbeat() bool                  { return false }
func (t *fakeTicket) Status() types.TicketStatus       { return types.TicketStatus{} }
func (t *fakeTicket) releases() int                    { return int(atomic.LoadInt32(&t.releaseCount)) }

type fakeScheduler struct {
	mu       sync.Mutex
	ticket   *fakeTicket
	lastSpec types.AcquireSpec
	acquired int
}

func (s *fakeScheduler) Acquire(spec types.AcquireSpec) types.Ticket {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSpec = spec
	s.acquired++
	return s.ticket
}

func (s *fakeScheduler) Snapshot() types.QueueSnapshot        { return types.QueueSnapshot{} }
func (s *fakeScheduler) History(time.Time) []types.HoldRecord { return nil }
func (s *fakeScheduler) Shutdown(ctx context.Context) error   { return nil }
func (s *fakeScheduler) spec() types.AcquireSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSpec
}

type fakeKeyStore struct {
	keys map[string]types.KeyInfo
	anon types.Priority
}

func (k *fakeKeyStore) Lookup(rawKey string) (types.KeyInfo, bool) {
	info, ok := k.keys[rawKey]
	return info, ok
}
func (k *fakeKeyStore) AnonymousPriority() types.Priority { return k.anon }
func (k *fakeKeyStore) Mint(string, types.Priority) (types.KeyInfo, error) {
	return types.KeyInfo{}, nil
}
func (k *fakeKeyStore) List() []types.KeyInfo { return nil }
func (k *fakeKeyStore) Revoke(string) error   { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---- tests ----

func TestProxyForwardsIntactBodyAndParsesModel(t *testing.T) {
	var upstreamBody string
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		b, _ := io.ReadAll(r.Body)
		upstreamBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b) // echo the body back
	}))
	defer upstream.Close()

	ticket := newFakeTicket(true)
	sched := &fakeScheduler{ticket: ticket}
	ks := &fakeKeyStore{keys: map[string]types.KeyInfo{
		"secret": {Name: "pi", Priority: 5},
	}, anon: 1}

	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	reqBody := `{"model":"qwen3-27b","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if got := atomic.LoadInt32(&upstreamHits); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
	if upstreamBody != reqBody {
		t.Errorf("upstream body mismatch:\n got: %s\nwant: %s", upstreamBody, reqBody)
	}
	if rec.Body.String() != reqBody {
		t.Errorf("client body mismatch:\n got: %s\nwant: %s", rec.Body.String(), reqBody)
	}
	if spec := sched.spec(); spec.Model != types.Model("qwen3-27b") {
		t.Errorf("AcquireSpec.Model = %q, want qwen3-27b", spec.Model)
	}
	if spec := sched.spec(); spec.Name != "pi" || spec.Priority != 5 || spec.Kind != types.KindLLM {
		t.Errorf("AcquireSpec = %+v, want name=pi priority=5 kind=llm", spec)
	}
	if spec := sched.spec(); spec.ID == "" {
		t.Error("AcquireSpec.ID is empty, want a random id")
	}
	if n := ticket.releases(); n != 1 {
		t.Errorf("Release called %d times, want exactly 1", n)
	}
}

func TestProxyStreamsResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("upstream ResponseWriter is not a Flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 3; i++ {
			io.WriteString(w, "data: chunk\n\n")
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	ticket := newFakeTicket(true)
	sched := &fakeScheduler{ticket: ticket}
	ks := &fakeKeyStore{anon: 1}

	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := strings.Count(rec.Body.String(), "data: chunk"); got != 3 {
		t.Errorf("streamed chunks = %d, want 3; body=%q", got, rec.Body.String())
	}
	if n := ticket.releases(); n != 1 {
		t.Errorf("Release called %d times, want exactly 1", n)
	}
}

func TestProxyObservesStreamingTextAndGenericToolCall(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"choices":[{"delta":{"content":"hello "}}]}`+"\n\n")
		io.WriteString(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"lookup","arguments":"{\"q\":"}}]}}]}`+"\n\n")
		io.WriteString(w, `data: {"choices":[{"delta":{"content":"world","tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}],"usage":{"completion_tokens":2}}`+"\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	store, err := activitystore.New(types.ActivityCfg{}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	h := NewObserved(sched, &fakeKeyStore{anon: 1}, types.LlamaSwapCfg{BaseURL: upstream.URL}, store, testLogger())
	reqBody := `{"model":"m","messages":[{"role":"user","content":"say hi"}]}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody)))

	list := store.List()
	if len(list) != 1 {
		t.Fatalf("activity count = %d", len(list))
	}
	got := list[0]
	if got.Status != types.ActivityCompleted || got.Prompt != "say hi" || got.Output != "hello world" || got.OutputTokens != 2 {
		t.Fatalf("activity = %+v", got)
	}
	if len(got.ToolCalls) != 1 || got.ToolCalls[0].Name != "lookup" || got.ToolCalls[0].Arguments != `{"q":1}` {
		t.Fatalf("tool calls = %+v", got.ToolCalls)
	}
}

func TestProxyClientCancelBeforeGrant(t *testing.T) {
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
	}))
	defer upstream.Close()

	ticket := newFakeTicket(false) // never granted
	sched := &fakeScheduler{ticket: ticket}
	ks := &fakeKeyStore{anon: 1}

	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	// Give the handler a moment to enqueue and block on the grant.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after client cancel")
	}

	if got := atomic.LoadInt32(&upstreamHits); got != 0 {
		t.Errorf("upstream hits = %d, want 0 (never granted)", got)
	}
	if n := ticket.releases(); n != 1 {
		t.Errorf("Release called %d times, want exactly 1", n)
	}
}

func TestProxyUnknownKeyUsesAnonymousPriority(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	ticket := newFakeTicket(true)
	sched := &fakeScheduler{ticket: ticket}
	ks := &fakeKeyStore{anon: 2} // no keys registered

	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set("Authorization", "Bearer nope")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	spec := sched.spec()
	if spec.Priority != 2 {
		t.Errorf("AcquireSpec.Priority = %d, want anonymous 2", spec.Priority)
	}
	if spec.Name != anonymousName {
		t.Errorf("AcquireSpec.Name = %q, want %q", spec.Name, anonymousName)
	}
}

func TestBearerTokenFallbackToXApiKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	ks := &fakeKeyStore{keys: map[string]types.KeyInfo{
		"xkey": {Name: "viax", Priority: 9},
	}, anon: 1}

	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("X-Api-Key", "xkey")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if spec := sched.spec(); spec.Name != "viax" || spec.Priority != 9 {
		t.Errorf("X-Api-Key not honored: spec=%+v", spec)
	}
}

func TestProxyNonJSONBodyStillProxied(t *testing.T) {
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamBody = string(b)
	}))
	defer upstream.Close()

	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	ks := &fakeKeyStore{anon: 1}
	h := New(sched, ks, types.LlamaSwapCfg{BaseURL: upstream.URL}, testLogger())

	body := "this is not json"
	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if upstreamBody != body {
		t.Errorf("upstream body = %q, want %q", upstreamBody, body)
	}
	if spec := sched.spec(); spec.Model != types.Model("") {
		t.Errorf("model = %q, want empty for non-JSON", spec.Model)
	}
}
