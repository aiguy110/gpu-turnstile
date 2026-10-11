package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

func testRouting() types.RoutingCfg {
	return types.RoutingCfg{
		Models: map[types.Model]types.ModelCfg{"bonsai-2-27b": {Slots: 2, CtxPool: 98304}},
		Virtual: map[types.Model]types.VirtualModelCfg{
			"bonsai-agent": {Backend: "bonsai-2-27b", MaxCtx: 81920},
			"bonsai-mem": {Backend: "bonsai-2-27b", MaxCtx: 12288, DefaultMaxTokens: 1024,
				Inject: map[string]any{
					"chat_template_kwargs": map[string]any{"enable_thinking": false},
					"id_slot":              1,
				}},
		},
	}
}

type upstreamCapture struct {
	body   string
	length string
	hits   int32
	server *httptest.Server
}

func newUpstream(t *testing.T) *upstreamCapture {
	c := &upstreamCapture{}
	c.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&c.hits, 1)
		b, _ := io.ReadAll(r.Body)
		c.body = string(b)
		c.length = r.Header.Get("Content-Length")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	t.Cleanup(c.server.Close)
	return c
}

func serve(t *testing.T, up *upstreamCapture, sched *fakeScheduler, body string) *httptest.ResponseRecorder {
	t.Helper()
	h := NewObserved(sched, &fakeKeyStore{anon: 1}, types.LlamaSwapCfg{BaseURL: up.server.URL}, testRouting(), nil, testLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	return rec
}

func TestVirtualModelRewriteAndDeepMergeInject(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	serve(t, up, sched, `{"model":"bonsai-mem","messages":[{"role":"user","content":"hi"}],`+
		`"chat_template_kwargs":{"enable_thinking":true,"keep":"me"},"id_slot":0,"temperature":0.2}`)

	var got map[string]any
	if err := json.Unmarshal([]byte(up.body), &got); err != nil {
		t.Fatalf("upstream body not JSON: %v (%s)", err, up.body)
	}
	if got["model"] != "bonsai-2-27b" {
		t.Errorf("model = %v, want backend name", got["model"])
	}
	kw := got["chat_template_kwargs"].(map[string]any)
	if kw["enable_thinking"] != false || kw["keep"] != "me" {
		t.Errorf("chat_template_kwargs = %v, want inject to win and client keys to survive", kw)
	}
	if got["id_slot"] != float64(1) || got["temperature"] != 0.2 {
		t.Errorf("id_slot/temperature = %v/%v", got["id_slot"], got["temperature"])
	}
	if up.length != strconv.Itoa(len(up.body)) {
		t.Errorf("Content-Length %s != body length %d", up.length, len(up.body))
	}
	spec := sched.spec()
	if spec.Model != "bonsai-2-27b" || spec.Alias != "bonsai-mem" {
		t.Errorf("spec model/alias = %q/%q", spec.Model, spec.Alias)
	}
	// "hi" = 1 token (ceil 2/3) + 8 overhead; no max_tokens → defaultMaxTokens 1024.
	if spec.CtxTokens != 9+1024 {
		t.Errorf("CtxTokens = %d, want %d", spec.CtxTokens, 9+1024)
	}
}

func TestEstimateUsesMaxTokensAndTools(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	content := strings.Repeat("x", 3000) // 1000 tokens
	serve(t, up, sched, `{"model":"bonsai-agent","max_tokens":500,"messages":[{"role":"user","content":"`+content+`"}]}`)
	if got := sched.spec().CtxTokens; got != 1000+8+500 {
		t.Errorf("CtxTokens = %d, want %d", got, 1508)
	}
	serve(t, up, sched, `{"model":"bonsai-agent","max_tokens":500,"tools":[{"type":"function","function":{"name":"read"}}],"messages":[{"role":"user","content":"`+content+`"}]}`)
	withTools := sched.spec().CtxTokens
	if withTools <= 1508 {
		t.Errorf("tool definitions should add to the estimate, got %d", withTools)
	}
}

func TestUnboundedRequestBudgetsRestOfContext(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	serve(t, up, sched, `{"model":"bonsai-agent","messages":[{"role":"user","content":"hi"}]}`)
	if got := sched.spec().CtxTokens; got != 81920 {
		t.Errorf("unbounded agent request CtxTokens = %d, want maxCtx 81920", got)
	}
	serve(t, up, sched, `{"model":"bonsai-2-27b","messages":[{"role":"user","content":"hi"}]}`)
	if got := sched.spec().CtxTokens; got != 98304 {
		t.Errorf("unbounded real-model request CtxTokens = %d, want pool 98304", got)
	}
}

func TestOverMaxCtxRejectedWithoutQueueing(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	content := strings.Repeat("y", 3*12000)
	rec := serve(t, up, sched, `{"model":"bonsai-mem","messages":[{"role":"user","content":"`+content+`"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Error.Code != "context_length_exceeded" {
		t.Errorf("error code = %q (%s)", e.Error.Code, rec.Body.String())
	}
	if sched.acquired != 0 || atomic.LoadInt32(&up.hits) != 0 {
		t.Errorf("rejected request must not queue (acquired=%d) or reach upstream (hits=%d)", sched.acquired, up.hits)
	}
	// Explicit max_tokens pushing past maxCtx is rejected too.
	rec = serve(t, up, sched, `{"model":"bonsai-mem","max_tokens":12288,"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for max_tokens over maxCtx", rec.Code)
	}
}

func TestUnconfiguredModelPassesThroughUntouched(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	body := `{"model":"qwen3.6-27b",   "messages":[{"role":"user","content":"hi"}]}`
	serve(t, up, sched, body)
	if up.body != body {
		t.Errorf("body changed:\n got %s\nwant %s", up.body, body)
	}
	if spec := sched.spec(); spec.CtxTokens != 0 || spec.Alias != "" || spec.Model != "qwen3.6-27b" {
		t.Errorf("spec = %+v, want untouched", spec)
	}
}

func TestOversizedBodyIsUnsized(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	big := `{"model":"bonsai-2-27b","messages":[{"role":"user","content":"` + strings.Repeat("z", maxModelPeekBytes) + `"}]}`
	serve(t, up, sched, big)
	if up.body != big {
		t.Error("oversized body must be forwarded intact")
	}
	if spec := sched.spec(); spec.CtxTokens != 0 {
		t.Errorf("oversized CtxTokens = %d, want 0 (budgeted as the whole pool)", spec.CtxTokens)
	}
}

func TestInjectedMaxTokensIsBudgeted(t *testing.T) {
	up := newUpstream(t)
	sched := &fakeScheduler{ticket: newFakeTicket(true)}
	routing := testRouting()
	mem := routing.Virtual["bonsai-mem"]
	mem.Inject = map[string]any{"max_tokens": 2048} // YAML decodes numbers as int
	routing.Virtual["bonsai-mem"] = mem
	h := NewObserved(sched, &fakeKeyStore{anon: 1}, types.LlamaSwapCfg{BaseURL: up.server.URL}, routing, nil, testLogger())
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"bonsai-mem","messages":[{"role":"user","content":"hi"}]}`)))
	if got := sched.spec().CtxTokens; got != 9+2048 {
		t.Errorf("CtxTokens = %d, want %d (injected max_tokens)", got, 9+2048)
	}
}
