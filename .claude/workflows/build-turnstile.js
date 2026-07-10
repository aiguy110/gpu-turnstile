export const meta = {
  name: 'build-turnstile',
  description:
    'Implement the turnstile Go packages in parallel against the frozen internal/types contract, then integrate until go build/test pass.',
}

const repo = '/home/josiah/Projects/gpu-turnstile'

// Shared preamble handed to every agent. The contract in internal/types is
// FROZEN — no agent may change it.
const contract = `
You are implementing one package of the "turnstile" Go service (module
github.com/aiguy110/gpu-turnstile) at ${repo}.

FIRST, read the frozen contract and design notes — do not restate them back, just obey them:
  - ${repo}/internal/types/types.go   (shared types + interfaces)
  - ${repo}/internal/types/doc.go     (HTTP wiring contract + the exact scheduler policy)
  - ${repo}/README.md                 (behavior overview)
  - ${repo}/config.example.yaml       (config shape)

HARD RULES:
  - internal/types is FROZEN. Do NOT edit it or any package other than the one assigned to you.
  - Match the constructor signature in doc.go EXACTLY (name, params, return type).
  - Use log/slog for logging. Never call log.Fatal / os.Exit; return errors.
  - Standard library only unless your spec says otherwise (do NOT run "go get" or edit go.mod / go.sum — the integration step owns dependencies).
  - Your package depends ONLY on internal/types. Do not import sibling packages (proxy/lease/queue/keys/web/llamaswap).
  - Before finishing: run "gofmt -w" on your files, then
    "cd ${repo} && go test ./internal/<yourpkg>/... 2>&1" and make it pass.
    Your package + its tests must build and pass in isolation against internal/types.
  - Write focused table-driven tests covering the behavior your spec calls out.
Report a 3-5 line summary of what you implemented and the test result.
`

const packages = [
  {
    name: 'keys',
    spec: `
Implement internal/keys implementing types.KeyStore, constructor:
  keys.New(seed []types.KeyInfo, anon types.Priority) (types.KeyStore, error)
- In-memory, sync.RWMutex-guarded map keyed by the raw secret. Seed it from the
  provided KeyInfo slice (these carry real secrets from config).
- Lookup(raw) resolves a secret -> KeyInfo (found=false when absent/empty).
- AnonymousPriority() returns the configured anon tier.
- Mint(name, priority) generates a cryptographically-random secret
  (crypto/rand, url-safe, prefix "tk-"), stores it with Created=now, returns the
  KeyInfo WITH the secret populated. Duplicate name is allowed (names are labels).
- List() returns all keys with Key redacted ("") — never leak secrets.
- Revoke(nameOrKey) removes by exact secret OR by name (all keys with that name);
  return an error if nothing matched.
Note: persistence is intentionally out of scope — keys live in memory, reseeded
from config on boot. Tests: seeding+lookup, anonymous fallback, mint uniqueness,
list redaction, revoke by name and by key.
`,
  },
  {
    name: 'llamaswap',
    spec: `
Implement internal/llamaswap implementing types.Evictor, constructor:
  llamaswap.New(cfg types.LlamaSwapCfg, log *slog.Logger) types.Evictor
- Unload(ctx, model): POST to cfg.BaseURL + cfg.UnloadPath with "{model}"
  substituted by url.PathEscape(model). If model=="", unload all via
  "POST {BaseURL}/api/models/unload". This call is meant to be SYNCHRONOUS
  (llama-swap blocks until the upstream process exits); use the ctx for
  cancellation and a generous default timeout (e.g. 120s) when ctx has none.
  Treat 2xx as success; non-2xx -> error including status + body snippet.
- Running(ctx): GET "{BaseURL}/running", parse llama-swap's JSON, and return the
  loaded model ids as []types.Model. Be defensive: unknown/absent shape -> empty
  slice + nil error is acceptable (Running is display/sanity only). Handle both a
  bare array and an object with a "running"/"models" field if unsure.
Use net/http with a shared *http.Client. Tests: drive Unload/Running against an
httptest.Server (assert the path/{model} substitution, all-unload path, error on
500, ctx cancellation).
`,
  },
  {
    name: 'queue',
    spec: `
Implement internal/queue: the concrete Scheduler + Ticket. THIS IS THE CORE.
Constructor:
  queue.New(cfg types.SchedulerConfig, ev types.Evictor, log *slog.Logger) types.Scheduler
Implement EXACTLY the "Scheduler policy" in internal/types/doc.go:
- A single owner goroutine serializes ALL state (communicate via channels; do not
  share mutable state across goroutines). Acquire(spec) returns a Ticket
  immediately without blocking; the ticket's Granted() channel closes when it
  wins the GPU.
- Service order: priority DESC, then FIFO by an increasing enqueue sequence.
  Admission: grant the front waiter only when there is no holder AND no strictly
  higher-priority waiter pending.
- Ticket: Granted()/RevokeRequested() return channels; Release() is idempotent
  and removes the ticket whether queued or granted; Heartbeat() marks liveness
  and returns whether revoke was requested; Status() returns a TicketStatus with
  correct Position (0 = holder/next).
- Warm-model tracking: WarmModel/WarmTier follow the most recent granted KindLLM
  ticket's Model/Priority; cleared to "" after an Unload.
- Dynamic-TTL eviction (see doc.go): decide when to Evictor.Unload the warm model
  before granting the next waiter. No pending -> never unload. Pending P<=WarmTier
  -> wait cfg.TTL.GraceSeconds since last LLM completion (reset by any new LLM
  grant of tier>=WarmTier), then unload+grant. Pending P>WarmTier -> unload
  immediately then grant. Optional cfg.TTL.IdleFloorSeconds absolute idle unload.
  Eviction is only needed when the next waiter is KindLease OR a KindLLM needing a
  DIFFERENT model than WarmModel.
- Cooperative preemption: when a waiter of priority strictly greater than the
  current LEASE holder is admitted, close the holder's RevokeRequested and set its
  status flag; wait up to cfg.Preemption.GraceSeconds for Release, else proceed
  once the holder's heartbeat TTL (cfg.Lease.HeartbeatTimeoutSeconds) lapses.
  Never revoke KindLLM holders.
- Heartbeat reaping: a granted/queued lease with no Heartbeat within the timeout
  is released as dead.
- Shutdown(ctx) stops the goroutine and releases the holder.
TESTABILITY: make time injectable (accept an optional clock, or a package-level
"now" var + a way to advance timers in tests) so tests don't sleep for minutes —
this is important, design for it. Provide a fake Evictor in tests recording
Unload calls.
Tests (thorough, this is the linchpin): priority ordering; FIFO within priority;
no-start-while-higher-pending; grant->release->next; immediate eviction for
higher-priority waiter; grace-window hold for <=warm-tier waiter and reset on new
LLM; no eviction when same model reused; cooperative revoke signaling; heartbeat
reaping; Position correctness; Snapshot shape.
`,
  },
  {
    name: 'proxy',
    spec: `
Implement internal/proxy exposing:
  proxy.New(sched types.Scheduler, ks types.KeyStore, cfg types.LlamaSwapCfg, log *slog.Logger) *proxy.Handler
  func (*Handler) ServeHTTP(w, r)      // handles /v1/* OpenAI-compatible calls
  func (*Handler) Routes() http.Handler
- Extract the bearer token (Authorization: Bearer X, or X-Api-Key). ks.Lookup ->
  priority; on miss use ks.AnonymousPriority(). Name = keyinfo.Name or "anon".
- Peek the request body to read the JSON "model" field WITHOUT consuming it for
  the upstream (buffer it, then restore r.Body from the buffer). Non-JSON/absent
  model -> Model "".
- Acquire(KindLLM, id=random, name, priority, model). Wait on
  ticket.Granted(). If r.Context() is canceled first, ticket.Release() and abort
  (499/return). On grant, reverse-proxy to cfg.BaseURL using
  httputil.ReverseProxy (must stream SSE responses; do not buffer the response).
  ticket.Release() exactly once when the upstream response completes (use the
  proxy's ModifyResponse/error hooks or wrap so Release always runs).
- Routes() returns a mux mounting this handler under "/v1/".
Tests: httptest upstream + a fake Scheduler (granting immediately) — assert the
request is proxied, the model is parsed from the body, the body still reaches
upstream intact, and Release is called. Also a test where Granted is delayed and
client cancels -> Release without proxying.
`,
  },
  {
    name: 'lease',
    spec: `
Implement internal/lease exposing:
  lease.New(sched types.Scheduler, ks types.KeyStore, cfg types.LeaseCfg, log *slog.Logger) *lease.Handler
  func (*Handler) ServeHTTP(w, r)
  func (*Handler) Routes() http.Handler   // mounts /lease and /lease/...
Endpoints (JSON), per internal/types/doc.go:
  POST /lease {api_key?, est_seconds?}  (key may also be Authorization: Bearer)
    -> resolve priority via ks; sched.Acquire(KindLease, id=random leaseID,...);
       store ticket in a sync.Map/mutex map by leaseID; if ticket.Granted() is
       already closed (non-blocking select) return 200 {lease_id,status:"granted"}
       else 202 {lease_id,status:"queued",poll_after_ms: cfg.PollAfterMs}.
  GET  /lease/{id} -> {status,position,revoke_requested,ahead}. On each GET call
       ticket.Heartbeat() (polling counts as liveness). status "granted" once
       Granted() is closed. 404 if unknown id.
  POST /lease/{id}/heartbeat -> {revoke_requested} (calls ticket.Heartbeat()).
  POST /lease/{id}/release  -> 200, idempotent (ticket.Release(); drop from map).
- Derive status/position from ticket.Status(). Do not implement reaping here (the
  scheduler owns heartbeat TTL); just forward Heartbeat calls.
Tests: fake Scheduler with a controllable Ticket — POST returns queued then GET
flips to granted; heartbeat returns revoke flag; release is idempotent; unknown
id -> 404.
`,
  },
  {
    name: 'web',
    spec: `
Implement internal/web exposing:
  web.New(sched types.Scheduler, ks types.KeyStore, log *slog.Logger) *web.Handler
  func (*Handler) ServeHTTP(w, r)
  func (*Handler) Routes() http.Handler
Serve a small single-page console plus JSON APIs:
  GET  /                     the console HTML (embed.FS; inline or embedded CSS/JS)
  GET  /api/queue            sched.Snapshot() as JSON
  GET  /api/queue/stream     Server-Sent Events pushing the snapshot ~1/sec (or on change)
  GET  /api/keys             ks.List() (redacted)
  POST /api/keys             {name,priority} -> ks.Mint -> 201 with the secret (shown once)
  DELETE /api/keys/{nameOrKey} -> ks.Revoke -> 204
The console must render a LIVE queue visualization: the current holder and the
ordered pending list, each row showing name, priority, kind (llm/lease), state,
and wait time; plus the warm model/tier; and a small form to mint/revoke keys.
Consume /api/queue/stream (EventSource) for live updates; degrade to polling
/api/queue. Keep the UI clean and legible in light and dark; vanilla JS, no CDN
(strict self-contained). Embed all assets via embed.FS.
Tests: /api/queue returns the snapshot JSON with a fake Scheduler; /api/keys GET
is redacted and POST mints via a fake KeyStore; "/" returns 200 text/html.
`,
  },
]

// Phase 1 — implement every package in parallel (all depend only on internal/types).
const built = await pipeline(packages, p =>
  agent(`${contract}\n\n## YOUR PACKAGE: internal/${p.name}\n${p.spec}`, { label: p.name }),
)

// Phase 2 — integrate: wire cmd/main.go, own dependencies, get the whole module green.
const integration = await agent(
  `You are the INTEGRATION step for the turnstile Go service at ${repo}.
All six internal packages (keys, llamaswap, queue, proxy, lease, web) have just
been implemented in parallel against the frozen internal/types contract. Now make
the whole module build, wire, and test green.

Read internal/types/types.go and internal/types/doc.go for the wiring contract.

Do:
1. Wire cmd/turnstile/main.go: load config.yaml (add the only external dependency,
   gopkg.in/yaml.v3, via "go get" — you OWN go.mod/go.sum) into types.Config;
   derive types.SchedulerConfig; construct keys.New, llamaswap.New, queue.New,
   proxy.New, lease.New, web.New; mount their Routes() on one http.ServeMux at
   the mount points in doc.go (/v1/, /lease, /lease/, /); run an http.Server on
   cfg.Listen with graceful shutdown on SIGINT/SIGTERM that calls
   sched.Shutdown(ctx). Keep the -version and -config flags.
2. Run "cd ${repo} && gofmt -w ./... && go mod tidy && go vet ./... && go build ./... && go test ./... 2>&1".
   Fix all failures. You MAY edit any package to resolve integration mismatches,
   but do NOT change internal/types (it is frozen) — instead fix the package that
   deviated from the contract. Do not weaken or delete tests to make them pass;
   fix the code.
3. Confirm the binary runs: build it, copy config.example.yaml to a temp
   config.yaml, and run "./turnstile -version" and a start-with-bad-config check.
Report: final go build/vet/test results (paste the passing summary), the go.mod
dependency added, and anything you had to fix in a package to integrate.`,
  { label: 'integrate' },
)

return { built, integration }
