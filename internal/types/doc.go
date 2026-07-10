package types

// Wiring contract
// ===============
//
// cmd/turnstile/main.go owns a single http.ServeMux and constructs each package,
// then mounts their handlers. To keep independently-authored packages
// integrable, every package MUST match these constructor signatures and mount
// points exactly.
//
// Constructors
// ------------
//
//	keys.New(seed []types.KeyInfo, anon types.Priority) (types.KeyStore, error)
//	llamaswap.New(cfg types.LlamaSwapCfg, log *slog.Logger) types.Evictor
//	queue.New(cfg types.SchedulerConfig, ev types.Evictor, log *slog.Logger) types.Scheduler
//	proxy.New(sched types.Scheduler, ks types.KeyStore, cfg types.LlamaSwapCfg, log *slog.Logger) *proxy.Handler
//	lease.New(sched types.Scheduler, ks types.KeyStore, cfg types.LeaseCfg, log *slog.Logger) *lease.Handler
//	web.New(sched types.Scheduler, ks types.KeyStore, log *slog.Logger) *web.Handler
//
// Each of *proxy.Handler, *lease.Handler, *web.Handler implements http.Handler
// (has ServeHTTP) AND additionally exposes:
//
//	Routes() http.Handler   // a handler pre-routed for that package's paths
//
// so main can mount them without knowing internal paths.
//
// Mount points (main wires these onto the root mux)
// -------------------------------------------------
//
//	proxy:  "/v1/"                       OpenAI-compatible endpoints (chat/completions, models, ...)
//	lease:  "/lease" and "/lease/"       the cooperative lease API (see below)
//	web:    "/"                          console UI, plus "/api/keys", "/api/queue", "/api/queue/stream"
//
// The lease API surface (all JSON)
// --------------------------------
//
//	POST /lease                       body {api_key?, est_seconds?} (key may also come via Authorization: Bearer)
//	                                  -> 200 {lease_id, status:"granted"} | 202 {lease_id, status:"queued", poll_after_ms}
//	GET  /lease/{id}                  -> {status, position, revoke_requested, ahead:[...]}   (poll to wait)
//	POST /lease/{id}/heartbeat        -> {revoke_requested}                                  (keepalive + yield signal)
//	POST /lease/{id}/release          -> 200 (idempotent)
//
// Auth
// ----
// Both proxy and lease resolve the caller's priority from the API key via
// KeyStore.Lookup(bearerToken). On miss, use KeyStore.AnonymousPriority().
// Nothing hard-fails on auth. The proxy reads the model from the request body's
// "model" field to set AcquireSpec.Model.
//
// Scheduler policy that package queue MUST implement
// ==================================================
//
// A single goroutine owns all state. Service order is priority descending, then
// FIFO by enqueue sequence within a priority. Admission rule: grant the GPU to
// the front-of-queue waiter only when there is no current holder AND no strictly
// higher-priority waiter is pending.
//
// Holds & release:
//   - KindLLM: granted, the proxy reverse-proxies one request, then Release().
//     Never forcibly revoked.
//   - KindLease: granted until the client calls release, OR its heartbeat TTL
//     (Lease.HeartbeatTimeoutSeconds) lapses with no heartbeat (reaped as dead).
//
// Warm-model tracking:
//   - WarmModel = the llama-swap model most recently served; WarmTier = the
//     priority of that LLM user. Cleared to "" when the model is unloaded.
//
// Dynamic-TTL eviction (deciding when to unload the warm model to hand the GPU
// to the next waiter). "Eviction" is needed when the next admitted waiter is a
// lease OR an LLM needing a different model. Let W = WarmTier and P = priority of
// the pending waiter:
//   - No pending waiter                -> never unload; model stays warm.
//   - Pending waiter with P <= W       -> keep warm until TTL.GraceSeconds have
//                                         elapsed since the last LLM request
//                                         completed; any new LLM request from a
//                                         tier >= W resets that timer; on expiry,
//                                         Evictor.Unload(warmModel) then grant.
//   - Pending waiter with P >  W       -> grace collapses to 0: Evictor.Unload
//                                         immediately, then grant.
//   - Optional: TTL.IdleFloorSeconds>0 -> unload after that much total silence
//                                         even with nothing pending.
//
// Cooperative preemption:
//   - When a waiter with priority strictly greater than the current lease
//     holder's priority is admitted, the scheduler closes the holder's
//     RevokeRequested channel and sets RevokeRequested=true in its status. It
//     waits up to Preemption.GraceSeconds for the holder to Release; if it does
//     not, the scheduler proceeds once the holder's heartbeat TTL lapses.
//   - LLM holders are never revoked this way.
//
// Logging: every constructor takes a *slog.Logger (log/slog). Packages must not
// call log.Fatal or os.Exit; return errors instead.
