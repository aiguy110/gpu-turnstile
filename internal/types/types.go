// Package types is the frozen contract shared by every turnstile package.
//
// The dependency graph is a hub-and-spoke around this package:
//
//	types  ← (implemented by) queue      [Scheduler, Ticket]
//	types  ← (implemented by) keys       [KeyStore]
//	types  ← (implemented by) llamaswap  [Evictor]
//	types  ← (implemented by) activity   [ActivityStore]
//	types  → (consumed by)    proxy      [Scheduler + KeyStore]
//	types  → (consumed by)    lease      [Scheduler + KeyStore]
//	types  → (consumed by)    web        [Scheduler + KeyStore]
//
// Nothing in this package may import any other turnstile package. See doc.go
// for the HTTP wiring contract (mount points + constructor signatures).
package types

import (
	"context"
	"encoding/json"
	"time"
)

// Priority orders GPU access. Higher value = higher priority.
type Priority int

// Kind distinguishes the two front doors that feed the single queue.
type Kind string

const (
	// KindLLM is a request coming through the OpenAI proxy. Its GPU hold lasts
	// exactly one HTTP request/response and is never forcibly revoked.
	KindLLM Kind = "llm"
	// KindLease is a cooperative non-LLM GPU job acquired via the lease API. It
	// may be asked to yield (RevokeRequested) when a higher-priority waiter
	// arrives.
	KindLease Kind = "lease"
)

// Model is a llama-swap model id (e.g. "qwen3.6-27b"). Set on LLM waiters so the
// scheduler can decide whether admitting a waiter requires evicting the resident
// model. Empty for lease waiters.
type Model string

// TicketState is the lifecycle of a GPU acquisition.
type TicketState string

const (
	StateQueued   TicketState = "queued"
	StateGranted  TicketState = "granted"
	StateReleased TicketState = "released"
)

// AcquireSpec describes a request for GPU access handed to Scheduler.Acquire.
type AcquireSpec struct {
	// ID uniquely identifies this acquisition (a lease id for leases, a request
	// id for LLM calls). Used as the map key for lookups and display.
	ID string
	// Name is the caller identity from the API key (e.g. "pi"), for display.
	Name string
	// Priority is resolved from the caller's API key by the KeyStore.
	Priority Priority
	// Kind is KindLLM or KindLease.
	Kind Kind
	// Model is the requested llama-swap model for KindLLM; "" for KindLease.
	Model Model
	// EstSeconds is an optional caller-supplied duration estimate, for display.
	EstSeconds int
	// Alias is the client-facing virtual model name when the request was routed
	// through a VirtualModelCfg (Model then holds the real backend). Display only.
	Alias Model
	// CtxTokens is the estimated context the request will occupy (prompt +
	// max_tokens), used for the per-model context budget. 0 = unknown, which the
	// scheduler budgets as the whole pool when one is configured.
	CtxTokens int
}

// TicketStatus is a point-in-time snapshot of a Ticket, safe to serialize.
type TicketStatus struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	Priority Priority    `json:"priority"`
	Kind     Kind        `json:"kind"`
	Model    Model       `json:"model,omitempty"`
	Alias    Model       `json:"alias,omitempty"`
	State    TicketState `json:"state"`
	// Position is 0 for the current holder or the next-to-be-granted; larger
	// values are further back in service order (priority desc, then FIFO).
	Position        int       `json:"position"`
	RevokeRequested bool      `json:"revoke_requested"`
	EstSeconds      int       `json:"est_seconds,omitempty"`
	CtxTokens       int       `json:"ctx_tokens,omitempty"`
	EnqueuedAt      time.Time `json:"enqueued_at"`
	GrantedAt       time.Time `json:"granted_at,omitzero"`
}

// Ticket is a handle to a queued or granted GPU acquisition. Returned by
// Scheduler.Acquire. All methods are safe for concurrent use.
type Ticket interface {
	// Granted is closed exactly once, when this ticket is granted the GPU.
	Granted() <-chan struct{}
	// RevokeRequested is closed when the scheduler asks this (lease) holder to
	// cooperatively yield. Never fires for KindLLM tickets.
	RevokeRequested() <-chan struct{}
	// Release relinquishes the GPU and removes the ticket from the queue. It is
	// idempotent and safe to call whether the ticket was granted or still
	// queued (e.g. client gave up).
	Release()
	// Heartbeat marks the ticket alive for dead-client reaping and returns
	// whether a yield has been requested. Callers polling a lease should call
	// this on each poll.
	Heartbeat() (revokeRequested bool)
	// Status returns a serializable snapshot.
	Status() TicketStatus
}

// QueueSnapshot is the whole-queue view for the web console.
type QueueSnapshot struct {
	// Holder is the first current GPU holder, or nil when the GPU is idle. Kept
	// for older clients; Holders lists every holder.
	Holder *TicketStatus `json:"holder"`
	// Holders are all current holders: several LLM requests for one model when
	// it is configured with slots > 1, otherwise at most one.
	Holders []TicketStatus `json:"holders"`
	// Pending is the ordered list of waiters (service order).
	Pending []TicketStatus `json:"pending"`
	// WarmModel is the llama-swap model currently resident (KV cache warm), or
	// "" if none is loaded.
	WarmModel Model `json:"warm_model,omitempty"`
	// WarmTier is the priority of the most recent LLM user (governs dynamic-TTL
	// eviction decisions).
	WarmTier  Priority  `json:"warm_tier"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HoldRecord is one GPU hold for the timeline view. A zero EndedAt means the
// holder is still active.
type HoldRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Priority  Priority  `json:"priority"`
	Kind      Kind      `json:"kind"`
	Model     Model     `json:"model,omitempty"`
	Alias     Model     `json:"alias,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitzero"`
}

// ActivityStatus describes the lifecycle of an observed LLM HTTP request.
type ActivityStatus string

const (
	ActivityQueued    ActivityStatus = "queued"
	ActivityRunning   ActivityStatus = "running"
	ActivityCompleted ActivityStatus = "completed"
	ActivityError     ActivityStatus = "error"
	ActivityCanceled  ActivityStatus = "canceled"
)

// ToolCall is a provider-neutral view of a model tool call. Arguments and
// results deliberately remain raw JSON/text: the console must not know how to
// render individual tools.
type ToolCall struct {
	Index     int    `json:"index,omitempty"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	Result    string `json:"result,omitempty"`
}

// LLMActivity is one proxied OpenAI-compatible request and its observed output.
// It never includes request headers or API keys.
type LLMActivity struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Priority     Priority        `json:"priority"`
	Model        Model           `json:"model,omitempty"`
	Alias        Model           `json:"alias,omitempty"`
	Endpoint     string          `json:"endpoint"`
	Status       ActivityStatus  `json:"status"`
	Prompt       string          `json:"prompt,omitempty"`
	Output       string          `json:"output,omitempty"`
	ToolCalls    []ToolCall      `json:"tool_calls,omitempty"`
	Usage        json.RawMessage `json:"usage,omitempty"`
	Error        string          `json:"error,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
	EnqueuedAt   time.Time       `json:"enqueued_at"`
	GrantedAt    time.Time       `json:"granted_at,omitzero"`
	FirstTokenAt time.Time       `json:"first_token_at,omitzero"`
	CompletedAt  time.Time       `json:"completed_at,omitzero"`
	QueueWaitMs  int64           `json:"queue_wait_ms,omitempty"`
	FirstTokenMs int64           `json:"first_token_ms,omitempty"`
	OutputBytes  int             `json:"output_bytes"`
	OutputTokens int             `json:"output_tokens,omitempty"`
	TokensPerSec float64         `json:"tokens_per_sec,omitempty"`
}

// ActivityDelta is an incremental provider-neutral update extracted from an
// upstream response stream.
type ActivityDelta struct {
	Text     string
	ToolCall *ToolCall
	Usage    json.RawMessage
}

// ActivityStore records live requests, retains history, and fans snapshots out
// to console subscribers. Implementations must not block the proxy stream.
type ActivityStore interface {
	Begin(LLMActivity)
	Granted(id string, at time.Time)
	Append(id string, delta ActivityDelta, at time.Time)
	Complete(id string, at time.Time)
	Fail(id, message string, canceled bool, at time.Time)
	List() []LLMActivity
	Get(id string) (LLMActivity, bool)
	Subscribe() (<-chan LLMActivity, func())
}

// Scheduler owns the single GPU priority queue. Implemented by package queue;
// its concrete constructor is queue.New (see doc.go). A single internal
// goroutine serializes all state, so the interface is safe for concurrent use.
type Scheduler interface {
	// Acquire enqueues a request and returns a Ticket immediately without
	// blocking. Callers wait on Ticket.Granted() (LLM path blocks; lease path
	// polls Ticket.Status()).
	Acquire(spec AcquireSpec) Ticket
	// Snapshot returns the current holder + ordered pending list + warm-model
	// state for display.
	Snapshot() QueueSnapshot
	// History returns completed GPU holds after since plus the active holder as
	// an open record. It backs the Overview timeline.
	History(since time.Time) []HoldRecord
	// Shutdown stops the scheduler goroutine, releasing any holder.
	Shutdown(ctx context.Context) error
}

// Evictor frees the resident llama-swap model's VRAM. Implemented by package
// llamaswap. Unload is synchronous: it returns only after the upstream process
// has exited and VRAM is released (llama-swap's unload endpoint blocks).
type Evictor interface {
	// Unload evicts the named model (or all resident models if model == "").
	Unload(ctx context.Context, model Model) error
	// Running reports the models currently loaded upstream ("" slice if none).
	Running(ctx context.Context) ([]Model, error)
}

// KeyInfo is the identity and priority behind an API key. Key is the secret and
// is redacted (empty) in listings returned by List.
type KeyInfo struct {
	Name     string    `json:"name"`
	Priority Priority  `json:"priority"`
	Key      string    `json:"key,omitempty"`
	Created  time.Time `json:"created,omitzero"`
}

// KeyStore maps API keys to priority tiers and manages key lifecycle.
// Implemented by package keys. Safe for concurrent use.
type KeyStore interface {
	// Lookup resolves a raw API key (the bearer token) to its identity. When
	// found is false the caller is anonymous and should be treated as
	// AnonymousPriority.
	Lookup(rawKey string) (info KeyInfo, found bool)
	// AnonymousPriority is the tier for unknown or missing keys (below every
	// named key by default).
	AnonymousPriority() Priority
	// Mint creates and persists a new key, returning it with the generated
	// secret populated.
	Mint(name string, priority Priority) (KeyInfo, error)
	// List returns all keys with secrets redacted.
	List() []KeyInfo
	// Revoke removes a key identified by its name or its secret.
	Revoke(nameOrKey string) error
}

// ---- Configuration (shapes shared so every package references one struct) ----

// Config is the fully-parsed turnstile configuration (loaded from YAML in cmd).
type Config struct {
	Listen            string        `yaml:"listen"`
	LlamaSwap         LlamaSwapCfg  `yaml:"llamaSwap"`
	TTL               TTLCfg        `yaml:"ttl"`
	Preemption        PreemptionCfg `yaml:"preemption"`
	Lease             LeaseCfg      `yaml:"lease"`
	Activity          ActivityCfg   `yaml:"activity"`
	AnonymousPriority Priority      `yaml:"anonymousPriority"`
	Keys              []KeyInfo     `yaml:"keys"`
	// Models holds per-model concurrency settings, keyed by the real llama-swap
	// model name. A model with no entry gets 1 slot and no context budget.
	Models map[Model]ModelCfg `yaml:"models"`
	// Virtual maps client-facing model names onto a real model in Models.
	Virtual map[Model]VirtualModelCfg `yaml:"virtual"`
	// HistoryLog persists completed GPU holds for the Overview timeline.
	// Empty uses a history.jsonl next to the config file; "none" disables it.
	HistoryLog string `yaml:"historyLog"`
}

// ModelCfg is the concurrency shape of one llama-swap model.
type ModelCfg struct {
	// Slots is the max number of concurrent LLM requests (the server's
	// --parallel). 0 or 1 = exclusive, as before.
	Slots int `yaml:"slots"`
	// CtxPool is the server's total context (-c). When > 0, co-admitted
	// requests' estimated tokens must fit in it. 0 = no budget accounting.
	CtxPool int `yaml:"ctxPool"`
}

// VirtualModelCfg is a client-facing model name that the proxy rewrites to a
// real backend model, with its own context limit and injected body fields.
type VirtualModelCfg struct {
	// Backend is the real model name; it must have an entry in Config.Models.
	Backend Model `yaml:"backend"`
	// MaxCtx rejects requests whose estimated prompt + max_tokens exceeds it
	// (400 context_length_exceeded). 0 = no limit.
	MaxCtx int `yaml:"maxCtx"`
	// DefaultMaxTokens budgets requests that set no max_tokens. 0 = budget the
	// rest of the context.
	DefaultMaxTokens int `yaml:"defaultMaxTokens"`
	// Inject is deep-merged into the JSON body (it wins over client values).
	Inject map[string]any `yaml:"inject"`
}

// RoutingCfg is the subset of Config the proxy needs for virtual models and
// context estimates.
type RoutingCfg struct {
	Models  map[Model]ModelCfg
	Virtual map[Model]VirtualModelCfg
}

// ActivityCfg controls LLM output observation and local history retention.
type ActivityCfg struct {
	HistoryPath    string `yaml:"historyPath"`
	MaxRequests    int    `yaml:"maxRequests"`
	MaxOutputBytes int    `yaml:"maxOutputBytes"`
	RetentionHours int    `yaml:"retentionHours"`
}

// LlamaSwapCfg configures the upstream llama-swap fronted by the proxy/evictor.
type LlamaSwapCfg struct {
	BaseURL    string `yaml:"baseUrl"`
	UnloadPath string `yaml:"unloadPath"` // may contain "{model}"
}

// TTLCfg holds the demand-driven dynamic-TTL knobs. See README/doc.go for the
// exact policy the scheduler must implement.
type TTLCfg struct {
	GraceSeconds     int `yaml:"graceSeconds"`
	IdleFloorSeconds int `yaml:"idleFloorSeconds"`
}

// PreemptionCfg configures cooperative yield of lower-priority lease holders.
type PreemptionCfg struct {
	GraceSeconds int `yaml:"graceSeconds"`
}

// LeaseCfg configures lease heartbeat/reaping and poll pacing.
type LeaseCfg struct {
	HeartbeatTimeoutSeconds int `yaml:"heartbeatTimeoutSeconds"`
	PollAfterMs             int `yaml:"pollAfterMs"`
}

// SchedulerConfig is the subset of Config the scheduler needs. cmd derives it
// from Config when constructing the queue.
type SchedulerConfig struct {
	TTL         TTLCfg
	Preemption  PreemptionCfg
	Lease       LeaseCfg
	HistoryPath string
	// Models gives per-model slots and context pool (see ModelCfg).
	Models map[Model]ModelCfg
}
