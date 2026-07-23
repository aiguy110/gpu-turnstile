// Package types is the frozen contract shared by every turnstile package.
//
// The dependency graph is a hub-and-spoke around this package:
//
//	types  ← (implemented by) queue      [Scheduler, Ticket]
//	types  ← (implemented by) keys       [KeyStore]
//	types  ← (implemented by) llamaswap  [Evictor]
//	types  → (consumed by)    proxy      [Scheduler + KeyStore]
//	types  → (consumed by)    lease      [Scheduler + KeyStore]
//	types  → (consumed by)    web        [Scheduler + KeyStore]
//
// Nothing in this package may import any other turnstile package. See doc.go
// for the HTTP wiring contract (mount points + constructor signatures).
package types

import (
	"context"
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
}

// TicketStatus is a point-in-time snapshot of a Ticket, safe to serialize.
type TicketStatus struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	Priority Priority    `json:"priority"`
	Kind     Kind        `json:"kind"`
	Model    Model       `json:"model,omitempty"`
	State    TicketState `json:"state"`
	// Position is 0 for the current holder or the next-to-be-granted; larger
	// values are further back in service order (priority desc, then FIFO).
	Position        int       `json:"position"`
	RevokeRequested bool      `json:"revoke_requested"`
	EstSeconds      int       `json:"est_seconds,omitempty"`
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
	// Holder is the current GPU holder, or nil when the GPU is idle.
	Holder *TicketStatus `json:"holder"`
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

// HoldRecord is one GPU hold (a granted acquisition) for the timeline view.
// A record with a zero EndedAt is still in progress (the current holder).
type HoldRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Priority  Priority  `json:"priority"`
	Kind      Kind      `json:"kind"`
	Model     Model     `json:"model,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitzero"`
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
	// History returns completed GPU holds that ended at or after `since`,
	// oldest first, plus the current holder as an open record (zero EndedAt).
	// Used by the console timeline.
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
	AnonymousPriority Priority      `yaml:"anonymousPriority"`
	Keys              []KeyInfo     `yaml:"keys"`
	// HistoryLog is the path to the append-only JSONL file the scheduler
	// persists completed GPU holds to (for the console timeline). Defaults to
	// "history.jsonl" next to the config file; set "" to disable persistence.
	HistoryLog string `yaml:"historyLog"`
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
	TTL        TTLCfg
	Preemption PreemptionCfg
	Lease      LeaseCfg
	// HistoryPath, if non-empty, is the JSONL file completed holds are
	// persisted to and reloaded from on startup.
	HistoryPath string
}
