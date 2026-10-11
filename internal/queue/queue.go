// Package queue implements the concrete GPU Scheduler and Ticket described by
// the frozen contract in internal/types (see types.go and doc.go).
//
// A single owner goroutine (run) serializes ALL mutable state. Every public
// method funnels a closure onto the cmds channel; the owner goroutine executes
// closures one at a time, so no state is ever touched concurrently. Time is
// injectable (the now function and tickInterval fields) so tests can drive the
// grace/heartbeat/preemption logic deterministically without sleeping.
package queue

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// ticket is the concrete implementation of types.Ticket. All fields other than
// the channels and sched pointer are read/written ONLY inside the owner
// goroutine, so they need no locking.
type ticket struct {
	sched *scheduler

	// immutable identity
	id       string
	name     string
	priority types.Priority
	kind     types.Kind
	model    types.Model
	alias    types.Model
	ctx      int
	est      int

	// owner-goroutine-only mutable state
	seq             uint64
	state           types.TicketState
	enqueuedAt      time.Time
	grantedAt       time.Time
	lastHeartbeat   time.Time
	revokeRequested bool
	preemptDeadline time.Time

	// closed exactly once by the owner goroutine
	granted chan struct{}
	revoke  chan struct{}
}

func (t *ticket) Granted() <-chan struct{}         { return t.granted }
func (t *ticket) RevokeRequested() <-chan struct{} { return t.revoke }

func (t *ticket) Release() {
	t.sched.do(func() {
		t.sched.releaseTicket(t)
		t.sched.reconcile()
	})
}

func (t *ticket) Heartbeat() (revokeRequested bool) {
	if !t.sched.do(func() {
		if t.state != types.StateReleased {
			t.lastHeartbeat = t.sched.now()
		}
		revokeRequested = t.revokeRequested
	}) {
		// scheduler already shut down
		return false
	}
	return revokeRequested
}

func (t *ticket) Status() types.TicketStatus {
	var st types.TicketStatus
	if !t.sched.do(func() { st = t.sched.statusOf(t) }) {
		st = t.baseStatus()
	}
	return st
}

func (t *ticket) baseStatus() types.TicketStatus {
	return types.TicketStatus{
		ID:              t.id,
		Name:            t.name,
		Priority:        t.priority,
		Kind:            t.kind,
		Model:           t.model,
		Alias:           t.alias,
		State:           t.state,
		RevokeRequested: t.revokeRequested,
		EstSeconds:      t.est,
		CtxTokens:       t.ctx,
		EnqueuedAt:      t.enqueuedAt,
		GrantedAt:       t.grantedAt,
	}
}

// scheduler is the concrete types.Scheduler.
type scheduler struct {
	cfg types.SchedulerConfig
	ev  types.Evictor
	log *slog.Logger

	// injectable time (defaults set in newScheduler; overridden in tests)
	now          func() time.Time
	tickInterval time.Duration

	cmds chan func()
	quit chan struct{}
	done chan struct{}
	stop sync.Once

	// owner-goroutine-only state
	// holders is empty (GPU idle), exactly one lease, or one or more LLM
	// requests for the same model (up to that model's slots).
	holders         []*ticket
	pending         []*ticket
	seq             uint64
	warmModel       types.Model
	warmTier        types.Priority
	lastLLMComplete time.Time
	deferredID      string // id of the waiter we last logged a grace-deferral for
	budgetDeferID   string // id of the waiter we last logged a budget/slot deferral for
	history         []types.HoldRecord
	sink            func(types.HoldRecord)
	hlog            *historyLog
}

const historyRetention = 7 * 24 * time.Hour

// New constructs the scheduler and starts its owner goroutine. Signature is
// fixed by internal/types/doc.go.
func New(cfg types.SchedulerConfig, ev types.Evictor, log *slog.Logger) types.Scheduler {
	s := newScheduler(cfg, ev, log)
	if cfg.HistoryPath != "" {
		hlog, seed, err := openHistoryLog(cfg.HistoryPath, historyRetention, s.now())
		if err != nil {
			s.log.Warn("history log disabled", "path", cfg.HistoryPath, "err", err)
		} else {
			s.history = seed
			s.hlog = hlog
			s.sink = hlog.append
		}
	}
	s.start()
	return s
}

// newScheduler builds an unstarted scheduler. Tests use this so they can swap
// the clock (now) and disable the internal ticker (tickInterval=0) before
// calling start.
func newScheduler(cfg types.SchedulerConfig, ev types.Evictor, log *slog.Logger) *scheduler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &scheduler{
		cfg:          cfg,
		ev:           ev,
		log:          log,
		now:          time.Now,
		tickInterval: 250 * time.Millisecond,
		cmds:         make(chan func()),
		quit:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

func (s *scheduler) start() { go s.run() }

func (s *scheduler) run() {
	defer close(s.done)

	var tickC <-chan time.Time
	if s.tickInterval > 0 {
		t := time.NewTicker(s.tickInterval)
		defer t.Stop()
		tickC = t.C
	}

	for {
		select {
		case cmd := <-s.cmds:
			cmd()
		case <-tickC:
			s.reconcile()
		case <-s.quit:
			s.shutdownRelease()
			return
		}
	}
}

// do runs fn inside the owner goroutine and blocks until it completes. Returns
// false if the scheduler has shut down (fn is not run).
func (s *scheduler) do(fn func()) bool {
	done := make(chan struct{})
	select {
	case s.cmds <- func() { fn(); close(done) }:
		<-done
		return true
	case <-s.done:
		return false
	}
}

// poke forces a reconcile inside the owner goroutine. Used by the ticker path
// and by tests after advancing the clock.
func (s *scheduler) poke() { s.do(func() { s.reconcile() }) }

// ---- types.Scheduler ----

func (s *scheduler) Acquire(spec types.AcquireSpec) types.Ticket {
	t := &ticket{
		sched:    s,
		id:       spec.ID,
		name:     spec.Name,
		priority: spec.Priority,
		kind:     spec.Kind,
		model:    spec.Model,
		alias:    spec.Alias,
		ctx:      spec.CtxTokens,
		est:      spec.EstSeconds,
		state:    types.StateReleased, // becomes queued once enqueued below
		granted:  make(chan struct{}),
		revoke:   make(chan struct{}),
	}
	s.do(func() {
		now := s.now()
		s.seq++
		t.seq = s.seq
		t.enqueuedAt = now
		t.lastHeartbeat = now
		t.state = types.StateQueued
		s.pending = append(s.pending, t)
		s.log.Info("enqueued", "id", t.id, "name", t.name, "kind", t.kind,
			"priority", t.priority, "model", t.model, "alias", t.alias, "ctx_tokens", t.ctx)
		s.reconcile()
	})
	return t
}

func (s *scheduler) Snapshot() types.QueueSnapshot {
	var snap types.QueueSnapshot
	if !s.do(func() {
		snap.UpdatedAt = s.now()
		snap.WarmModel = s.warmModel
		snap.WarmTier = s.warmTier
		for _, h := range s.holders {
			snap.Holders = append(snap.Holders, s.statusOf(h))
		}
		if len(snap.Holders) > 0 {
			h := snap.Holders[0]
			snap.Holder = &h
		}
		for _, t := range s.serviceOrder() {
			snap.Pending = append(snap.Pending, s.statusOf(t))
		}
	}) {
		snap.UpdatedAt = s.now()
	}
	return snap
}

func (s *scheduler) Shutdown(ctx context.Context) error {
	s.stop.Do(func() { close(s.quit) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *scheduler) shutdownRelease() {
	for _, h := range s.holders {
		h.state = types.StateReleased
	}
	s.holders = nil
	for _, t := range s.pending {
		t.state = types.StateReleased
	}
	s.pending = nil
	if s.hlog != nil {
		_ = s.hlog.Close()
	}
}

// ---- owner-goroutine logic ----

// reconcile is the heart of the policy. It is safe to call after any event and
// on every tick; it is idempotent for a fixed clock value.
func (s *scheduler) reconcile() {
	now := s.now()
	s.reapDead(now)
	if lh := s.leaseHolder(); lh != nil {
		s.handlePreemption(lh, now)
	}
	s.tryAdmit(now)
	if len(s.holders) == 0 && len(s.pending) == 0 {
		s.maybeIdleFloor(now)
	}
}

func (s *scheduler) heartbeatTTL() time.Duration {
	return time.Duration(s.cfg.Lease.HeartbeatTimeoutSeconds) * time.Second
}

// reapDead releases leases (holder or queued) that have missed their heartbeat
// TTL. LLM tickets are never reaped this way.
func (s *scheduler) reapDead(now time.Time) {
	ttl := s.heartbeatTTL()
	if ttl <= 0 {
		return
	}
	if h := s.leaseHolder(); h != nil && now.Sub(h.lastHeartbeat) >= ttl {
		s.log.Info("reaping dead lease holder", "id", h.id)
		s.releaseTicket(h)
	}
	kept := s.pending[:0]
	for _, t := range s.pending {
		if t.kind == types.KindLease && now.Sub(t.lastHeartbeat) >= ttl {
			s.log.Info("reaping dead queued lease", "id", t.id)
			t.state = types.StateReleased
			continue
		}
		kept = append(kept, t)
	}
	s.pending = kept
}

// leaseHolder returns the lease holder, or nil when the GPU is idle or held by
// LLM requests. A lease always holds the GPU alone.
func (s *scheduler) leaseHolder() *ticket {
	if len(s.holders) == 1 && s.holders[0].kind == types.KindLease {
		return s.holders[0]
	}
	return nil
}

// handlePreemption cooperatively revokes a lease holder when a strictly
// higher-priority waiter is pending. LLM holders are never revoked.
func (s *scheduler) handlePreemption(h *ticket, now time.Time) {
	order := s.serviceOrder()
	if len(order) == 0 {
		return
	}
	top := order[0]
	if top.priority <= h.priority {
		return
	}
	if !h.revokeRequested {
		h.revokeRequested = true
		close(h.revoke)
		h.preemptDeadline = now.Add(time.Duration(s.cfg.Preemption.GraceSeconds) * time.Second)
		s.log.Info("revoke requested", "holder", h.id, "waiter", top.id)
	}
	// After the preemption grace elapses, proceed once the holder's heartbeat
	// TTL has also lapsed (it stopped heartbeating).
	if !now.Before(h.preemptDeadline) {
		ttl := s.heartbeatTTL()
		if ttl > 0 && now.Sub(h.lastHeartbeat) >= ttl {
			s.log.Info("forcing revoke after grace + heartbeat lapse", "holder", h.id)
			s.releaseTicket(h)
		}
	}
}

// tryAdmit grants the front-of-queue waiter when possible, honoring the
// dynamic-TTL eviction policy on an idle GPU and the slot/context budget when
// joining LLM holders. It only ever considers the front waiter: a waiter that
// would fit never skips ahead of one that doesn't.
func (s *scheduler) tryAdmit(now time.Time) {
	for len(s.pending) > 0 {
		front := s.serviceOrder()[0]
		if len(s.holders) > 0 {
			if !s.canJoin(front) {
				return
			}
			s.grant(front, now)
			continue
		}
		if !s.admitIdle(front, now) {
			return
		}
	}
}

// canJoin reports whether front may run alongside the current holders: it is
// an LLM request for the same model, the model has a free slot, and the
// in-flight context estimate plus its own fits the model's pool.
func (s *scheduler) canJoin(front *ticket) bool {
	first := s.holders[0]
	if front.kind != types.KindLLM || first.kind != types.KindLLM || front.model != first.model {
		return false
	}
	mc := s.cfg.Models[front.model]
	if mc.Slots <= 1 {
		return false // exclusive model: the pre-co-admission behaviour
	}
	if len(s.holders) >= mc.Slots {
		s.logBudgetDefer(front, "slots full", mc)
		return false
	}
	if mc.CtxPool > 0 && s.ctxInFlight(mc)+s.ctxOf(front, mc) > mc.CtxPool {
		s.logBudgetDefer(front, "context budget", mc)
		return false
	}
	return true
}

// ctxOf is a ticket's budgeted context; unknown (0) counts as the whole pool.
func (s *scheduler) ctxOf(t *ticket, mc types.ModelCfg) int {
	if t.ctx <= 0 || t.ctx > mc.CtxPool {
		return mc.CtxPool
	}
	return t.ctx
}

func (s *scheduler) ctxInFlight(mc types.ModelCfg) int {
	sum := 0
	for _, h := range s.holders {
		sum += s.ctxOf(h, mc)
	}
	return sum
}

// logBudgetDefer logs once per waiter why it cannot join the current holders.
func (s *scheduler) logBudgetDefer(front *ticket, reason string, mc types.ModelCfg) {
	if s.budgetDeferID == front.id {
		return
	}
	s.budgetDeferID = front.id
	s.log.Info("deferring: "+reason, "waiter", front.id, "waiter_name", front.name,
		"model", front.model, "holders", len(s.holders), "slots", max(mc.Slots, 1),
		"ctx_used", s.ctxInFlight(mc), "ctx_needed", s.ctxOf(front, mc), "ctx_pool", mc.CtxPool)
}

// admitIdle grants front on an idle GPU, unloading the warm model first when
// policy allows. Returns false when front must keep waiting.
func (s *scheduler) admitIdle(front *ticket, now time.Time) bool {
	if s.needsEviction(front) {
		if !s.canEvictNow(front, now) {
			s.logDefer(front, now)
			return false // keep the model warm; a later tick will re-check
		}
		reason := "grace-expired"
		if front.priority >= s.warmTier {
			reason = "equal-or-higher-priority-waiter"
		}
		s.unloadWarm(now, reason)
	}
	s.grant(front, now)
	return true
}

// logDefer emits one line the first time we defer a given waiter, so a request
// that sits waiting has a visible reason (and an ETA) in the log rather than
// silently blocking. Throttled by deferredID to avoid spamming every tick.
func (s *scheduler) logDefer(front *ticket, now time.Time) {
	if s.deferredID == front.id {
		return
	}
	s.deferredID = front.id
	grace := time.Duration(s.cfg.TTL.GraceSeconds) * time.Second
	remaining := grace - now.Sub(s.lastLLMComplete)
	if remaining < 0 {
		remaining = 0
	}
	s.log.Info("deferring grant to keep model warm",
		"waiter", front.id, "waiter_name", front.name, "waiter_priority", front.priority,
		"warm_model", s.warmModel, "warm_tier", s.warmTier,
		"grace_remaining", remaining.Round(time.Second).String())
}

// needsEviction reports whether granting front requires unloading the warm
// model first: true for a lease, or an LLM needing a DIFFERENT model.
func (s *scheduler) needsEviction(front *ticket) bool {
	if s.warmModel == "" {
		return false
	}
	if front.kind == types.KindLLM && front.model == s.warmModel {
		return false
	}
	return true
}

// canEvictNow applies the grace policy. W = warmTier, P = front priority.
// Only a STRICTLY LOWER priority waiter (P < W) has to wait out the grace window
// that protects the warm tier's potential follow-up call. A waiter at the same
// tier (P == W) is that tier itself moving on — if it needs a different model,
// keeping the old one warm serves no one, so it evicts immediately, exactly like
// a higher-priority waiter. (This is what stops pi from ever blocking behind its
// own stale warm cache when it switches models.)
func (s *scheduler) canEvictNow(front *ticket, now time.Time) bool {
	if front.priority >= s.warmTier {
		return true // equal-or-higher tier: grace collapses to zero
	}
	grace := time.Duration(s.cfg.TTL.GraceSeconds) * time.Second
	return now.Sub(s.lastLLMComplete) >= grace
}

func (s *scheduler) maybeIdleFloor(now time.Time) {
	fl := s.cfg.TTL.IdleFloorSeconds
	if fl <= 0 || s.warmModel == "" {
		return
	}
	if now.Sub(s.lastLLMComplete) >= time.Duration(fl)*time.Second {
		s.unloadWarm(now, "idle-floor")
	}
}

func (s *scheduler) unloadWarm(now time.Time, reason string) {
	m := s.warmModel
	if m == "" {
		return
	}
	s.log.Info("unloading warm model", "model", m, "reason", reason)
	if err := s.ev.Unload(context.Background(), m); err != nil {
		s.log.Warn("unload failed", "model", m, "err", err)
	}
	s.warmModel = ""
	s.warmTier = 0
	s.lastLLMComplete = now
}

func (s *scheduler) grant(t *ticket, now time.Time) {
	joining := len(s.holders) > 0
	s.removePending(t)
	t.state = types.StateGranted
	t.grantedAt = now
	t.lastHeartbeat = now
	s.holders = append(s.holders, t)
	if t.kind == types.KindLLM {
		// A new LLM grant of tier >= the current warm tier resets the grace
		// timer; warm tracking then follows this ticket. A joiner never lowers
		// the warm tier below a holder that is still running.
		if t.priority >= s.warmTier {
			s.lastLLMComplete = now
		}
		s.warmModel = t.model
		if !joining || t.priority > s.warmTier {
			s.warmTier = t.priority
		}
	}
	s.deferredID = ""
	s.budgetDeferID = ""
	close(t.granted)
	msg := "granted"
	attrs := []any{"id", t.id, "name", t.name, "kind", t.kind,
		"priority", t.priority, "model", t.model,
		"waited", now.Sub(t.enqueuedAt).Round(time.Millisecond).String()}
	if t.alias != "" {
		attrs = append(attrs, "alias", t.alias)
	}
	if joining {
		mc := s.cfg.Models[t.model]
		msg = "joined"
		attrs = append(attrs, "holders", len(s.holders), "ctx_used", s.ctxInFlight(mc), "ctx_pool", mc.CtxPool)
	}
	s.log.Info(msg, attrs...)
}

// releaseTicket is idempotent and removes the ticket whether queued or granted.
func (s *scheduler) releaseTicket(t *ticket) {
	if t.state == types.StateReleased {
		return
	}
	t.state = types.StateReleased
	for i, h := range s.holders {
		if h != t {
			continue
		}
		now := s.now()
		s.recordHold(t, now)
		if t.kind == types.KindLLM {
			s.lastLLMComplete = now // LLM request completed: restart grace
		}
		s.holders = append(s.holders[:i], s.holders[i+1:]...)
		return
	}
	s.removePending(t)
}

func (s *scheduler) recordHold(t *ticket, end time.Time) {
	if t.grantedAt.IsZero() {
		return
	}
	rec := types.HoldRecord{ID: t.id, Name: t.name, Priority: t.priority, Kind: t.kind, Model: t.model, Alias: t.alias, StartedAt: t.grantedAt, EndedAt: end}
	s.history = append(s.history, rec)
	if s.sink != nil {
		s.sink(rec)
	}
	cutoff := end.Add(-historyRetention)
	drop := 0
	for drop < len(s.history) && s.history[drop].EndedAt.Before(cutoff) {
		drop++
	}
	if drop > 0 {
		s.history = append(s.history[:0], s.history[drop:]...)
	}
}

func (s *scheduler) History(since time.Time) []types.HoldRecord {
	var out []types.HoldRecord
	if !s.do(func() {
		for _, record := range s.history {
			if record.EndedAt.After(since) {
				out = append(out, record)
			}
		}
		for _, h := range s.holders {
			if !h.grantedAt.IsZero() {
				out = append(out, types.HoldRecord{ID: h.id, Name: h.name, Priority: h.priority, Kind: h.kind, Model: h.model, Alias: h.alias, StartedAt: h.grantedAt})
			}
		}
	}) {
		return nil
	}
	return out
}

func (s *scheduler) removePending(t *ticket) {
	for i, p := range s.pending {
		if p == t {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			return
		}
	}
}

// serviceOrder returns pending sorted by priority desc, then FIFO by seq.
func (s *scheduler) serviceOrder() []*ticket {
	order := make([]*ticket, len(s.pending))
	copy(order, s.pending)
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].priority != order[j].priority {
			return order[i].priority > order[j].priority
		}
		return order[i].seq < order[j].seq
	})
	return order
}

func (s *scheduler) statusOf(t *ticket) types.TicketStatus {
	st := t.baseStatus()
	st.Position = s.positionOf(t)
	return st
}

// positionOf: 0 for a holder or (when no holder) the front waiter; larger
// values are further back in service order.
func (s *scheduler) positionOf(t *ticket) int {
	base := 0
	for _, h := range s.holders {
		if h == t {
			return 0
		}
		base = 1
	}
	for i, p := range s.serviceOrder() {
		if p == t {
			return base + i
		}
	}
	return 0
}
