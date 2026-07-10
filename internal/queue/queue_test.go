package queue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// ---- test helpers ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type fakeEvictor struct {
	mu       sync.Mutex
	unloaded []types.Model
}

func (e *fakeEvictor) Unload(ctx context.Context, m types.Model) error {
	e.mu.Lock()
	e.unloaded = append(e.unloaded, m)
	e.mu.Unlock()
	return nil
}

func (e *fakeEvictor) Running(ctx context.Context) ([]types.Model, error) { return nil, nil }

func (e *fakeEvictor) calls() []types.Model {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]types.Model, len(e.unloaded))
	copy(out, e.unloaded)
	return out
}

func testConfig() types.SchedulerConfig {
	return types.SchedulerConfig{
		TTL:        types.TTLCfg{GraceSeconds: 300, IdleFloorSeconds: 0},
		Preemption: types.PreemptionCfg{GraceSeconds: 30},
		Lease:      types.LeaseCfg{HeartbeatTimeoutSeconds: 30, PollAfterMs: 1000},
	}
}

// newTestScheduler builds a scheduler driven by a fake clock with the internal
// ticker disabled, so tests advance time and poke() explicitly.
func newTestScheduler(t *testing.T, cfg types.SchedulerConfig) (*scheduler, *fakeClock, *fakeEvictor) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ev := &fakeEvictor{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := newScheduler(cfg, ev, log)
	s.now = clk.Now
	s.tickInterval = 0
	s.start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s, clk, ev
}

func isGranted(tk types.Ticket) bool {
	select {
	case <-tk.Granted():
		return true
	default:
		return false
	}
}

func isRevoked(tk types.Ticket) bool {
	select {
	case <-tk.RevokeRequested():
		return true
	default:
		return false
	}
}

func lease(id string, prio types.Priority) types.AcquireSpec {
	return types.AcquireSpec{ID: id, Name: id, Priority: prio, Kind: types.KindLease}
}

func llm(id string, prio types.Priority, model types.Model) types.AcquireSpec {
	return types.AcquireSpec{ID: id, Name: id, Priority: prio, Kind: types.KindLLM, Model: model}
}

// ---- tests ----

func TestPriorityOrdering(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	blocker := s.Acquire(lease("blk", 100)) // lease holder sets no warm model
	if !isGranted(blocker) {
		t.Fatal("blocker should be granted immediately")
	}
	a := s.Acquire(lease("a", 10))
	b := s.Acquire(lease("b", 20))
	if isGranted(a) || isGranted(b) {
		t.Fatal("waiters must not be granted while holder present")
	}
	blocker.Release()
	if !isGranted(b) {
		t.Fatal("higher-priority b should be granted first")
	}
	if isGranted(a) {
		t.Fatal("a should still wait")
	}
	b.Release()
	if !isGranted(a) {
		t.Fatal("a should be granted after b releases")
	}
}

func TestFIFOWithinPriority(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	blocker := s.Acquire(lease("blk", 100))
	_ = blocker
	a := s.Acquire(lease("a", 10))
	b := s.Acquire(lease("b", 10))
	c := s.Acquire(lease("c", 10))
	blocker.Release()
	if !isGranted(a) || isGranted(b) || isGranted(c) {
		t.Fatal("FIFO: a (first enqueued) must win")
	}
	a.Release()
	if !isGranted(b) || isGranted(c) {
		t.Fatal("FIFO: b must be next")
	}
	b.Release()
	if !isGranted(c) {
		t.Fatal("FIFO: c must be last")
	}
}

func TestNoStartWhileHigherPending(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	blocker := s.Acquire(lease("blk", 100))
	low := s.Acquire(lease("low", 5))
	high := s.Acquire(lease("high", 50))
	blocker.Release()
	if isGranted(low) {
		t.Fatal("low must not start while higher-priority waiter pending")
	}
	if !isGranted(high) {
		t.Fatal("high should be granted")
	}
}

func TestGrantReleaseNext(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	a := s.Acquire(lease("a", 10))
	if !isGranted(a) {
		t.Fatal("a should be granted on empty queue")
	}
	b := s.Acquire(lease("b", 10))
	if isGranted(b) {
		t.Fatal("b must wait")
	}
	a.Release()
	if !isGranted(b) {
		t.Fatal("b should be granted after a releases")
	}
}

func TestImmediateEvictionForHigherPriority(t *testing.T) {
	s, _, ev := newTestScheduler(t, testConfig())
	// Warm a model at tier 10.
	m := s.Acquire(llm("m", 10, "m1"))
	if !isGranted(m) {
		t.Fatal("llm should be granted")
	}
	m.Release()
	// Higher-priority lease waiter -> immediate eviction.
	l := s.Acquire(lease("l", 20))
	if !isGranted(l) {
		t.Fatal("higher-priority lease should be granted immediately via eviction")
	}
	calls := ev.calls()
	if len(calls) != 1 || calls[0] != "m1" {
		t.Fatalf("expected one Unload(m1), got %v", calls)
	}
	if snap := s.Snapshot(); snap.WarmModel != "" {
		t.Fatalf("warm model should be cleared after unload, got %q", snap.WarmModel)
	}
}

func TestGraceWindowHoldsLowerTierWaiter(t *testing.T) {
	s, clk, ev := newTestScheduler(t, testConfig())
	m := s.Acquire(llm("m", 10, "m1"))
	m.Release() // warm m1 @ tier 10, lastLLMComplete = now
	l := s.Acquire(lease("l", 5))
	// Before grace: held warm, lease waits. The lease client keeps
	// heartbeating while it polls so it is not reaped.
	clk.Advance(299 * time.Second)
	l.Heartbeat()
	s.poke()
	if isGranted(l) {
		t.Fatal("lease must wait during grace window")
	}
	if len(ev.calls()) != 0 {
		t.Fatal("no unload should happen during grace")
	}
	// After grace: unload + grant.
	clk.Advance(2 * time.Second)
	l.Heartbeat()
	s.poke()
	if !isGranted(l) {
		t.Fatal("lease should be granted after grace expires")
	}
	if calls := ev.calls(); len(calls) != 1 || calls[0] != "m1" {
		t.Fatalf("expected Unload(m1) after grace, got %v", calls)
	}
}

func TestGraceResetOnNewLLMGrant(t *testing.T) {
	s, clk, ev := newTestScheduler(t, testConfig())
	m := s.Acquire(llm("m", 10, "m1"))
	m.Release() // grace anchor @ t0
	l := s.Acquire(lease("l", 5))

	clk.Advance(290 * time.Second)
	l.Heartbeat()
	s.poke()
	if isGranted(l) || len(ev.calls()) != 0 {
		t.Fatal("still within grace")
	}
	// New LLM (tier >= warm tier, same model) is higher priority than the
	// lease so it is admitted, resetting the grace anchor.
	m2 := s.Acquire(llm("m2", 10, "m1"))
	if !isGranted(m2) {
		t.Fatal("new llm should be granted (reuses warm model)")
	}
	if len(ev.calls()) != 0 {
		t.Fatal("no eviction: same model reused")
	}
	m2.Release() // grace anchor reset to this time (t0+290s)

	// t0+301s: past original grace, but only 11s since reset -> still held.
	clk.Advance(11 * time.Second)
	l.Heartbeat()
	s.poke()
	if isGranted(l) {
		t.Fatal("grace timer should have reset on new LLM grant")
	}
	if len(ev.calls()) != 0 {
		t.Fatal("no unload yet after reset")
	}
	// Advance a full grace past the reset.
	clk.Advance(300 * time.Second)
	l.Heartbeat()
	s.poke()
	if !isGranted(l) {
		t.Fatal("lease should be granted a full grace after the reset")
	}
	if calls := ev.calls(); len(calls) != 1 {
		t.Fatalf("expected exactly one unload, got %v", calls)
	}
}

func TestNoEvictionWhenSameModelReused(t *testing.T) {
	s, _, ev := newTestScheduler(t, testConfig())
	m := s.Acquire(llm("m", 10, "m1"))
	m.Release() // warm m1
	// Lower-priority LLM waiter needing the SAME model: no eviction, grant now.
	n := s.Acquire(llm("n", 5, "m1"))
	if !isGranted(n) {
		t.Fatal("same-model LLM should be granted without eviction")
	}
	if len(ev.calls()) != 0 {
		t.Fatalf("expected no unload, got %v", ev.calls())
	}
	if snap := s.Snapshot(); snap.WarmModel != "m1" {
		t.Fatalf("warm model should remain m1, got %q", snap.WarmModel)
	}
}

func TestCooperativeRevokeSignaling(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("holder", 10))
	if !isGranted(holder) {
		t.Fatal("holder should be granted")
	}
	waiter := s.Acquire(lease("waiter", 20))
	// Higher-priority waiter -> revoke requested, but holder not yet released.
	if !isRevoked(holder) {
		t.Fatal("holder should have revoke requested")
	}
	if !holder.Status().RevokeRequested {
		t.Fatal("holder status flag should be set")
	}
	if isGranted(waiter) {
		t.Fatal("waiter must wait for holder to yield within grace")
	}
	// Holder cooperatively releases.
	holder.Release()
	if !isGranted(waiter) {
		t.Fatal("waiter should be granted after holder releases")
	}
}

func TestPreemptionForceAfterGraceAndHeartbeatLapse(t *testing.T) {
	s, clk, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("holder", 10))
	waiter := s.Acquire(lease("waiter", 20))
	if !isRevoked(holder) {
		t.Fatal("revoke should be requested")
	}
	// Advance past preemption grace (30s) AND heartbeat TTL (30s) with no
	// heartbeat from the stubborn holder. The waiter is alive and heartbeats.
	clk.Advance(31 * time.Second)
	waiter.Heartbeat()
	s.poke()
	if !isGranted(waiter) {
		t.Fatal("waiter should be granted once grace and heartbeat TTL lapse")
	}
	if holder.Status().State != types.StateReleased {
		t.Fatal("stubborn holder should have been force-released")
	}
}

func TestNeverRevokeLLMHolder(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(llm("h", 10, "m1"))
	if !isGranted(holder) {
		t.Fatal("llm holder granted")
	}
	waiter := s.Acquire(lease("w", 50))
	if isRevoked(holder) {
		t.Fatal("LLM holders must never be revoked")
	}
	if isGranted(waiter) {
		t.Fatal("waiter must wait for the LLM to finish on its own")
	}
}

func TestHeartbeatReapingDeadLease(t *testing.T) {
	s, clk, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("dead", 10))
	waiter := s.Acquire(lease("next", 10))
	if !isGranted(holder) {
		t.Fatal("holder granted")
	}
	// Holder sends no heartbeat; the live waiter does. Advance past TTL.
	clk.Advance(31 * time.Second)
	waiter.Heartbeat()
	s.poke()
	if holder.Status().State != types.StateReleased {
		t.Fatal("dead lease should be reaped")
	}
	if !isGranted(waiter) {
		t.Fatal("next waiter should be granted after reaping")
	}
}

func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	s, clk, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("live", 10))
	clk.Advance(20 * time.Second)
	holder.Heartbeat() // refresh liveness
	clk.Advance(20 * time.Second)
	s.poke()
	if holder.Status().State != types.StateGranted {
		t.Fatal("heartbeating lease must not be reaped")
	}
}

func TestQueuedDeadLeaseReaped(t *testing.T) {
	s, clk, _ := newTestScheduler(t, testConfig())
	// LLM holder is never reaped, so it keeps the queued lease waiting.
	holder := s.Acquire(llm("h", 100, "m1"))
	if !isGranted(holder) {
		t.Fatal("llm holder granted")
	}
	q := s.Acquire(lease("q", 10))
	clk.Advance(31 * time.Second)
	s.poke()
	if q.Status().State != types.StateReleased {
		t.Fatal("queued dead lease should be reaped")
	}
	if p := q.Status().Position; p != 0 {
		t.Logf("released ticket position=%d", p)
	}
}

func TestPositionCorrectness(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("holder", 100))
	a := s.Acquire(lease("a", 10)) // seq lower than c
	b := s.Acquire(lease("b", 20))
	c := s.Acquire(lease("c", 10))

	if got := holder.Status().Position; got != 0 {
		t.Fatalf("holder position = %d, want 0", got)
	}
	// Service order among pending: b(20), a(10,seq<c), c(10). With a holder
	// present, positions are offset by 1.
	if got := b.Status().Position; got != 1 {
		t.Fatalf("b position = %d, want 1", got)
	}
	if got := a.Status().Position; got != 2 {
		t.Fatalf("a position = %d, want 2", got)
	}
	if got := c.Status().Position; got != 3 {
		t.Fatalf("c position = %d, want 3", got)
	}
	// Without a holder, front waiter is position 0.
	holder.Release()
	if got := b.Status().Position; got != 0 {
		t.Fatalf("front waiter position = %d, want 0", got)
	}
}

func TestSnapshotShape(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	// Warm a model, then hold it as the LLM holder is released and a lease is
	// admitted... instead keep it simple: an LLM holder + pending lease.
	holder := s.Acquire(llm("h", 40, "qwen"))
	if !isGranted(holder) {
		t.Fatal("holder granted")
	}
	w1 := s.Acquire(lease("w1", 30))
	w2 := s.Acquire(lease("w2", 20))
	_ = w1
	_ = w2

	snap := s.Snapshot()
	if snap.Holder == nil || snap.Holder.ID != "h" {
		t.Fatalf("snapshot holder wrong: %+v", snap.Holder)
	}
	if snap.Holder.Kind != types.KindLLM {
		t.Fatal("holder kind should be llm")
	}
	if len(snap.Pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(snap.Pending))
	}
	if snap.Pending[0].ID != "w1" || snap.Pending[1].ID != "w2" {
		t.Fatalf("pending order wrong: %s, %s", snap.Pending[0].ID, snap.Pending[1].ID)
	}
	if snap.WarmModel != "qwen" || snap.WarmTier != 40 {
		t.Fatalf("warm tracking wrong: model=%q tier=%d", snap.WarmModel, snap.WarmTier)
	}
	if snap.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt should be set")
	}
}

func TestWarmModelClearedAfterUnload(t *testing.T) {
	s, _, ev := newTestScheduler(t, testConfig())
	m := s.Acquire(llm("m", 10, "m1"))
	m.Release()
	if s.Snapshot().WarmModel != "m1" {
		t.Fatal("warm model should be m1 after llm completes")
	}
	// Higher-priority lease forces immediate unload.
	l := s.Acquire(lease("l", 20))
	if !isGranted(l) {
		t.Fatal("lease granted")
	}
	if len(ev.calls()) != 1 {
		t.Fatal("expected one unload")
	}
	if s.Snapshot().WarmModel != "" || s.Snapshot().WarmTier != 0 {
		t.Fatal("warm model/tier should be cleared")
	}
}

func TestIdleFloorUnload(t *testing.T) {
	cfg := testConfig()
	cfg.TTL.IdleFloorSeconds = 600
	s, clk, ev := newTestScheduler(t, cfg)
	m := s.Acquire(llm("m", 10, "m1"))
	m.Release() // warm, nothing pending
	clk.Advance(599 * time.Second)
	s.poke()
	if len(ev.calls()) != 0 {
		t.Fatal("idle floor not yet reached")
	}
	clk.Advance(2 * time.Second)
	s.poke()
	if len(ev.calls()) != 1 {
		t.Fatalf("idle floor unload expected, got %v", ev.calls())
	}
	if s.Snapshot().WarmModel != "" {
		t.Fatal("warm model should be cleared by idle floor")
	}
}

func TestReleaseIdempotentAndRemovesQueued(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("holder", 100))
	q := s.Acquire(lease("q", 10))
	q.Release() // queued release
	q.Release() // idempotent
	if s.Snapshot().Holder == nil {
		t.Fatal("holder still present")
	}
	if len(s.Snapshot().Pending) != 0 {
		t.Fatal("queued ticket should be removed")
	}
	holder.Release()
	holder.Release() // idempotent on granted
	if s.Snapshot().Holder != nil {
		t.Fatal("holder should be gone")
	}
}

func TestShutdownReleasesHolder(t *testing.T) {
	s, _, _ := newTestScheduler(t, testConfig())
	holder := s.Acquire(lease("h", 10))
	if !isGranted(holder) {
		t.Fatal("granted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	// Post-shutdown calls must not block or panic.
	holder.Release()
	if holder.Status().State != types.StateReleased {
		t.Fatal("holder should read released after shutdown")
	}
	if hb := holder.Heartbeat(); hb {
		t.Fatal("heartbeat after shutdown should return false")
	}
}
