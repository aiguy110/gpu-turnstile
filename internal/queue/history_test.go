package queue

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

func ctxSecond() context.Context { return context.Background() }

func rec(id string, start, end time.Time) types.HoldRecord {
	return types.HoldRecord{ID: id, Name: id, Kind: types.KindLease, StartedAt: start, EndedAt: end}
}

func TestHistoryLogPersistsAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	now := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)

	hl, seed, err := openHistoryLog(path, historyRetention, now)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(seed) != 0 {
		t.Fatalf("fresh log should seed empty, got %d", len(seed))
	}
	hl.append(rec("a", now.Add(-2*time.Hour), now.Add(-time.Hour)))
	hl.append(rec("b", now.Add(-30*time.Minute), now.Add(-20*time.Minute)))
	if err := hl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: both records are within retention and should reload in order.
	hl2, seed2, err := openHistoryLog(path, historyRetention, now)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer hl2.Close()
	if len(seed2) != 2 || seed2[0].ID != "a" || seed2[1].ID != "b" {
		t.Fatalf("want [a b], got %+v", seed2)
	}
}

func TestHistoryLogCompactsOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	now := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)

	hl, _, err := openHistoryLog(path, historyRetention, now)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// One old record (outside the 7d window) and one recent.
	hl.append(rec("old", now.Add(-10*24*time.Hour), now.Add(-9*24*time.Hour)))
	hl.append(rec("new", now.Add(-time.Hour), now.Add(-30*time.Minute)))
	hl.Close()

	// Reopening at `now` should drop the old record and rewrite the file.
	_, seed, err := openHistoryLog(path, historyRetention, now)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(seed) != 1 || seed[0].ID != "new" {
		t.Fatalf("want only [new] after compaction, got %+v", seed)
	}
	// The compacted file itself should now contain just the kept record.
	onDisk := loadRecords(path, time.Time{})
	if len(onDisk) != 1 || onDisk[0].ID != "new" {
		t.Fatalf("compacted file should hold only [new], got %+v", onDisk)
	}
}

func TestSchedulerSeedsAndPersistsHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	cfg := testConfig()
	cfg.HistoryPath = path

	// First scheduler: run one hold to completion, then shut down.
	s1 := New(cfg, &fakeEvictor{}, nil)
	tk := s1.Acquire(lease("h1", 10))
	select {
	case <-tk.Granted():
	case <-time.After(time.Second):
		t.Fatal("h1 not granted")
	}
	tk.Release()
	if err := s1.Shutdown(ctxSecond()); err != nil {
		t.Fatalf("shutdown s1: %v", err)
	}

	// Second scheduler reading the same file should see the persisted hold.
	s2 := New(cfg, &fakeEvictor{}, nil)
	defer s2.Shutdown(ctxSecond())
	hist := s2.History(time.Now().Add(-time.Hour))
	found := false
	for _, r := range hist {
		if r.ID == "h1" && !r.EndedAt.IsZero() {
			found = true
		}
	}
	if !found {
		t.Fatalf("persisted hold h1 not reloaded, got %+v", hist)
	}
}
