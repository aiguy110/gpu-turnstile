package queue

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// historyLog is append-only JSONL persistence for completed GPU holds, backing
// the console timeline across restarts. On open it compacts the file down to
// the records still inside the retention window and returns them so the
// scheduler can seed its in-memory history. Appends are serialized by a mutex;
// they are issued from the scheduler's owner goroutine and are small, so the
// synchronous write does not meaningfully stall scheduling.
type historyLog struct {
	mu sync.Mutex
	f  *os.File
}

// openHistoryLog loads existing records within [now-retain, now], rewrites the
// file to just those (compaction), and returns a log open for appends plus the
// kept records in chronological order.
func openHistoryLog(path string, retain time.Duration, now time.Time) (*historyLog, []types.HoldRecord, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}
	kept := loadRecords(path, now.Add(-retain))
	if err := writeRecords(path, kept); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return &historyLog{f: f}, kept, nil
}

// loadRecords reads whole, completed records whose EndedAt is after cutoff.
// Corrupt or partial lines (e.g. from a crash mid-write) are skipped.
func loadRecords(path string, cutoff time.Time) []types.HoldRecord {
	f, err := os.Open(path)
	if err != nil {
		return nil // missing file == empty history
	}
	defer f.Close()
	var out []types.HoldRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r types.HoldRecord
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		if r.EndedAt.IsZero() || !r.EndedAt.After(cutoff) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// writeRecords atomically replaces path with recs (temp file + rename).
func writeRecords(path string, recs []types.HoldRecord) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range recs {
		if err := enc.Encode(recs[i]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// append writes one record as a JSON line. Errors are swallowed: persistence is
// best-effort and must never wedge the scheduler.
func (h *historyLog) append(r types.HoldRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.f == nil {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	b = append(b, '\n')
	_, _ = h.f.Write(b)
}

func (h *historyLog) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}
