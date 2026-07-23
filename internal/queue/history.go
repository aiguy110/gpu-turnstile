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

// historyLog is append-only JSONL persistence for completed GPU holds.
type historyLog struct {
	mu sync.Mutex
	f  *os.File
}

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

func loadRecords(path string, cutoff time.Time) []types.HoldRecord {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []types.HoldRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record types.HoldRecord
		if json.Unmarshal(scanner.Bytes(), &record) == nil && !record.EndedAt.IsZero() && record.EndedAt.After(cutoff) {
			out = append(out, record)
		}
	}
	return out
}

func writeRecords(path string, records []types.HoldRecord) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range records {
		if err := enc.Encode(records[i]); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (h *historyLog) append(record types.HoldRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.f == nil {
		return
	}
	if raw, err := json.Marshal(record); err == nil {
		_, _ = h.f.Write(append(raw, '\n'))
	}
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
