// Package activity stores live and recent LLM request activity for the console.
package activity

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// Store is a bounded, concurrency-safe activity history. Completed records are
// persisted as a small JSON snapshot; live deltas stay in memory so token
// streaming never waits on disk.
type Store struct {
	mu             sync.RWMutex
	persistMu      sync.Mutex
	items          map[string]*types.LLMActivity
	order          []string
	lastPublish    map[string]time.Time
	subs           map[uint64]chan types.LLMActivity
	nextSub        uint64
	maxRequests    int
	maxOutputBytes int
	retention      time.Duration
	historyPath    string
	persistCh      chan struct{}
	persistDone    chan struct{}
	closeOnce      sync.Once
	log            *slog.Logger
}

func New(cfg types.ActivityCfg, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = 500
	}
	if cfg.MaxOutputBytes <= 0 {
		cfg.MaxOutputBytes = 1 << 20
	}
	if cfg.RetentionHours <= 0 {
		cfg.RetentionHours = 168
	}
	s := &Store{
		items:          make(map[string]*types.LLMActivity),
		lastPublish:    make(map[string]time.Time),
		subs:           make(map[uint64]chan types.LLMActivity),
		maxRequests:    cfg.MaxRequests,
		maxOutputBytes: cfg.MaxOutputBytes,
		retention:      time.Duration(cfg.RetentionHours) * time.Hour,
		historyPath:    cfg.HistoryPath,
		log:            log,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	if s.historyPath != "" {
		s.persistCh = make(chan struct{}, 1)
		s.persistDone = make(chan struct{})
		go func() {
			defer close(s.persistDone)
			for range s.persistCh {
				if err := s.persist(); err != nil {
					s.log.Warn("persist activity history", "err", err)
				}
			}
		}()
	}
	return s, nil
}

func (s *Store) Begin(a types.LLMActivity) {
	s.mu.Lock()
	if a.EnqueuedAt.IsZero() {
		a.EnqueuedAt = time.Now()
	}
	a.Status = types.ActivityQueued
	const maxInputBytes = 256 << 10
	inputBudget := maxInputBytes
	if len(a.Prompt) > inputBudget {
		a.Prompt = validPrefix(a.Prompt, inputBudget)
		a.Truncated = true
	}
	inputBudget -= len(a.Prompt)
	if len(a.ToolCalls) > 100 {
		a.ToolCalls = a.ToolCalls[:100]
		a.Truncated = true
	}
	for i := range a.ToolCalls {
		for _, value := range []*string{&a.ToolCalls[i].Arguments, &a.ToolCalls[i].Result} {
			if len(*value) > inputBudget {
				*value = validPrefix(*value, inputBudget)
				a.Truncated = true
			}
			inputBudget -= len(*value)
		}
	}
	// Tool results arrive in the next completion request. Attach them to the
	// original observed call when its provider ID is still in history.
	for _, incoming := range a.ToolCalls {
		if incoming.ID == "" || incoming.Result == "" {
			continue
		}
		for _, existing := range s.items {
			for i := range existing.ToolCalls {
				if existing.ToolCalls[i].ID == incoming.ID {
					existing.ToolCalls[i].Result = incoming.Result
					s.publishLocked(clone(*existing))
				}
			}
		}
	}
	cp := clone(a)
	s.items[a.ID] = &cp
	s.order = append([]string{a.ID}, s.order...)
	s.pruneLocked(time.Now())
	s.publishLocked(cp)
	s.mu.Unlock()
}

func (s *Store) Granted(id string, at time.Time) {
	s.update(id, at, true, func(a *types.LLMActivity) {
		a.Status = types.ActivityRunning
		a.GrantedAt = at
		a.QueueWaitMs = at.Sub(a.EnqueuedAt).Milliseconds()
	})
}

func (s *Store) Append(id string, d types.ActivityDelta, at time.Time) {
	s.update(id, at, false, func(a *types.LLMActivity) {
		if a.FirstTokenAt.IsZero() && (d.Text != "" || d.ToolCall != nil) {
			a.FirstTokenAt = at
			if !a.GrantedAt.IsZero() {
				a.FirstTokenMs = at.Sub(a.GrantedAt).Milliseconds()
			}
		}
		if d.Text != "" {
			remaining := s.maxOutputBytes - len(a.Output)
			if remaining <= 0 {
				a.Truncated = true
			} else if len(d.Text) > remaining {
				a.Output += validPrefix(d.Text, remaining)
				a.Truncated = true
			} else {
				a.Output += d.Text
			}
			a.OutputBytes = len(a.Output)
		}
		if d.ToolCall != nil {
			upsertToolCall(a, *d.ToolCall, s.maxOutputBytes)
		}
		if len(d.Usage) > 0 {
			a.Usage = append(a.Usage[:0], d.Usage...)
			a.OutputTokens = outputTokens(d.Usage)
		}
	})
}

func (s *Store) Complete(id string, at time.Time) {
	s.finish(id, "", false, at)
}

func (s *Store) Fail(id, message string, canceled bool, at time.Time) {
	s.finish(id, message, canceled, at)
}

func (s *Store) finish(id, message string, canceled bool, at time.Time) {
	s.mu.Lock()
	a, ok := s.items[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	a.CompletedAt = at
	a.Error = message
	if message == "" {
		a.Status = types.ActivityCompleted
	} else if canceled {
		a.Status = types.ActivityCanceled
	} else {
		a.Status = types.ActivityError
	}
	if a.OutputTokens > 0 && !a.FirstTokenAt.IsZero() {
		seconds := at.Sub(a.FirstTokenAt).Seconds()
		if seconds > 0 {
			a.TokensPerSec = float64(a.OutputTokens) / seconds
		}
	}
	s.pruneLocked(at)
	cp := clone(*a)
	s.publishLocked(cp)
	s.mu.Unlock()
	if s.persistCh != nil {
		select {
		case s.persistCh <- struct{}{}:
		default:
		}
	}
}

func (s *Store) update(id string, at time.Time, forcePublish bool, fn func(*types.LLMActivity)) {
	s.mu.Lock()
	a, ok := s.items[id]
	if ok {
		fn(a)
		last := s.lastPublish[id]
		if forcePublish || last.IsZero() || at.Sub(last) >= 100*time.Millisecond {
			s.lastPublish[id] = at
			s.publishLocked(clone(*a))
		}
	}
	s.mu.Unlock()
}

func (s *Store) List() []types.LLMActivity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]types.LLMActivity, 0, len(s.order))
	for _, id := range s.order {
		if a := s.items[id]; a != nil {
			out = append(out, clone(*a))
		}
	}
	return out
}

func (s *Store) Get(id string) (types.LLMActivity, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.items[id]
	if !ok {
		return types.LLMActivity{}, false
	}
	return clone(*a), true
}

func (s *Store) Subscribe() (<-chan types.LLMActivity, func()) {
	s.mu.Lock()
	s.nextSub++
	id := s.nextSub
	ch := make(chan types.LLMActivity, 8)
	s.subs[id] = ch
	s.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			s.mu.Lock()
			if current, ok := s.subs[id]; ok {
				delete(s.subs, id)
				close(current)
			}
			s.mu.Unlock()
		})
	}
}

// Flush synchronously writes the latest completed snapshot. It is intended for
// graceful process shutdown; live proxy traffic uses the asynchronous worker.
func (s *Store) Flush() error { return s.persist() }

// Close drains the persistence worker and writes one final snapshot.
func (s *Store) Close() error {
	if s.persistCh == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.persistCh)
		<-s.persistDone
	})
	return s.persist()
}

func (s *Store) publishLocked(a types.LLMActivity) {
	for _, ch := range s.subs {
		select {
		case ch <- a:
		default:
			// Keep the newest snapshot when a browser cannot keep up.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- a:
			default:
			}
		}
	}
}

func (s *Store) pruneLocked(now time.Time) {
	cutoff := now.Add(-s.retention)
	kept := s.order[:0]
	for _, id := range s.order {
		a := s.items[id]
		if a == nil {
			continue
		}
		terminal := a.Status == types.ActivityCompleted || a.Status == types.ActivityError || a.Status == types.ActivityCanceled
		if terminal && !a.CompletedAt.IsZero() && a.CompletedAt.Before(cutoff) {
			delete(s.items, id)
			delete(s.lastPublish, id)
			continue
		}
		kept = append(kept, id)
	}
	s.order = kept
	for len(s.order) > s.maxRequests {
		last := len(s.order) - 1
		id := s.order[last]
		if a := s.items[id]; a != nil && (a.Status == types.ActivityQueued || a.Status == types.ActivityRunning) {
			break
		}
		delete(s.items, id)
		delete(s.lastPublish, id)
		s.order = s.order[:last]
	}
}

func (s *Store) load() error {
	if s.historyPath == "" {
		return nil
	}
	raw, err := os.ReadFile(s.historyPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var records []types.LLMActivity
	if err := json.Unmarshal(raw, &records); err != nil {
		return err
	}
	sort.SliceStable(records, func(i, j int) bool { return records[i].EnqueuedAt.After(records[j].EnqueuedAt) })
	for i := range records {
		a := clone(records[i])
		// A process restart makes an in-flight record historical, not live.
		if a.Status == types.ActivityQueued || a.Status == types.ActivityRunning {
			a.Status = types.ActivityError
			a.Error = "turnstile restarted before the response completed"
			a.CompletedAt = time.Now()
		}
		s.items[a.ID] = &a
		s.order = append(s.order, a.ID)
	}
	s.pruneLocked(time.Now())
	return nil
}

func (s *Store) persist() error {
	if s.historyPath == "" {
		return nil
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	records := s.List()
	raw, err := json.Marshal(records)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.historyPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".activity-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.historyPath)
}

func clone(a types.LLMActivity) types.LLMActivity {
	a.ToolCalls = append([]types.ToolCall(nil), a.ToolCalls...)
	a.Usage = append(json.RawMessage(nil), a.Usage...)
	return a
}

func validPrefix(value string, n int) string {
	if n >= len(value) {
		return value
	}
	for n > 0 && !utf8.RuneStart(value[n]) {
		n--
	}
	return value[:n]
}

func upsertToolCall(a *types.LLMActivity, delta types.ToolCall, maxBytes int) {
	idx := -1
	for i := range a.ToolCalls {
		if delta.ID != "" && a.ToolCalls[i].ID == delta.ID || delta.ID == "" && a.ToolCalls[i].Index == delta.Index {
			idx = i
			break
		}
	}
	if idx < 0 {
		if len(a.ToolCalls) >= 100 {
			a.Truncated = true
			return
		}
		a.ToolCalls = append(a.ToolCalls, types.ToolCall{Index: delta.Index, ID: delta.ID})
		idx = len(a.ToolCalls) - 1
	}
	t := &a.ToolCalls[idx]
	if delta.ID != "" {
		t.ID = delta.ID
	}
	if delta.Name != "" {
		t.Name += delta.Name
	}
	if remaining := maxBytes - len(t.Arguments); remaining > 0 {
		t.Arguments += validPrefix(delta.Arguments, min(remaining, len(delta.Arguments)))
	} else if delta.Arguments != "" {
		a.Truncated = true
	}
	if delta.Result != "" {
		t.Result = delta.Result
	}
}

func outputTokens(raw json.RawMessage) int {
	var usage struct {
		OutputTokens     int `json:"output_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	}
	if json.Unmarshal(raw, &usage) != nil {
		return 0
	}
	if usage.OutputTokens > 0 {
		return usage.OutputTokens
	}
	return usage.CompletionTokens
}
