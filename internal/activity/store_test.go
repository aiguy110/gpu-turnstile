package activity

import (
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

func TestStoreLifecycleToolCallsAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	s, err := New(types.ActivityCfg{HistoryPath: path, MaxRequests: 10, MaxOutputBytes: 1000, RetentionHours: 168}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// Recent, so the reload below does not prune it by retention.
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	s.Begin(types.LLMActivity{ID: "r1", Name: "pi", Model: "qwen", EnqueuedAt: base})
	s.Granted("r1", base.Add(time.Second))
	s.Append("r1", types.ActivityDelta{Text: "hello ", ToolCall: &types.ToolCall{ID: "call-1", Name: "shell", Arguments: `{"cmd":"`}}, base.Add(2*time.Second))
	s.Append("r1", types.ActivityDelta{Text: "world", ToolCall: &types.ToolCall{ID: "call-1", Arguments: `pwd"}`}, Usage: json.RawMessage(`{"completion_tokens":10}`)}, base.Add(3*time.Second))
	s.Complete("r1", base.Add(4*time.Second))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	got, ok := s.Get("r1")
	if !ok {
		t.Fatal("record not found")
	}
	if got.Status != types.ActivityCompleted || got.Output != "hello world" {
		t.Fatalf("record = %+v", got)
	}
	if got.QueueWaitMs != 1000 || got.FirstTokenMs != 1000 || got.OutputTokens != 10 || got.TokensPerSec != 5 {
		t.Fatalf("metrics = queue:%d first:%d tokens:%d rate:%v", got.QueueWaitMs, got.FirstTokenMs, got.OutputTokens, got.TokensPerSec)
	}
	if len(got.ToolCalls) != 1 || got.ToolCalls[0].Name != "shell" || got.ToolCalls[0].Arguments != `{"cmd":"pwd"}` {
		t.Fatalf("tool calls = %+v", got.ToolCalls)
	}

	reloaded, err := New(types.ActivityCfg{HistoryPath: path, MaxRequests: 10, MaxOutputBytes: 1000, RetentionHours: 168}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	if persisted, ok := reloaded.Get("r1"); !ok || persisted.Output != "hello world" {
		t.Fatalf("persisted record = %+v, %v", persisted, ok)
	}
}

func TestStoreTruncatesOutput(t *testing.T) {
	s, err := New(types.ActivityCfg{MaxOutputBytes: 5}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Begin(types.LLMActivity{ID: "r", EnqueuedAt: time.Now()})
	s.Append("r", types.ActivityDelta{Text: "123456789"}, time.Now())
	got, _ := s.Get("r")
	if got.Output != "12345" || !got.Truncated {
		t.Fatalf("got output %q truncated=%v", got.Output, got.Truncated)
	}
}
