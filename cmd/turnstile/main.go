// Command turnstile is a single-GPU arbitration service: one priority queue
// fronting llama-swap as an OpenAI-compatible proxy, plus a cooperative lease
// API for non-LLM GPU jobs. See internal/types/doc.go for the wiring contract.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/activity"
	"github.com/aiguy110/gpu-turnstile/internal/keys"
	"github.com/aiguy110/gpu-turnstile/internal/lease"
	"github.com/aiguy110/gpu-turnstile/internal/llamaswap"
	"github.com/aiguy110/gpu-turnstile/internal/proxy"
	"github.com/aiguy110/gpu-turnstile/internal/queue"
	"github.com/aiguy110/gpu-turnstile/internal/types"
	"github.com/aiguy110/gpu-turnstile/internal/web"

	"gopkg.in/yaml.v3"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("turnstile %s\n", version)
		return
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(*configPath, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath string, log *slog.Logger) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("config %q: %w", configPath, err)
	}

	// Construct the components (hub-and-spoke around internal/types).
	ks, err := keys.New(cfg.Keys, cfg.AnonymousPriority)
	if err != nil {
		return fmt.Errorf("keys: %w", err)
	}
	evictor := llamaswap.New(cfg.LlamaSwap, log)
	sched := queue.New(types.SchedulerConfig{
		TTL:         cfg.TTL,
		Preemption:  cfg.Preemption,
		Lease:       cfg.Lease,
		HistoryPath: cfg.HistoryLog,
	}, evictor, log)
	activityStore, err := activity.New(cfg.Activity, log)
	if err != nil {
		return fmt.Errorf("activity history: %w", err)
	}

	proxyH := proxy.NewObserved(sched, ks, cfg.LlamaSwap, activityStore, log)
	leaseH := lease.New(sched, ks, cfg.Lease, log)
	webH := web.NewObserved(sched, ks, activityStore, log)

	// Mount on one root mux. More-specific patterns win, so /v1/ and /lease
	// take precedence over the web console's catch-all "/".
	mux := http.NewServeMux()
	mux.Handle("/v1/", proxyH)
	mux.Handle("/lease", leaseH)
	mux.Handle("/lease/", leaseH)
	mux.Handle("/", webH)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("turnstile listening",
			"version", version, "addr", cfg.Listen, "upstream", cfg.LlamaSwap.BaseURL)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	if err := activityStore.Close(); err != nil {
		log.Warn("flush activity history", "err", err)
	}
	if err := sched.Shutdown(shutdownCtx); err != nil {
		log.Warn("scheduler shutdown", "err", err)
	}
	return nil
}

// loadConfig reads and parses the YAML config, applying a few defaults.
func loadConfig(path string) (types.Config, error) {
	var cfg types.Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse yaml: %w", err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.LlamaSwap.BaseURL == "" {
		cfg.LlamaSwap.BaseURL = "http://localhost:11434"
	}
	if cfg.LlamaSwap.UnloadPath == "" {
		cfg.LlamaSwap.UnloadPath = "/api/models/unload/{model}"
	}
	if cfg.TTL.GraceSeconds == 0 {
		cfg.TTL.GraceSeconds = 300
	}
	if cfg.Preemption.GraceSeconds == 0 {
		cfg.Preemption.GraceSeconds = 30
	}
	if cfg.Lease.HeartbeatTimeoutSeconds == 0 {
		cfg.Lease.HeartbeatTimeoutSeconds = 30
	}
	if cfg.Lease.PollAfterMs == 0 {
		cfg.Lease.PollAfterMs = 1000
	}
	if cfg.Activity.HistoryPath == "" {
		cfg.Activity.HistoryPath = ".turnstile-activity.json"
	}
	if cfg.Activity.MaxRequests == 0 {
		cfg.Activity.MaxRequests = 500
	}
	if cfg.Activity.MaxOutputBytes == 0 {
		cfg.Activity.MaxOutputBytes = 1 << 20
	}
	if cfg.Activity.RetentionHours == 0 {
		cfg.Activity.RetentionHours = 168
	}
	switch cfg.HistoryLog {
	case "":
		cfg.HistoryLog = filepath.Join(filepath.Dir(path), "history.jsonl")
	case "none", "-":
		cfg.HistoryLog = ""
	}
	return cfg, nil
}
