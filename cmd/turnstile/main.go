// Command turnstile is a single-GPU arbitration service: one priority queue
// fronting llama-swap as an OpenAI-compatible proxy, plus a cooperative lease
// API for non-LLM GPU jobs.
//
// This is the scaffold entrypoint; wiring is filled in as the internal
// packages land (see internal/types for the frozen contract).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("turnstile: ")

	if _, err := os.Stat(*configPath); err != nil {
		log.Fatalf("config %q: %v", *configPath, err)
	}

	// TODO: load config, start scheduler, proxy, lease API, and web console.
	log.Printf("scaffold build %s — not yet wired; config=%s", version, *configPath)
}
