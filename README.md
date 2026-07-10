# turnstile

A single-GPU arbitration service. `turnstile` owns **one priority queue** for a
shared GPU and hands access out one consumer at a time.

It presents two front doors over the same queue:

- **Transparent OpenAI-compatible proxy** — sits in front of
  [`llama-swap`](https://github.com/mostlygeek/llama-swap) at `:11434`. LLM
  clients (e.g. `pi`, `hermes`) point their base URL at turnstile and need no
  other change; turnstile holds the connection until the client wins the GPU,
  then reverse-proxies to llama-swap.
- **Cooperative lease API** — non-LLM GPU jobs (e.g. `xad-pod` Whisper,
  `slam-nerf` reconstruction) `POST /lease`, poll until granted, do their work,
  and `POST /lease/{id}/release`. Advisory (not enforced), poll-based (no
  client-side HTTP server required).

## Scheduling

- Priority derives from the caller's **API key**. No request is serviced while a
  higher-priority request is pending; equal priority is FIFO.
- In-progress LLM requests are never killed. In-progress **leases** can be asked
  to cooperatively yield (`revoke_requested`) when a higher-priority waiter
  arrives.
- **Demand-driven dynamic TTL:** the resident LLM model stays warm indefinitely
  while nothing is waiting. Once a competitor is pending, a configurable grace
  TTL protects multi-call agent turns before the model is evicted (via
  llama-swap's synchronous `POST /api/models/unload/{model}`). Only a *strictly
  lower*-priority waiter waits out that grace; an equal-or-higher-priority waiter
  collapses it to zero (so a client never blocks behind its own stale warm cache
  when switching models).

## Layout

```
cmd/turnstile      entrypoint
internal/types     shared types + interfaces (the frozen contract)
internal/queue     priority queue + scheduler goroutine
internal/proxy     OpenAI reverse proxy + admission
internal/lease     lease / poll API
internal/keys      API-key store + priority mapping
internal/web       console + live queue visualization
internal/llamaswap unload client
```

## Config

Copy `config.example.yaml` to `config.yaml` and edit. See that file for the
key → priority map, llama-swap upstream URL, listen port, and TTL knobs.

## Status

Under active development. See the design/plan for full details.
