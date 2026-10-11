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
- **Co-admission (opt-in per model):** a model listed under `models:` with
  `slots: N` runs up to N LLM requests at once (llama-server `--parallel N`). The
  front waiter joins the current holders when it is an LLM request for the same
  model, a slot is free, and (with `ctxPool`) the in-flight requests' estimated
  `prompt + max_tokens` plus its own fits the pool. It never skips ahead: if the
  front waiter can't join, everyone behind it waits too. Leases and other models
  still need an idle GPU. Models without an entry behave exactly as before.
- **Virtual models:** names under `virtual:` map to a real `models:` entry. The
  proxy rewrites `model` to the backend, deep-merges `inject` into the body
  (e.g. turn thinking off, pin `id_slot`), and answers 400
  `context_length_exceeded` without queueing when the estimate exceeds `maxCtx`.
  Eviction, warm tracking, and slots all use the real name; the alias shows in the
  console and activity history. Prompt tokens are estimated as `ceil(chars/3)`
  (pessimistic) over messages, tool definitions, and `prompt`/`input`; bodies over
  4 MiB are forwarded untouched and budgeted as the whole pool.

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

The console's **LLM activity** view follows running responses live and retains a
bounded local history. It shows normalized model text plus generic, expandable
tool-call arguments/results; it never records authorization headers or API keys.
Retention, history size, and per-output capture limits are configured under
`activity`.

## Config

Copy `config.example.yaml` to `config.yaml` and edit. See that file for the
key → priority map, llama-swap upstream URL, listen port, and TTL knobs.

## Status

Under active development. See the design/plan for full details.
