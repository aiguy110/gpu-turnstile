package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// charsPerToken is the deliberately pessimistic chars→tokens ratio used for
// context budgeting (real tokenizers average ~3.5–4 chars/token on English and
// code, so this overestimates).
const charsPerToken = 3

// perMessageTokens covers chat-template overhead (role markers etc.) per message.
const perMessageTokens = 8

// routeResult is what the proxy learned from a request body before acquiring.
type routeResult struct {
	alias     types.Model // virtual name the client used, "" if none
	ctxTokens int         // estimated prompt + max_tokens, 0 = unknown
	reject    string      // non-empty: answer 400 context_length_exceeded
}

// route applies virtual-model rewriting/injection and the context estimate.
// It returns the body to forward: buf itself when nothing changes.
func (h *Handler) route(buf []byte) ([]byte, routeResult) {
	var rt routeResult
	if len(h.routing.Models) == 0 && len(h.routing.Virtual) == 0 {
		return buf, rt
	}
	var body map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil || body == nil {
		return buf, rt
	}
	name, _ := body["model"].(string)
	model := types.Model(name)
	vm, isVirtual := h.routing.Virtual[model]
	if isVirtual {
		rt.alias = model
		model = vm.Backend
		body["model"] = string(model)
		if len(vm.Inject) > 0 {
			deepMerge(body, normalize(vm.Inject).(map[string]any))
		}
		if out, err := json.Marshal(body); err == nil {
			buf = out
		}
	}
	mc := h.routing.Models[model]
	if mc.CtxPool <= 0 && vm.MaxCtx <= 0 {
		return buf, rt
	}
	limit := mc.CtxPool
	if vm.MaxCtx > 0 {
		limit = vm.MaxCtx
	}
	prompt := estimatePromptTokens(body)
	maxTok := requestedMaxTokens(body)
	if maxTok <= 0 {
		maxTok = vm.DefaultMaxTokens
	}
	if maxTok <= 0 {
		maxTok = max(limit-prompt, 0) // unbounded: budget the rest of the context
	}
	if vm.MaxCtx > 0 && prompt+maxTok > vm.MaxCtx {
		rt.reject = fmt.Sprintf("estimated %d prompt tokens + %d max_tokens exceeds %s's context of %d tokens",
			prompt, maxTok, rt.alias, vm.MaxCtx)
	}
	rt.ctxTokens = prompt + maxTok
	return buf, rt
}

// estimatePromptTokens is ceil(chars/3) over message contents, tool
// definitions, and prompt/input, plus a fixed per-message template overhead.
func estimatePromptTokens(body map[string]any) int {
	chars, msgs := 0, 0
	if list, ok := body["messages"].([]any); ok {
		for _, m := range list {
			msg, ok := m.(map[string]any)
			if !ok {
				continue
			}
			msgs++
			chars += len(contentText(msg["content"]))
			if rc, ok := msg["reasoning_content"].(string); ok {
				chars += len(rc)
			}
			if calls, ok := msg["tool_calls"]; ok {
				raw, _ := json.Marshal(calls)
				chars += len(raw)
			}
		}
	}
	for _, k := range []string{"tools", "functions"} {
		if v, ok := body[k]; ok {
			raw, _ := json.Marshal(v)
			chars += len(raw)
		}
	}
	for _, k := range []string{"prompt", "input"} {
		if v, ok := body[k]; ok && v != nil {
			chars += len(contentText(v))
		}
	}
	return (chars+charsPerToken-1)/charsPerToken + msgs*perMessageTokens
}

// requestedMaxTokens reads max_tokens, max_completion_tokens, or n_predict.
func requestedMaxTokens(body map[string]any) int {
	for _, k := range []string{"max_tokens", "max_completion_tokens", "n_predict"} {
		if n, ok := body[k].(json.Number); ok {
			if v, err := n.Int64(); err == nil && v > 0 {
				return int(v)
			}
		}
	}
	return 0
}

// deepMerge copies src into dst; nested objects merge, everything else in src
// replaces dst's value.
func deepMerge(dst, src map[string]any) {
	for k, v := range src {
		sv, sok := v.(map[string]any)
		dv, dok := dst[k].(map[string]any)
		if sok && dok {
			deepMerge(dv, sv)
			continue
		}
		dst[k] = v
	}
}

// normalize deep-copies YAML-decoded values into JSON-shaped ones
// (map[any]any → map[string]any) so they marshal and merge cleanly.
func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = normalize(x)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[fmt.Sprint(k)] = normalize(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = normalize(x)
		}
		return out
	default:
		return v
	}
}

// writeContextError answers like OpenAI does for an over-long request.
func writeContextError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"message": msg,
		"type":    "invalid_request_error",
		"param":   "messages",
		"code":    "context_length_exceeded",
	}})
}
