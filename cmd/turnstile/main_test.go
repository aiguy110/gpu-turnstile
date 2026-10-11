package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const routingYAML = `
models:
  bonsai-2-27b: { slots: 2, ctxPool: 98304 }
virtual:
  bonsai-agent: { backend: bonsai-2-27b, maxCtx: 81920 }
  bonsai-mem:
    backend: bonsai-2-27b
    maxCtx: 12288
    defaultMaxTokens: 1024
    inject:
      chat_template_kwargs: { enable_thinking: false }
`

func TestLoadConfigRouting(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, routingYAML))
	if err != nil {
		t.Fatal(err)
	}
	if m := cfg.Models["bonsai-2-27b"]; m.Slots != 2 || m.CtxPool != 98304 {
		t.Errorf("models = %+v", cfg.Models)
	}
	mem := cfg.Virtual["bonsai-mem"]
	kw, ok := mem.Inject["chat_template_kwargs"].(map[string]any)
	if mem.Backend != "bonsai-2-27b" || mem.DefaultMaxTokens != 1024 || !ok || kw["enable_thinking"] != false {
		t.Errorf("bonsai-mem = %+v", mem)
	}
}

func TestLoadConfigRoutingValidation(t *testing.T) {
	cases := map[string]string{
		"has no models": "virtual:\n  v: { backend: nope }\n",
		"exceeds":       "models:\n  m: { slots: 2, ctxPool: 100 }\nvirtual:\n  v: { backend: m, maxCtx: 200 }\n",
		"collides":      "models:\n  m: { slots: 2 }\nvirtual:\n  m: { backend: m }\n",
		"must be >= 0":  "models:\n  m: { slots: -1 }\n",
	}
	for want, body := range cases {
		if _, err := loadConfig(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v", want, err)
		}
	}
}
