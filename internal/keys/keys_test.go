package keys

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

func newSeeded(t *testing.T) types.KeyStore {
	t.Helper()
	seed := []types.KeyInfo{
		{Name: "pi", Priority: 40, Key: "secret-pi", Created: time.Unix(100, 0)},
		{Name: "xad-pod", Priority: 30, Key: "secret-xad", Created: time.Unix(200, 0)},
		{Name: "empty", Priority: 5, Key: ""}, // must be skipped
	}
	ks, err := New(seed, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ks
}

func TestSeedingAndLookup(t *testing.T) {
	ks := newSeeded(t)

	info, found := ks.Lookup("secret-pi")
	if !found {
		t.Fatal("expected to find secret-pi")
	}
	if info.Name != "pi" || info.Priority != 40 {
		t.Fatalf("wrong info: %+v", info)
	}

	info, found = ks.Lookup("secret-xad")
	if !found || info.Name != "xad-pod" || info.Priority != 30 {
		t.Fatalf("wrong info for xad: %+v found=%v", info, found)
	}
}

func TestEmptySeedKeySkipped(t *testing.T) {
	ks := newSeeded(t)
	if _, found := ks.Lookup(""); found {
		t.Fatal("empty key must never be found")
	}
	// The seed entry named "empty" had no secret, so it must not be present.
	for _, ki := range ks.List() {
		if ki.Name == "empty" {
			t.Fatal("seed entry with empty Key should have been skipped")
		}
	}
}

func TestAnonymousFallback(t *testing.T) {
	seed := []types.KeyInfo{{Name: "pi", Priority: 40, Key: "secret-pi"}}
	ks, err := New(seed, 7)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := ks.AnonymousPriority(); got != 7 {
		t.Fatalf("AnonymousPriority = %d, want 7", got)
	}
	if _, found := ks.Lookup("nope"); found {
		t.Fatal("unknown key must not be found")
	}
	if _, found := ks.Lookup(""); found {
		t.Fatal("empty key must not be found")
	}
}

func TestMintUniqueness(t *testing.T) {
	ks := newSeeded(t)

	a, err := ks.Mint("agent", 15)
	if err != nil {
		t.Fatalf("Mint a: %v", err)
	}
	b, err := ks.Mint("agent", 15)
	if err != nil {
		t.Fatalf("Mint b: %v", err)
	}

	if a.Key == b.Key {
		t.Fatal("two mints produced identical secrets")
	}
	for _, k := range []string{a.Key, b.Key} {
		if !strings.HasPrefix(k, "tk-") {
			t.Fatalf("minted secret %q missing tk- prefix", k)
		}
	}
	if a.Created.IsZero() || b.Created.IsZero() {
		t.Fatal("minted keys must have Created set")
	}

	// Both must be looked-up-able.
	for _, want := range []types.KeyInfo{a, b} {
		got, found := ks.Lookup(want.Key)
		if !found {
			t.Fatalf("minted key %q not found", want.Key)
		}
		if got.Name != "agent" || got.Priority != 15 {
			t.Fatalf("looked-up mint mismatch: %+v", got)
		}
	}
}

func TestListRedaction(t *testing.T) {
	ks := newSeeded(t)
	if _, err := ks.Mint("agent", 15); err != nil {
		t.Fatalf("Mint: %v", err)
	}

	list := ks.List()
	if len(list) != 3 { // pi, xad-pod, agent
		t.Fatalf("expected 3 keys, got %d", len(list))
	}
	for _, ki := range list {
		if ki.Key != "" {
			t.Fatalf("List leaked a secret for %q: %q", ki.Name, ki.Key)
		}
	}

	// Verify stable sort by name.
	names := []string{list[0].Name, list[1].Name, list[2].Name}
	want := []string{"agent", "pi", "xad-pod"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("unstable order: got %v, want %v", names, want)
		}
	}
}

func TestRevokeByName(t *testing.T) {
	ks := newSeeded(t)
	// Two keys sharing the name "dup".
	k1, _ := ks.Mint("dup", 1)
	k2, _ := ks.Mint("dup", 2)

	if err := ks.Revoke("dup"); err != nil {
		t.Fatalf("Revoke by name: %v", err)
	}
	if _, found := ks.Lookup(k1.Key); found {
		t.Fatal("k1 should be revoked")
	}
	if _, found := ks.Lookup(k2.Key); found {
		t.Fatal("k2 should be revoked")
	}
	// Original seeded keys still present.
	if _, found := ks.Lookup("secret-pi"); !found {
		t.Fatal("unrelated key wrongly revoked")
	}
}

func TestRevokeByKey(t *testing.T) {
	ks := newSeeded(t)
	if err := ks.Revoke("secret-pi"); err != nil {
		t.Fatalf("Revoke by key: %v", err)
	}
	if _, found := ks.Lookup("secret-pi"); found {
		t.Fatal("secret-pi should be revoked")
	}
	if _, found := ks.Lookup("secret-xad"); !found {
		t.Fatal("secret-xad wrongly revoked")
	}
}

func TestRevokeMissing(t *testing.T) {
	ks := newSeeded(t)
	if err := ks.Revoke("does-not-exist"); err == nil {
		t.Fatal("expected error revoking missing key")
	} else if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := ks.Revoke(""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for empty, got %v", err)
	}
}
