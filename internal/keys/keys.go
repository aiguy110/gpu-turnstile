// Package keys is the in-memory implementation of types.KeyStore.
//
// Keys live only in memory; there is no persistence. On boot cmd reseeds the
// store from config (the []types.KeyInfo seed carries the real secrets). All
// state is guarded by a single sync.RWMutex, keyed by the raw secret.
package keys

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// secretPrefix is prepended to every minted secret. Callers present the whole
// string (prefix included) as the bearer token.
const secretPrefix = "tk-"

// secretRandBytes is the number of random bytes behind each minted secret,
// url-safe base64 encoded (no padding).
const secretRandBytes = 32

// store is the concrete KeyStore. byKey maps a raw secret -> its KeyInfo (with
// secret populated); it is the source of truth.
type store struct {
	mu    sync.RWMutex
	byKey map[string]types.KeyInfo
	anon  types.Priority
}

// New builds a KeyStore seeded from the provided KeyInfo slice. Seed entries
// with an empty Key are skipped (a key with no secret can never be presented as
// a bearer token, so it is unusable rather than an error). If two seed entries
// share the same secret, the later one wins.
func New(seed []types.KeyInfo, anon types.Priority) (types.KeyStore, error) {
	s := &store{
		byKey: make(map[string]types.KeyInfo, len(seed)),
		anon:  anon,
	}
	for _, ki := range seed {
		if ki.Key == "" {
			continue
		}
		// Store a copy so the caller's slice can't mutate our state.
		s.byKey[ki.Key] = types.KeyInfo{
			Name:     ki.Name,
			Priority: ki.Priority,
			Key:      ki.Key,
			Created:  ki.Created,
		}
	}
	return s, nil
}

// Lookup resolves a raw secret to its identity. An empty or unknown key returns
// found=false; the caller should then treat the request as anonymous.
func (s *store) Lookup(rawKey string) (types.KeyInfo, bool) {
	if rawKey == "" {
		return types.KeyInfo{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ki, ok := s.byKey[rawKey]
	if !ok {
		return types.KeyInfo{}, false
	}
	return ki, true
}

// AnonymousPriority returns the configured tier for unknown/missing keys.
func (s *store) AnonymousPriority() types.Priority {
	return s.anon
}

// Mint generates a cryptographically-random secret, stores the new key with
// Created=now, and returns the KeyInfo with the secret populated. Duplicate
// names are allowed; names are labels, not identities.
func (s *store) Mint(name string, priority types.Priority) (types.KeyInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate a secret, retrying on the astronomically unlikely collision.
	var secret string
	for {
		buf := make([]byte, secretRandBytes)
		if _, err := rand.Read(buf); err != nil {
			return types.KeyInfo{}, fmt.Errorf("keys: generating secret: %w", err)
		}
		secret = secretPrefix + base64.RawURLEncoding.EncodeToString(buf)
		if _, exists := s.byKey[secret]; !exists {
			break
		}
	}

	ki := types.KeyInfo{
		Name:     name,
		Priority: priority,
		Key:      secret,
		Created:  time.Now(),
	}
	s.byKey[secret] = ki
	return ki, nil
}

// List returns every key with its secret redacted (Key=""). Ordering is stable:
// by name, then by Created time, then by (unexported) secret as a final
// tiebreaker so duplicate-name/time entries have a deterministic order.
func (s *store) List() []types.KeyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type entry struct {
		info types.KeyInfo
		key  string
	}
	entries := make([]entry, 0, len(s.byKey))
	for k, ki := range s.byKey {
		entries = append(entries, entry{info: ki, key: k})
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.info.Name != b.info.Name {
			return a.info.Name < b.info.Name
		}
		if !a.info.Created.Equal(b.info.Created) {
			return a.info.Created.Before(b.info.Created)
		}
		return a.key < b.key
	})

	out := make([]types.KeyInfo, 0, len(entries))
	for _, e := range entries {
		redacted := e.info
		redacted.Key = "" // never leak secrets
		out = append(out, redacted)
	}
	return out
}

// ErrNotFound is returned by Revoke when nothing matched the given name or key.
var ErrNotFound = errors.New("keys: no key matching name or secret")

// Revoke removes a key by exact secret OR by name. Revoking by name removes
// every key sharing that name. It returns ErrNotFound if nothing matched.
func (s *store) Revoke(nameOrKey string) error {
	if nameOrKey == "" {
		return ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Exact secret match first.
	if _, ok := s.byKey[nameOrKey]; ok {
		delete(s.byKey, nameOrKey)
		return nil
	}

	// Otherwise remove all keys whose name matches.
	removed := false
	for k, ki := range s.byKey {
		if ki.Name == nameOrKey {
			delete(s.byKey, k)
			removed = true
		}
	}
	if !removed {
		return ErrNotFound
	}
	return nil
}
