// Package store is turnstile's durable state: a single SQLite database (pure-Go
// modernc driver, no cgo) that persists three things so arbitration survives a
// process restart:
//
//   - holds:  the completed-hold history that backs the console timeline.
//   - active: the live lease queue + current lease holder. LLM tickets are NOT
//     persisted here — an LLM hold is bound to a live HTTP connection that dies
//     with the process, so it cannot be resumed. Leases are poll/heartbeat
//     based, so a restored lease keeps working the moment its client polls again.
//   - kv:     singletons — the warm-model row (model/tier/last-LLM-complete, so
//     the keep-warm timer survives) and the enqueue sequence counter.
//
// All methods are safe for concurrent use (database/sql pools connections); in
// practice the scheduler's single owner goroutine is the only writer.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aiguy110/gpu-turnstile/internal/types"
)

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
}

// Active is a persisted live lease (holder or queued waiter).
type Active struct {
	ID              string
	Name            string
	Priority        types.Priority
	Kind            types.Kind
	Model           types.Model
	Est             int
	Seq             uint64
	State           types.TicketState
	EnqueuedAt      time.Time
	GrantedAt       time.Time
	LastHeartbeat   time.Time
	RevokeRequested bool
}

// Warm is the persisted warm-model singleton.
type Warm struct {
	Model           types.Model    `json:"model"`
	Tier            types.Priority `json:"tier"`
	LastLLMComplete time.Time      `json:"last_llm_complete"`
}

// Open opens (creating if needed) the database at path and runs migrations.
func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one writer; avoids WAL contention for our tiny load
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS holds (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  id         TEXT,
  name       TEXT,
  priority   INTEGER,
  kind       TEXT,
  model      TEXT,
  started_at INTEGER,
  ended_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_holds_ended ON holds(ended_at);
CREATE TABLE IF NOT EXISTS active (
  id               TEXT PRIMARY KEY,
  name             TEXT,
  priority         INTEGER,
  kind             TEXT,
  model            TEXT,
  est              INTEGER,
  seq              INTEGER,
  state            TEXT,
  enqueued_at      INTEGER,
  granted_at       INTEGER,
  last_heartbeat   INTEGER,
  revoke_requested INTEGER
);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
`
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

// ---- time helpers: store as unix nanoseconds, 0 == zero time ----

func toNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// ---- history ----

// AppendHold records one completed hold.
func (s *Store) AppendHold(r types.HoldRecord) error {
	_, err := s.db.Exec(
		`INSERT INTO holds (id,name,priority,kind,model,started_at,ended_at) VALUES (?,?,?,?,?,?,?)`,
		r.ID, r.Name, int(r.Priority), string(r.Kind), string(r.Model),
		toNanos(r.StartedAt), toNanos(r.EndedAt))
	return err
}

// LoadHolds returns completed holds ending after `since`, oldest first.
func (s *Store) LoadHolds(since time.Time) ([]types.HoldRecord, error) {
	rows, err := s.db.Query(
		`SELECT id,name,priority,kind,model,started_at,ended_at FROM holds
		 WHERE ended_at > ? ORDER BY ended_at ASC`, toNanos(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.HoldRecord
	for rows.Next() {
		var r types.HoldRecord
		var prio int
		var kind, model string
		var start, end int64
		if err := rows.Scan(&r.ID, &r.Name, &prio, &kind, &model, &start, &end); err != nil {
			return nil, err
		}
		r.Priority = types.Priority(prio)
		r.Kind = types.Kind(kind)
		r.Model = types.Model(model)
		r.StartedAt = fromNanos(start)
		r.EndedAt = fromNanos(end)
		out = append(out, r)
	}
	return out, rows.Err()
}

// PruneHolds deletes holds that ended at or before `before`.
func (s *Store) PruneHolds(before time.Time) error {
	_, err := s.db.Exec(`DELETE FROM holds WHERE ended_at <= ?`, toNanos(before))
	return err
}

// ---- active leases ----

// SaveActive upserts a live lease.
func (s *Store) SaveActive(a Active) error {
	rev := 0
	if a.RevokeRequested {
		rev = 1
	}
	_, err := s.db.Exec(
		`INSERT INTO active (id,name,priority,kind,model,est,seq,state,enqueued_at,granted_at,last_heartbeat,revoke_requested)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   name=excluded.name, priority=excluded.priority, kind=excluded.kind, model=excluded.model,
		   est=excluded.est, seq=excluded.seq, state=excluded.state, enqueued_at=excluded.enqueued_at,
		   granted_at=excluded.granted_at, last_heartbeat=excluded.last_heartbeat,
		   revoke_requested=excluded.revoke_requested`,
		a.ID, a.Name, int(a.Priority), string(a.Kind), string(a.Model), a.Est, int64(a.Seq),
		string(a.State), toNanos(a.EnqueuedAt), toNanos(a.GrantedAt), toNanos(a.LastHeartbeat), rev)
	return err
}

// DeleteActive removes a lease by id (idempotent).
func (s *Store) DeleteActive(id string) error {
	_, err := s.db.Exec(`DELETE FROM active WHERE id = ?`, id)
	return err
}

// LoadActive returns all persisted live leases ordered by enqueue sequence.
func (s *Store) LoadActive() ([]Active, error) {
	rows, err := s.db.Query(
		`SELECT id,name,priority,kind,model,est,seq,state,enqueued_at,granted_at,last_heartbeat,revoke_requested
		 FROM active ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Active
	for rows.Next() {
		var a Active
		var prio, est, rev int
		var seq, enq, grant, hb int64
		var kind, model, state string
		if err := rows.Scan(&a.ID, &a.Name, &prio, &kind, &model, &est, &seq, &state,
			&enq, &grant, &hb, &rev); err != nil {
			return nil, err
		}
		a.Priority = types.Priority(prio)
		a.Kind = types.Kind(kind)
		a.Model = types.Model(model)
		a.Est = est
		a.Seq = uint64(seq)
		a.State = types.TicketState(state)
		a.EnqueuedAt = fromNanos(enq)
		a.GrantedAt = fromNanos(grant)
		a.LastHeartbeat = fromNanos(hb)
		a.RevokeRequested = rev != 0
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- singletons (kv) ----

// SaveWarm persists the warm-model state. An empty model clears the row.
func (s *Store) SaveWarm(w Warm) error {
	if w.Model == "" {
		_, err := s.db.Exec(`DELETE FROM kv WHERE k = 'warm'`)
		return err
	}
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return s.putKV("warm", string(b))
}

// LoadWarm returns the warm-model state; found is false when none is stored.
func (s *Store) LoadWarm() (w Warm, found bool, err error) {
	v, ok, err := s.getKV("warm")
	if err != nil || !ok {
		return Warm{}, false, err
	}
	if err := json.Unmarshal([]byte(v), &w); err != nil {
		return Warm{}, false, err
	}
	return w, true, nil
}

// SaveSeq persists the enqueue sequence counter.
func (s *Store) SaveSeq(n uint64) error { return s.putKV("seq", fmt.Sprintf("%d", n)) }

// LoadSeq returns the persisted sequence counter (0 if none).
func (s *Store) LoadSeq() (uint64, error) {
	v, ok, err := s.getKV("seq")
	if err != nil || !ok {
		return 0, err
	}
	var n uint64
	_, err = fmt.Sscanf(v, "%d", &n)
	return n, err
}

func (s *Store) putKV(k, v string) error {
	_, err := s.db.Exec(
		`INSERT INTO kv (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

func (s *Store) getKV(k string) (string, bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM kv WHERE k = ?`, k).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}
