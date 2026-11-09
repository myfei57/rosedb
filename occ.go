package rosedb

import "sync"

// keyMeta is the in-memory optimistic-concurrency metadata of a single key.
//
// It is a logical descriptor: it never stores WAL positions, so a Merge
// (which only rewrites physical positions) does not perturb it in any way.
//
// Fields are guarded by occController.mu.
type keyMeta struct {
	// version is the global logical version at which the key last changed.
	// Every successful committing write (normal record, delete, TTL write
	// that ends up invisible, old-style Batch included) advances the global
	// version and stamps the keys it touches. Pure time-based expiry and
	// Merge do not advance it.
	version uint64

	// exists is the current logical visibility: false for a deleted key or a
	// write that was already expired at commit time.
	exists bool

	// expire is the absolute expiry time (UnixNano) of the current visible
	// record, 0 when the key has no TTL. It lets validation treat a record
	// expiring while a batch is in flight as a visibility change without
	// requiring a physical index probe.
	expire int64

	// refs counts how many unfinished OptimisticBatches currently hold an
	// observation (read or staged write) on this key. While refs > 0 the
	// delete generation must be retained so that create/delete ABA on a
	// previously missing key cannot slip past conflict validation.
	refs int
}

// committedWrite describes one key outcome of a successful commit, used to
// publish the new logical state to the OCC metadata.
type committedWrite struct {
	key    string
	exists bool
	expire int64
}

// occController keeps per-key logical versions for optimistic concurrency
// control.
//
// Historical metadata is reclaimed eagerly: a meta for a non-existent key is
// removed as soon as no unfinished optimistic batch observes it, so the map
// never grows into an unbounded tombstone table. Metas of existing keys are
// bounded by the number of live keys, exactly like the data index itself.
type occController struct {
	mu      sync.Mutex
	version uint64
	metas   map[string]*keyMeta
}

func newOccController() *occController {
	return &occController{metas: make(map[string]*keyMeta)}
}

// snapshot is the stable per-key observation captured by an OptimisticBatch
// the first time it reads or stages a key.
type keySnapshot struct {
	exists  bool
	version uint64
	expire  int64
}

// observe registers an observation of key and returns its current logical
// snapshot. physExists/physExpire describe the physical state just read from
// index+WAL (an expired-but-still-indexed record is already reported as not
// existing). The caller must hold occController.mu.
//
// Time-driven visibility loss (the record expired, or expiry cleanup removed
// the index entry) is reconciled here without advancing the version: a commit
// is the only thing allowed to advance the logical version. An earlier
// observer that saw the key existing then loses on the exists mismatch; a new
// observer sees the key as absent.
func (o *occController) observe(key string, physExists bool, physExpire int64) keySnapshot {
	m := o.metas[key]
	if m == nil {
		m = &keyMeta{
			exists: physExists,
			expire: physExpire,
		}
		o.metas[key] = m
	} else if m.exists && !physExists {
		// Logical expiry/lazy cleanup caught up with the physical state.
		m.exists = false
	}
	m.refs++
	return keySnapshot{exists: m.exists, version: m.version, expire: m.expire}
}

// release drops one observation reference. Tombstone generations that nobody
// observes anymore are reclaimed immediately. The caller must hold
// occController.mu.
func (o *occController) release(key string) {
	m := o.metas[key]
	if m == nil {
		return
	}
	if m.refs > 0 {
		m.refs--
	}
	if m.refs == 0 && !m.exists {
		delete(o.metas, key)
	}
}

// validate reports whether key still matches the captured snapshot. now is the
// commit time (UnixNano); a visible record that has expired by then is a
// visibility change and a conflict. The caller must hold occController.mu.
func (o *occController) validate(key string, snap keySnapshot, now int64) bool {
	m := o.metas[key]
	if m == nil || m.version != snap.version || m.exists != snap.exists {
		return false
	}
	if snap.exists && m.expire > 0 && m.expire <= now {
		return false
	}
	return true
}

// publish stamps all keys of one successful commit with a single new global
// version. A key whose final state is non-existent and which no unfinished
// optimistic batch observes is reclaimed immediately, so writers that only
// go through the old Batch/DB APIs cannot accumulate an unbounded tombstone
// map. Caller must hold occController.mu and must have validated first.
func (o *occController) publish(writes []committedWrite) {
	o.version++
	v := o.version
	for _, w := range writes {
		m := o.metas[w.key]
		if m == nil {
			m = &keyMeta{}
			o.metas[w.key] = m
		}
		m.version = v
		m.exists = w.exists
		m.expire = w.expire
		if !m.exists && m.refs == 0 {
			delete(o.metas, w.key)
		}
	}
}
