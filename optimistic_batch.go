package rosedb

import (
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
)

// OptimisticBatch is an optimistic-concurrency point-operation batch.
//
// Unlike Batch, it never holds the database write lock during the business
// phase: any number of optimistic batches can stage reads and writes
// concurrently. The DB write lock (and the WAL) is taken only at Commit,
// for a short critical section.
//
// The first time a key is read or staged the batch captures the key's logical
// version and existence together with a private snapshot of its value. Later
// reads return the staged overlay, or the stable content observed the first
// time: the batch has repeatable reads and never observes partial writes of
// another transaction.
//
// Commit validates ALL observed and written keys against their current logical
// versions in one shot and only then writes the WAL, appending the same
// batch-finished marker as Batch and publishing the index atomically. Any
// single conflict fails the whole commit; staged keys are never published one
// by one. Conflict detection is logical (monotonic version plus delete
// generations), never value or WAL-position based: delete/recreate ABA,
// same-value rewrites and create/delete of previously missing keys all
// conflict, while a Merge (pure physical relocation) never does.
//
// Only point operations (Get, Put, PutWithTTL, Delete) are supported. There is
// no range scan and no range phantom protection is claimed.
//
// Successful outcomes are equivalent to executing the batch's point
// operations in some serial order. On conflict (ErrOptimisticConflict) the
// caller must build a new OptimisticBatch, re-read and retry.
//
// Every batch must end with exactly one Commit or Rollback; both, as well as
// failed commits and a Close/Commit race, release all captured observations
// and references.
type OptimisticBatch struct {
	db         *DB
	options    BatchOptions
	instanceID uint64
	batchId    *snowflake.Node

	mu          sync.Mutex
	committed   bool
	rollbacked  bool
	observed    map[string]*optObservation // first-touch stable snapshots
	pending     map[string]*LogRecord      // staged writes, one record per key
	pendingKeys []string                   // deterministic staging order
	buffers     []*bytebufferpool.ByteBuffer
}

// optObservation is the first-touch observation of one key.
type optObservation struct {
	snap  keySnapshot
	value []byte // owned stable value snapshot, nil when the key was absent
}

// NewOptimisticBatch creates an OptimisticBatch bound to this DB instance.
// The call does not take any DB lock: business logic runs without blocking
// other writers. Options.Sync controls whether Commit fsyncs the WAL;
// Options.ReadOnly forbids staging writes.
func (db *DB) NewOptimisticBatch(options BatchOptions) *OptimisticBatch {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &OptimisticBatch{
		db:         db,
		options:    options,
		instanceID: db.instanceID,
		batchId:    node,
		observed:   make(map[string]*optObservation),
		pending:    make(map[string]*LogRecord),
	}
}

func (ob *OptimisticBatch) finishedLocked() bool {
	return ob.committed || ob.rollbacked
}

// observeKey captures the first-touch observation of key.
//
// The physical point read (index+WAL, expiry evaluation) and the logical
// version capture happen in ONE critical section with the same lock order
// used by commit (db.mu then occ.mu), so a committing transaction can never
// land in between: a value snapshot and the version it is validated against
// always describe the same logical instant. The locks are held only for this
// short read, never across the caller's business logic.
//
// An expired record still present in the physical index is observed as
// non-existent (and lazily removed from the index, as elsewhere).
//
// The returned string is the canonical owned key copy used by all
// batch-internal maps.
func (ob *OptimisticBatch) observeKey(key []byte) (string, *optObservation, error) {
	ob.mu.Lock()
	if ob.finishedLocked() {
		ob.mu.Unlock()
		return "", nil, ErrOptimisticBatchFinished
	}
	keyStr := string(key) // owned copy
	if obs, ok := ob.observed[keyStr]; ok {
		ob.mu.Unlock()
		return keyStr, obs, nil
	}
	ob.mu.Unlock()

	db := ob.db
	db.mu.RLock()
	// Close may have won the lock while we were waiting for it.
	if db.isClosed() {
		db.mu.RUnlock()
		return "", nil, ErrDBClosed
	}

	position := db.index.Get(key)
	var (
		physExists bool
		value      []byte
		expire     int64
	)
	if position != nil {
		chunk, err := db.dataFiles.Read(position)
		if err != nil {
			db.mu.RUnlock()
			if db.isClosed() {
				return "", nil, ErrDBClosed
			}
			return "", nil, err
		}
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordDeleted {
			db.mu.RUnlock()
			panic("rosedb: deleted data cannot exist in the index")
		}
		if record.IsExpired(time.Now().UnixNano()) {
			// expired but still physically indexed: observe as absent.
			db.index.Delete(record.Key)
		} else {
			physExists = true
			value = record.Value
			expire = record.Expire
		}
	}

	// Capture the logical version while the DB read lock is still held, so
	// no commit can publish between the physical read and this point.
	db.occ.mu.Lock()
	snap := db.occ.observe(keyStr, physExists, expire)
	db.occ.mu.Unlock()
	db.mu.RUnlock()

	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.finishedLocked() {
		// Rollback/Commit finished the batch while the read was in flight;
		// the observation never entered the map, drop its reference here.
		db.occ.mu.Lock()
		db.occ.release(keyStr)
		db.occ.mu.Unlock()
		return "", nil, ErrOptimisticBatchFinished
	}
	if obs, ok := ob.observed[keyStr]; ok {
		// A concurrent call for the same batch won the first-touch race;
		// release the duplicate reference and reuse the stored observation.
		db.occ.mu.Lock()
		db.occ.release(keyStr)
		db.occ.mu.Unlock()
		return keyStr, obs, nil
	}
	obs := &optObservation{snap: snap, value: value}
	ob.observed[keyStr] = obs
	return keyStr, obs, nil
}

// Get returns the value of key. A staged write takes precedence; otherwise the
// stable content captured at the first observation of key is returned.
func (ob *OptimisticBatch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if ob.db.isClosed() {
		return nil, ErrDBClosed
	}
	ob.mu.Lock()
	if ob.finishedLocked() {
		ob.mu.Unlock()
		return nil, ErrOptimisticBatchFinished
	}
	if rec, ok := ob.pending[string(key)]; ok {
		now := time.Now().UnixNano()
		if rec.Type == LogRecordDeleted || rec.IsExpired(now) {
			ob.mu.Unlock()
			return nil, ErrKeyNotFound
		}
		v := rec.Value
		ob.mu.Unlock()
		return v, nil
	}
	ob.mu.Unlock()

	_, obs, err := ob.observeKey(key)
	if err != nil {
		return nil, err
	}
	if !obs.snap.exists {
		return nil, ErrKeyNotFound
	}
	return obs.value, nil
}

// Put stages an unconditional write of key/value. User slices are copied at
// staging time; mutating them afterwards does not affect the batch.
func (ob *OptimisticBatch) Put(key, value []byte) error {
	return ob.stage(key, value, 0)
}

// PutWithTTL stages a write with an absolute expiry computed NOW, at the
// explicit staging moment. Commit never recomputes it, so commit delays or
// retries cannot accidentally extend the TTL.
func (ob *OptimisticBatch) PutWithTTL(key, value []byte, ttl time.Duration) error {
	expire := time.Now().Add(ttl).UnixNano()
	return ob.stage(key, value, expire)
}

// Delete stages a deletion of key. Deleting a key not observed before still
// registers its current state as the conflict baseline, so concurrent
// creation of that key causes a write/write conflict.
func (ob *OptimisticBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if ob.db.isClosed() {
		return ErrDBClosed
	}
	ob.mu.Lock()
	if ob.finishedLocked() {
		ob.mu.Unlock()
		return ErrOptimisticBatchFinished
	}
	if ob.options.ReadOnly {
		ob.mu.Unlock()
		return ErrReadOnlyBatch
	}
	ob.mu.Unlock()

	keyStr, _, err := ob.observeKey(key)
	if err != nil {
		return err
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.finishedLocked() {
		return ErrOptimisticBatchFinished
	}
	if rec := ob.pending[keyStr]; rec != nil {
		rec.Type = LogRecordDeleted
		rec.Value = nil
		rec.Expire = 0
		return nil
	}
	rec := &LogRecord{
		Key:  []byte(keyStr),
		Type: LogRecordDeleted,
	}
	ob.pending[keyStr] = rec
	ob.pendingKeys = append(ob.pendingKeys, keyStr)
	return nil
}

func (ob *OptimisticBatch) stage(key, value []byte, expire int64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if ob.db.isClosed() {
		return ErrDBClosed
	}
	ob.mu.Lock()
	if ob.finishedLocked() {
		ob.mu.Unlock()
		return ErrOptimisticBatchFinished
	}
	if ob.options.ReadOnly {
		ob.mu.Unlock()
		return ErrReadOnlyBatch
	}
	ob.mu.Unlock()

	// Staging a write observes the key first: written-but-never-read keys are
	// validated too, which is what detects write/write conflicts.
	keyStr, _, err := ob.observeKey(key)
	if err != nil {
		return err
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.finishedLocked() {
		return ErrOptimisticBatchFinished
	}

	// form owned snapshots of the caller's slices on acceptance.
	ownedKey := []byte(keyStr)
	ownedValue := append([]byte(nil), value...)

	rec := ob.pending[keyStr]
	if rec == nil {
		rec = &LogRecord{Key: ownedKey}
		ob.pending[keyStr] = rec
		ob.pendingKeys = append(ob.pendingKeys, keyStr)
	} else {
		rec.Key = ownedKey
	}
	rec.Type = LogRecordNormal
	rec.Value = ownedValue
	rec.Expire = expire
	return nil
}

// Commit validates every observed and written key and, if nothing changed,
// publishes all staged writes atomically via the WAL batch-finished marker.
// On conflict, I/O failure or a closed database no staged key becomes
// visible and all observations/references are released.
func (ob *OptimisticBatch) Commit() error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.committed {
		return ErrBatchCommitted
	}
	if ob.rollbacked {
		return ErrBatchRollbacked
	}

	db := ob.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.isClosed() {
		ob.rollbacked = true
		ob.finalizeLocked()
		return ErrDBClosed
	}
	if db.instanceID != ob.instanceID {
		ob.rollbacked = true
		ob.finalizeLocked()
		return ErrOptimisticInstanceMismatch
	}

	now := time.Now().UnixNano()
	occ := db.occ

	// Phase 1: validate the whole read/write set at once. Nothing is allowed
	// to be published per key.
	occ.mu.Lock()
	for key, obs := range ob.observed {
		if !occ.validate(key, obs.snap, now) {
			occ.mu.Unlock()
			ob.rollbacked = true
			ob.finalizeLocked()
			return ErrOptimisticConflict
		}
	}

	// Read-only (or empty) optimistic batch: nothing to publish.
	if len(ob.pending) == 0 {
		for key := range ob.observed {
			occ.release(key)
		}
		occ.mu.Unlock()
		ob.committed = true
		ob.clearResourcesLocked()
		return nil
	}

	// Phase 2: stage all records plus the batch-finished marker.
	batchId := ob.batchId.Generate()
	for _, key := range ob.pendingKeys {
		record := ob.pending[key]
		record.BatchId = uint64(batchId)
		buf := bytebufferpool.Get()
		ob.buffers = append(ob.buffers, buf)
		db.dataFiles.PendingWrites(encodeLogRecord(record, db.encodeHeader, buf))
	}
	endBuf := bytebufferpool.Get()
	ob.buffers = append(ob.buffers, endBuf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, db.encodeHeader, endBuf)
	db.dataFiles.PendingWrites(endRecord)

	// Phase 3: one sequential WAL write makes the batch physically whole.
	positions, err := db.dataFiles.WriteAll()
	if err != nil {
		db.dataFiles.ClearPendingWrites()
		occ.mu.Unlock()
		ob.rollbacked = true
		ob.finalizeLocked()
		return err
	}
	if len(positions) != len(ob.pendingKeys)+1 {
		occ.mu.Unlock()
		ob.rollbacked = true
		ob.finalizeLocked()
		panic("rosedb: chunk positions length is not equal to pending writes length")
	}

	if ob.options.Sync || db.options.Sync {
		if err := db.dataFiles.Sync(); err != nil {
			occ.mu.Unlock()
			ob.rollbacked = true
			ob.finalizeLocked()
			return err
		}
	}

	// Phase 4: publish index and logical versions together while db.mu and
	// occ.mu are held, so validation and publication are atomic.
	writes := make([]committedWrite, 0, len(ob.pendingKeys))
	for i, key := range ob.pendingKeys {
		record := ob.pending[key]
		visible := record.Type != LogRecordDeleted && !record.IsExpired(now)
		if visible {
			db.index.Put([]byte(key), positions[i])
		} else {
			db.index.Delete([]byte(key))
		}
		writes = append(writes, committedWrite{
			key:    key,
			exists: visible,
			expire: record.Expire,
		})

		if db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			db.watcher.putEvent(e)
		}
	}
	occ.publish(writes)
	for key := range ob.observed {
		occ.release(key)
	}
	occ.mu.Unlock()

	ob.committed = true
	ob.clearResourcesLocked()
	return nil
}

// Rollback discards the batch, releasing every captured observation,
// reference and buffer, even if the database has been closed in the meantime.
func (ob *OptimisticBatch) Rollback() error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.committed || ob.rollbacked {
		return ErrOptimisticBatchFinished
	}
	ob.rollbacked = true
	ob.finalizeLocked()
	if ob.db.isClosed() {
		return ErrDBClosed
	}
	return nil
}

// finalizeLocked drops OCC references and recycles buffers. It must run on
// every terminal path: conflict, WAL/sync error, commit against a closed DB,
// explicit rollback and Close/Commit races. Caller must hold ob.mu.
func (ob *OptimisticBatch) finalizeLocked() {
	if len(ob.observed) > 0 {
		occ := ob.db.occ
		occ.mu.Lock()
		for key := range ob.observed {
			occ.release(key)
		}
		occ.mu.Unlock()
	}
	ob.clearResourcesLocked()
}

// clearResourcesLocked recycles byte buffers and drops all owned maps.
// Caller must hold ob.mu.
func (ob *OptimisticBatch) clearResourcesLocked() {
	for _, buf := range ob.buffers {
		bytebufferpool.Put(buf)
	}
	ob.buffers = nil
	ob.pending = nil
	ob.pendingKeys = nil
	ob.observed = nil
}
