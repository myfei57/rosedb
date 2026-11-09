package rosedb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/valyala/bytebufferpool"
)

// Batch is a batch operations of the database.
// If readonly is true, you can only get data from the batch by Get method.
// An error will be returned if you try to use Put or Delete method.
//
// If readonly is false, you can use Put and Delete method to write data to the batch.
// The data will be written to the database permanently after you call Commit method.
//
// NB. There can only one write batch and multi read-only batches at the same time.
// And the db method is not allowed to use before the batch commit/rollback.
// So a typical usage of Batch is like:
//
// batch := db.NewBatch(rosedb.DefaultBatchOptions)
// batch.Put/batch.Get (and other methods)
// /* 1. a new write batch is not allowed */
// /* 2. invoke DB method is not allowed, like db.Put */
// batch.Commit() or batch.Rollback()
//
// Batch is not a transaction, it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability(if the Sync options is true).
//
// You must call Commit or Rollback method after using the batch,
// otherwise the DB will be locked in an unexpected way.
type Batch struct {
	db               *DB
	pendingWrites    []*LogRecord     // save the data to be written
	pendingWritesMap map[uint64][]int // map record hash key to index, fast lookup to pendingWrites
	options          BatchOptions
	mu               sync.RWMutex
	committed        bool // whether the batch has been committed
	rollbacked       bool // whether the batch has been rollbacked
	batchId          *snowflake.Node
	buffers          []*bytebufferpool.ByteBuffer

	// savepoints are the stack-nested savepoints created on this batch.
	// savepointSeq is a monotonically increasing, never reused generation
	// counter; undoRefCount counts how many live undo entries reference a
	// record, so a record still needed by an outer savepoint is never put
	// back to the record pool.
	savepoints   []*savepoint
	savepointSeq uint64
	undoRefCount map[*LogRecord]int
}

// SavepointToken is an opaque handle of a savepoint created by Savepoint.
// A token is bound to the concrete Batch instance and the generation that
// issued it. It becomes invalid after ReleaseSavepoint, after a RollbackTo
// of an earlier savepoint (later savepoints are invalidated), or after the
// batch has reached its terminal state (committed or rollbacked).
type SavepointToken struct {
	batch      *Batch
	id         uint64
	generation uint64
}

// undoEntry is an incremental undo record. It never deep-copies pendingWrites:
// for an in-place mutation it only keeps the before-image references of the
// affected LogRecord, and for a newly appended record it only remembers the
// hash bucket so the collision slot mapping can be restored.
type undoEntry struct {
	record  *LogRecord
	hashKey uint64
	// existed reports whether the record was already part of pendingWrites
	// when the change was recorded. When false, undo removes the record.
	existed bool
	// before-image of an existing record; the slice references are kept
	// instead of a pointer to the LogRecord that is mutated in place.
	key    []byte
	value  []byte
	typ    LogRecordType
	expire int64
}

// savepoint holds the incremental undo entries produced while it is the
// innermost active savepoint. pendingLen is the length of pendingWrites at
// creation time; every record appended afterwards belongs to the undo tail.
type savepoint struct {
	id         uint64
	pendingLen int
	undo       []*undoEntry
	// tracked dedups entries: only the first mutation of a record inside a
	// savepoint scope needs to carry the before-image.
	tracked map[*LogRecord]struct{}
}

// NewBatch creates a new Batch instance.
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() any {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() any {
	return &LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.pendingWritesMap = nil
	b.committed = false
	b.rollbacked = false
	// put all buffers back to the pool
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
	// discard every savepoint token and undo resource of the previous lifecycle
	b.savepoints = b.savepoints[:0]
	b.savepointSeq = 0
	b.undoRefCount = nil
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key-value pair to the batch for writing.
func (b *Batch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	record := b.lookupPendingWrites(key)
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		record = b.db.recordPool.Get().(*LogRecord)
		b.captureUndoBeforeAppend(key, record)
		b.appendPendingWrites(key, record)
	} else {
		b.captureUndoBeforeUpdate(record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.mu.Unlock()

	return nil
}

// PutWithTTL adds a key-value pair with ttl to the batch for writing.
func (b *Batch) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	record := b.lookupPendingWrites(key)
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		record = b.db.recordPool.Get().(*LogRecord)
		b.captureUndoBeforeAppend(key, record)
		b.appendPendingWrites(key, record)
	} else {
		b.captureUndoBeforeUpdate(record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()

	return nil
}

// Get retrieves the value associated with a given key from the batch.
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// get from pendingWrites
	b.mu.RLock()
	record := b.lookupPendingWrites(key)
	b.mu.RUnlock()

	// if the record is in pendingWrites, return the value directly
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}

	// get key/value from data file
	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	// check if the record is deleted or expired
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// only need key and type when deleting a value.
	var exist bool
	record := b.lookupPendingWrites(key)
	if record != nil {
		b.captureUndoBeforeUpdate(record)
		record.Type = LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}
	if !exist {
		record = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
		b.captureUndoBeforeAppend(key, record)
		b.appendPendingWrites(key, record)
	}
	b.mu.Unlock()

	return nil
}

// Exist checks if the key exists in the database.
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// check if the key exists in pendingWrites
	b.mu.RLock()
	record := b.lookupPendingWrites(key)
	b.mu.RUnlock()

	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	// check if the key exists in index
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}

	// check if the record is deleted or expired
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire sets the ttl of the key.
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	record := b.lookupPendingWrites(key)

	// if the key exists in pendingWrites, update the expiry time directly
	if record != nil {
		// return key not found if the record is deleted or expired
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		b.captureUndoBeforeUpdate(record)
		record.Expire = time.Now().Add(ttl).UnixNano()
		return nil
	}
	// if the key does not exist in pendingWrites, get the value from wal
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	now := time.Now()
	record = decodeLogRecord(chunk)
	// if the record is deleted or expired, we can assume that the key does not exist,
	// and delete the key from the index
	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return ErrKeyNotFound
	}
	// now we get the value from wal, update the expiry time
	// and rewrite the record to pendingWrites
	record.Expire = now.Add(ttl).UnixNano()
	b.captureUndoBeforeAppend(key, record)
	b.appendPendingWrites(key, record)

	return nil
}

// TTL returns the ttl of the key.
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	record := b.lookupPendingWrites(key)
	if record != nil {
		if record.Expire == 0 {
			return -1, nil
		}
		// return key not found if the record is deleted or expired
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, ErrKeyNotFound
		}
		// now we get the valid expiry time, we can calculate the ttl
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	// if the key does not exist in pendingWrites, get the value from wal
	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	// return key not found if the record is deleted or expired
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}

	// now we get the valid expiry time, we can calculate the ttl
	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

// Persist removes the ttl of the key.
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// if the key exists in pendingWrites, update the expiry time directly
	record := b.lookupPendingWrites(key)
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		b.captureUndoBeforeUpdate(record)
		record.Expire = 0
		return nil
	}

	// check if the key exists in index
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	record = decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	// check if the record is deleted or expired
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return ErrKeyNotFound
	}
	// if the expiration time is 0, it means that the key has no expiration time,
	// so we can return directly
	if record.Expire == 0 {
		return nil
	}

	// set the expiration time to 0, and rewrite the record to wal
	record.Expire = 0
	b.captureUndoBeforeAppend(key, record)
	b.appendPendingWrites(key, record)

	return nil
}

// Commit commits the batch, if the batch is readonly or empty, it will return directly.
//
// It will iterate the pendingWrites and write the data to the database,
// then write a record to indicate the end of the batch to guarantee atomicity.
// Finally, it will write the index.
func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		// invalidate every outstanding savepoint before leaving the batch
		b.clearSavepoints()
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed or rollbacked
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	// write to wal buffer
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	// write a record to indicate the end of the batch
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)

	// write to wal file
	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	// flush wal if necessary
	if b.options.Sync || b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// write to index
	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
		// put the record back to the pool
		b.db.recordPool.Put(record)
	}

	// terminal state: invalidate every savepoint token and reclaim all
	// undo resources; only the retained records above were written to wal.
	b.clearSavepoints()
	b.committed = true
	return nil
}

// Rollback discards an uncommitted batch instance.
// the discard operation will clear the buffered data and release the lock.
func (b *Batch) Rollback() error {
	defer b.unlock()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}

	if !b.options.ReadOnly {
		// clear pendingWrites
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
		for key := range b.pendingWritesMap {
			delete(b.pendingWritesMap, key)
		}
	}

	// terminal state: invalidate every savepoint token and reclaim undo resources
	b.clearSavepoints()
	b.rollbacked = true
	return nil
}

// lookupPendingWrites if the key exists in pendingWrites, update the value directly
func (b *Batch) lookupPendingWrites(key []byte) *LogRecord {
	if len(b.pendingWritesMap) == 0 {
		return nil
	}

	hashKey := utils.MemHash(key)
	for _, entry := range b.pendingWritesMap[hashKey] {
		if bytes.Equal(b.pendingWrites[entry].Key, key) {
			return b.pendingWrites[entry]
		}
	}
	return nil
}

// add new record to pendingWrites and pendingWritesMap.
func (b *Batch) appendPendingWrites(key []byte, record *LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
	if b.pendingWritesMap == nil {
		b.pendingWritesMap = make(map[uint64][]int)
	}
	hashKey := utils.MemHash(key)
	b.pendingWritesMap[hashKey] = append(b.pendingWritesMap[hashKey], len(b.pendingWrites)-1)
}

// captureUndoBeforeUpdate records the before-image of an existing pending
// record that is about to be mutated in place. Only the first mutation of a
// record inside the innermost savepoint scope is recorded; the captured slice
// references (not the mutable LogRecord pointer) are enough to restore the
// record's Type, Value, Expire and identity.
func (b *Batch) captureUndoBeforeUpdate(record *LogRecord) {
	if len(b.savepoints) == 0 {
		return
	}
	sp := b.savepoints[len(b.savepoints)-1]
	if _, ok := sp.tracked[record]; ok {
		return
	}
	sp.undo = append(sp.undo, &undoEntry{
		record:  record,
		hashKey: utils.MemHash(record.Key),
		existed: true,
		key:     record.Key,
		value:   record.Value,
		typ:     record.Type,
		expire:  record.Expire,
	})
	sp.tracked[record] = struct{}{}
	if b.undoRefCount == nil {
		b.undoRefCount = make(map[*LogRecord]int)
	}
	b.undoRefCount[record]++
}

// captureUndoBeforeAppend records that a record about to be appended to
// pendingWrites (a first Put, a Delete of an unknown key, or a record read
// from the wal by Expire/Persist) did not exist when the innermost savepoint
// was entered. Undo of such an entry removes the record and its hash slot.
func (b *Batch) captureUndoBeforeAppend(key []byte, record *LogRecord) {
	if len(b.savepoints) == 0 {
		return
	}
	sp := b.savepoints[len(b.savepoints)-1]
	if _, ok := sp.tracked[record]; ok {
		return
	}
	sp.undo = append(sp.undo, &undoEntry{
		record:  record,
		hashKey: utils.MemHash(key),
		existed: false,
	})
	sp.tracked[record] = struct{}{}
	if b.undoRefCount == nil {
		b.undoRefCount = make(map[*LogRecord]int)
	}
	b.undoRefCount[record]++
}

// Savepoint creates a stack-nested savepoint on a writable batch and returns
// a token bound to this batch instance and a fresh, never reused generation.
//
// Savepoint is incremental: it only records the length of pendingWrites and
// does not copy any existing pending record or value, so its cost does not
// grow with the number of pending records.
func (b *Batch) Savepoint() (SavepointToken, error) {
	if b.db.closed {
		return SavepointToken{}, ErrDBClosed
	}
	if b.options.ReadOnly {
		return SavepointToken{}, ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed {
		return SavepointToken{}, ErrBatchCommitted
	}
	if b.rollbacked {
		return SavepointToken{}, ErrBatchRollbacked
	}

	b.savepointSeq++
	sp := &savepoint{
		id:         b.savepointSeq,
		pendingLen: len(b.pendingWrites),
		tracked:    make(map[*LogRecord]struct{}),
	}
	b.savepoints = append(b.savepoints, sp)
	return SavepointToken{batch: b, id: sp.id, generation: sp.id}, nil
}

// resolveSavepoint validates a token before any state change. Tokens from
// another batch, released or invalidated generations, and tokens used after
// the batch reached its terminal state are all rejected here.
func (b *Batch) resolveSavepoint(token SavepointToken) (*savepoint, int, error) {
	if b.db.closed {
		return nil, 0, ErrDBClosed
	}
	if b.options.ReadOnly {
		return nil, 0, ErrReadOnlyBatch
	}
	if b.committed {
		return nil, 0, ErrBatchCommitted
	}
	if b.rollbacked {
		return nil, 0, ErrBatchRollbacked
	}
	if token.batch != b || token.id == 0 || token.generation != token.id {
		return nil, 0, ErrInvalidSavepoint
	}
	for i, sp := range b.savepoints {
		if sp.id == token.id {
			return sp, i, nil
		}
	}
	return nil, 0, ErrInvalidSavepoint
}

// RollbackTo undoes every change performed after the target savepoint and
// restores the state captured when that savepoint was created. The target
// savepoint is retained and stays usable, while every later savepoint is
// invalidated.
//
// The rollback is purely in-memory: it neither reads nor writes the wal,
// emits no watch event and does not release the db lock held by the batch.
func (b *Batch) RollbackTo(token SavepointToken) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	target, idx, err := b.resolveSavepoint(token)
	if err != nil {
		return err
	}
	restoreLen := target.pendingLen

	// 1. Apply undo from the innermost scope down to the target, so repeated
	//    overwrites converge on the target-time before-image. In-place
	//    changes restore Type/Value/Expire/identity; appended records are
	//    collected for removal and their hash buckets for slot repair.
	touchedHashes := make(map[uint64]struct{})
	var removedRecords []*LogRecord
	for s := len(b.savepoints) - 1; s >= idx; s-- {
		sp := b.savepoints[s]
		for j := len(sp.undo) - 1; j >= 0; j-- {
			entry := sp.undo[j]
			if entry.existed {
				entry.record.Key = entry.key
				entry.record.Value = entry.value
				entry.record.Type = entry.typ
				entry.record.Expire = entry.expire
			} else {
				touchedHashes[entry.hashKey] = struct{}{}
				removedRecords = append(removedRecords, entry.record)
			}
			b.undoRefCount[entry.record]--
			if b.undoRefCount[entry.record] == 0 {
				delete(b.undoRefCount, entry.record)
			}
		}
	}

	// 2. Repair the hash collision slots: every surviving record keeps an
	//    index below restoreLen, every removed record pointed at the tail.
	for hashKey := range touchedHashes {
		slots := b.pendingWritesMap[hashKey]
		kept := slots[:0]
		for _, slot := range slots {
			if slot < restoreLen {
				kept = append(kept, slot)
			}
		}
		if len(kept) == 0 {
			delete(b.pendingWritesMap, hashKey)
		} else {
			b.pendingWritesMap[hashKey] = kept
		}
	}

	// 3. Drop the undo tail of pendingWrites. All records appended after the
	//    target savepoint form exactly this tail, so the surviving records
	//    never move and their pointers stay valid.
	for i := restoreLen; i < len(b.pendingWrites); i++ {
		b.pendingWrites[i] = nil
	}
	b.pendingWrites = b.pendingWrites[:restoreLen]

	// 4. Return a removed record to the pool only when no surviving outer
	//    savepoint still holds undo data referencing it.
	for _, record := range removedRecords {
		if _, needed := b.undoRefCount[record]; !needed {
			b.db.recordPool.Put(record)
		}
	}

	// 5. Invalidate every younger savepoint; the target itself is retained
	//    with empty undo, matching the state of its creation moment.
	for s := len(b.savepoints) - 1; s > idx; s-- {
		b.savepoints[s] = nil
	}
	b.savepoints = b.savepoints[:idx+1]
	target.undo = target.undo[:0]
	target.tracked = make(map[*LogRecord]struct{})
	return nil
}

// ReleaseSavepoint releases the target savepoint and every later savepoint.
// The changes protected by them are accepted and stay in pendingWrites; only
// the undo data that no surviving outer savepoint needs is reclaimed.
// Records referenced by an outer savepoint are never returned to the pool
// here, so they cannot be reused while the outer rollback still needs them.
func (b *Batch) ReleaseSavepoint(token SavepointToken) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, idx, err := b.resolveSavepoint(token)
	if err != nil {
		return err
	}

	// Merge from the innermost scope outwards: an undo entry is transferred
	// to the nearest surviving savepoint unless that savepoint already holds
	// an older before-image of the same record.
	for s := len(b.savepoints) - 1; s >= idx; s-- {
		sp := b.savepoints[s]
		var outer *savepoint
		if s > 0 {
			outer = b.savepoints[s-1]
		}
		for _, entry := range sp.undo {
			if outer != nil {
				if _, ok := outer.tracked[entry.record]; ok {
					// the outer scope already restores this record, the
					// younger before-image is no longer needed
					b.dropUndoRef(entry.record)
					continue
				}
				outer.undo = append(outer.undo, entry)
				outer.tracked[entry.record] = struct{}{}
				continue
			}
			// no surviving outer savepoint: reclaim the undo data
			b.dropUndoRef(entry.record)
		}
		sp.undo = sp.undo[:0]
		sp.tracked = nil
		b.savepoints[s] = nil
	}
	b.savepoints = b.savepoints[:idx]
	return nil
}

// dropUndoRef releases one live undo reference and forgets the record once no
// savepoint needs it any more. Live pending records themselves are only
// returned to the pool at Commit/Rollback, never during savepoint release.
func (b *Batch) dropUndoRef(record *LogRecord) {
	b.undoRefCount[record]--
	if b.undoRefCount[record] == 0 {
		delete(b.undoRefCount, record)
	}
}

// clearSavepoints discards the whole savepoint stack and every undo resource.
// It is used when the batch reaches its terminal state or is recycled.
func (b *Batch) clearSavepoints() {
	for i := range b.savepoints {
		b.savepoints[i] = nil
	}
	b.savepoints = b.savepoints[:0]
	b.undoRefCount = nil
}
