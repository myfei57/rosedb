package rosedb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
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
	db            *DB
	pendingWrites []*LogRecord // ordered batch operations, range markers included
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool // whether the batch has been committed
	rollbacked    bool // whether the batch has been rollbacked
	batchId       *snowflake.Node
	buffers       []*bytebufferpool.ByteBuffer
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
	b.committed = false
	b.rollbacked = false
	// put all buffers back to the pool
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
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
	// Every operation is appended in call order so that a later Put can
	// revive a key that an earlier range delete (or point delete) removed:
	// the ordered log, not an in-place record replacement, is the source
	// of truth inside the batch.
	record := b.db.recordPool.Get().(*LogRecord)
	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.appendPendingWrite(record)
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
	// append a new record in call order, see Put for details.
	record := b.db.recordPool.Get().(*LogRecord)
	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.appendPendingWrite(record)
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
	// get from pendingWrites, range markers are honored in call order
	b.mu.RLock()
	record, deleted := b.pendingKeyState(key)
	b.mu.RUnlock()

	// the batch has an opinion on this key: a covering range marker or a
	// point delete hides it, otherwise the latest point record wins.
	if deleted {
		return nil, ErrKeyNotFound
	}
	if record != nil {
		if record.IsExpired(now) {
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
	// append a tombstone in call order. A later Put on the same key can
	// therefore revive it instead of being trapped before an earlier marker.
	record := b.db.recordPool.Get().(*LogRecord)
	record.Key, record.Value = key, nil
	record.Type, record.Expire = LogRecordDeleted, 0
	b.appendPendingWrite(record)
	b.mu.Unlock()

	return nil
}

// DeleteRange schedules the deletion of every key falling into the
// lexicographic half-open interval [startKey, endKey).
//
// It is expressed by a single range-delete marker in the WAL instead of one
// tombstone per affected key, so the log size does not grow with the number
// of keys that match the interval.
//
// Boundary conventions:
//   - A nil startKey means an unbounded lower bound (negative infinity).
//   - A nil endKey means an unbounded upper bound (positive infinity).
//   - A non-nil empty byte slice is a finite boundary (the smallest possible
//     key) and is never confused with an unbounded side.
//
// Reversed finite boundaries (startKey > endKey) are rejected with
// ErrInvalidRange before the batch is modified. An empty interval
// (startKey == endKey, both finite) is a no-op: it succeeds without
// appending any record and has no side effect at commit time.
//
// Within the batch, all later operations as well as Get/Exist/TTL observe the
// deletion in call order: writes performed before this call disappear when
// their key is covered, while a later Put on the same key revives it.
func (b *Batch) DeleteRange(startKey, endKey []byte) error {
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	startUnbounded := startKey == nil
	endUnbounded := endKey == nil

	// validate the boundaries before anything in the batch can change.
	if !startUnbounded && !endUnbounded {
		switch bytes.Compare(startKey, endKey) {
		case 0:
			// half-open [x, x) is empty: nothing to do, no side effect.
			return nil
		case 1:
			return ErrInvalidRange
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Copy the finite boundaries: callers may reuse the slices after the
	// call returns, while the marker must keep its own stable bytes.
	record := b.db.recordPool.Get().(*LogRecord)
	record.Type = LogRecordRangeDeleted
	record.BatchId = 0
	if startUnbounded {
		record.Key = nil
	} else {
		record.Key = append([]byte(nil), startKey...)
	}
	if endUnbounded {
		record.Value = nil
	} else {
		record.Value = append([]byte(nil), endKey...)
	}
	record.Expire = 0
	if startUnbounded {
		record.Expire |= rangeStartUnbounded
	}
	if endUnbounded {
		record.Expire |= rangeEndUnbounded
	}
	b.appendPendingWrite(record)

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
	// check the ordered batch operations, range markers included
	b.mu.RLock()
	record, deleted := b.pendingKeyState(key)
	b.mu.RUnlock()

	if deleted {
		return false, nil
	}
	if record != nil {
		return !record.IsExpired(now), nil
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

	record, deleted := b.pendingKeyState(key)

	// a covering range marker or a point tombstone means the key is gone
	// for the rest of this batch unless a later Put revives it.
	if deleted {
		return ErrKeyNotFound
	}
	// the key exists in the batch, update the expiry time directly
	if record != nil {
		if record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
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
	b.appendPendingWrite(record)

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

	record, deleted := b.pendingKeyState(key)
	// a covering range marker hides the key completely.
	if record == nil && deleted {
		return -1, ErrKeyNotFound
	}
	if record != nil {
		// keep the historical behavior for point tombstones: they carry no
		// expiry, so TTL reports no expiration instead of an error.
		if record.Type == LogRecordDeleted {
			return -1, nil
		}
		if record.Expire == 0 {
			return -1, nil
		}
		if record.IsExpired(now.UnixNano()) {
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

	record, deleted := b.pendingKeyState(key)
	// the key was removed by a range marker or a point tombstone.
	if deleted {
		return ErrKeyNotFound
	}
	// if the key exists in pendingWrites, update the expiry time directly
	if record != nil {
		if record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
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
	b.appendPendingWrite(record)

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
	// write to wal buffer, strictly in the batch operation order.
	// range-delete markers are single fixed-shape records regardless of how
	// many keys they affect, so no per-key list is ever written here.
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		// range markers already carry their boundaries and unbounded
		// flags, so they use the same frame encoder as every other record.
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

	// apply to the in-memory index in the exact batch operation order.
	// range markers only walk the actually affected index items here; they
	// never cause extra wal writes.
	for i, record := range b.pendingWrites {
		switch record.Type {
		case LogRecordRangeDeleted:
			affectedKeys := b.db.deleteIndexRange(record.Key, record.Value,
				record.StartUnbounded(), record.EndUnbounded())
			if b.db.options.WatchQueueSize > 0 {
				for _, affectedKey := range affectedKeys {
					b.db.watcher.putEvent(&Event{
						Key:     affectedKey,
						Action:  WatchActionDelete,
						BatchId: record.BatchId,
					})
				}
			}
		case LogRecordDeleted:
			b.db.index.Delete(record.Key)
			if b.db.options.WatchQueueSize > 0 {
				b.db.watcher.putEvent(&Event{
					Key: record.Key, Action: WatchActionDelete, BatchId: record.BatchId,
				})
			}
		default:
			if record.IsExpired(now) {
				b.db.index.Delete(record.Key)
			} else {
				b.db.index.Put(record.Key, chunkPositions[i])
			}

			if b.db.options.WatchQueueSize > 0 {
				e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
				e.Action = WatchActionPut
				b.db.watcher.putEvent(e)
			}
		}
		// put the record back to the pool
		b.db.recordPool.Put(record)
	}

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
		// clear pendingWrites, nothing has reached the wal or the live index yet
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
	}

	b.rollbacked = true
	return nil
}

// pendingKeyState folds the ordered batch operations (point records and
// range-delete markers) into the effective state of a single key.
//
// Returns:
//   - record: the latest point record (normal or deleted) that is still
//     effective for key, nil when no point record survives or the key has
//     never been touched by a point operation;
//   - deleted: true when key is currently removed by the batch, either by a
//     point tombstone or a covering range marker that no later point write
//     has overridden.
//
// Iteration follows the actual call order, so a Put after a covering range
// marker correctly revives the key instead of being shadowed by an older
// record located before the marker.
func (b *Batch) pendingKeyState(key []byte) (record *LogRecord, deleted bool) {
	for _, rec := range b.pendingWrites {
		switch rec.Type {
		case LogRecordRangeDeleted:
			if rec.RangeContains(key) {
				record = nil
				deleted = true
			}
		case LogRecordDeleted:
			if bytes.Equal(rec.Key, key) {
				record = rec
				deleted = true
			}
		default: // LogRecordNormal
			if bytes.Equal(rec.Key, key) {
				record = rec
				deleted = false
			}
		}
	}
	return record, deleted
}

// appendPendingWrite appends a new operation to the ordered batch log.
func (b *Batch) appendPendingWrite(record *LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
}
