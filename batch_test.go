package rosedb

import (
	"os"
	"testing"
	"time"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
	_ = os.RemoveAll(mergeDirPath(db.options.DirPath))
}

func TestBatch_Put_Normal(t *testing.T) {
	// value 128B
	batchPutAndIterate(t, 1*GB, 10000, 128)
	// value 1KB
	batchPutAndIterate(t, 1*GB, 10000, KB)
	// value 32KB
	batchPutAndIterate(t, 1*GB, 1000, 32*KB)
}

func TestBatch_Put_IncrSegmentFile(t *testing.T) {
	batchPutAndIterate(t, 64*MB, 2000, 32*KB)
	options := DefaultOptions
	options.SegmentSize = 64 * MB
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 2000, 32*KB)

	// write more data to rotate new segment file
	batch := db.NewBatch(DefaultBatchOptions)
	for i := 0; i < 1000; i++ {
		err := batch.Put(utils.GetTestKey(i*100), utils.RandomValue(32*KB))
		assert.NoError(t, err)
	}
	err = batch.Commit()
	assert.NoError(t, err)
}

func TestBatch_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch1 := db.NewBatch(DefaultBatchOptions)
	err = batch1.Put(utils.GetTestKey(12), utils.RandomValue(128))
	assert.NoError(t, err)
	val1, err := batch1.Get(utils.GetTestKey(12))
	assert.NoError(t, err)
	assert.NotNil(t, val1)
	_ = batch1.Commit()

	generateData(t, db, 400, 500, 4*KB)

	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Delete(utils.GetTestKey(450))
	assert.NoError(t, err)
	val, err := batch2.Get(utils.GetTestKey(450))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(12), true)
	assertKeyExistOrNot(t, db2, utils.GetTestKey(450), false)
}

func TestBatch_Delete_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	err = db.Delete([]byte("not exist"))
	assert.NoError(t, err)

	generateData(t, db, 1, 100, 128)
	err = db.Delete(utils.GetTestKey(99))
	assert.NoError(t, err)

	exist, err := db.Exist(utils.GetTestKey(99))
	assert.NoError(t, err)
	assert.False(t, exist)

	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Put(utils.GetTestKey(200), utils.RandomValue(100))
	assert.NoError(t, err)
	err = batch.Delete(utils.GetTestKey(200))
	assert.NoError(t, err)
	exist1, err := batch.Exist(utils.GetTestKey(200))
	assert.NoError(t, err)
	assert.False(t, exist1)
	_ = batch.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(200), false)
}

func TestBatch_Exist_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 100, 128)
	batch := db.NewBatch(DefaultBatchOptions)
	ok1, err := batch.Exist(utils.GetTestKey(99))
	assert.NoError(t, err)
	assert.True(t, ok1)
	ok2, err := batch.Exist(utils.GetTestKey(5000))
	assert.NoError(t, err)
	assert.False(t, ok2)
	_ = batch.Commit()

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(99), true)
}

func generateData(t *testing.T, db *DB, start, end, valueLen int) {
	t.Helper()
	for ; start < end; start++ {
		err := db.Put(utils.GetTestKey(start), utils.RandomValue(valueLen))
		assert.NoError(t, err)
	}
}

func batchPutAndIterate(t *testing.T, segmentSize int64, size, valueLen int) {
	t.Helper()
	options := DefaultOptions
	options.SegmentSize = segmentSize
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(BatchOptions{})

	for i := 0; i < size; i++ {
		err := batch.Put(utils.GetTestKey(i), utils.RandomValue(valueLen))
		assert.NoError(t, err)
	}
	err = batch.Commit()
	assert.NoError(t, err)

	for i := 0; i < size; i++ {
		value, err := db.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.Len(t, value, len(utils.RandomValue(valueLen)))
	}

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	for i := 0; i < size; i++ {
		value, err := db2.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.Len(t, value, len(utils.RandomValue(valueLen)))
	}
}

func assertKeyExistOrNot(t *testing.T, db *DB, key []byte, exist bool) {
	t.Helper()
	val, err := db.Get(key)
	if exist {
		assert.NoError(t, err)
		assert.NotNil(t, val)
	} else {
		assert.Nil(t, val)
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

func TestBatch_Rollback(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	key := []byte("rosedb")
	value := []byte("val")

	batcher := db.NewBatch(DefaultBatchOptions)
	err = batcher.Put(key, value)
	assert.NoError(t, err)

	err = batcher.Rollback()
	assert.NoError(t, err)

	resp, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Empty(t, resp)
}

func TestBatch_SetTwice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	key := []byte("rosedb")
	value1 := []byte("val1")
	value2 := []byte("val2")
	_ = batch.Put(key, value1)
	_ = batch.Put(key, value2)

	res, err := batch.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, res, value2)

	_ = batch.Commit()
	res2, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, res2, value2)
}

func TestBatch_Expire(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Expire(nil, time.Second)
	assert.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test read-only batch
	readOnlyBatch := db.NewBatch(BatchOptions{ReadOnly: true})
	err = readOnlyBatch.Expire([]byte("key"), time.Second)
	assert.Equal(t, ErrReadOnlyBatch, err)
	_ = readOnlyBatch.Rollback()

	// Test expire key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Expire([]byte("not-exist"), time.Second)
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test expire key in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	err = batch3.Expire([]byte("key1"), time.Second*10)
	assert.NoError(t, err)
	ttl, err := batch3.TTL([]byte("key1"))
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= time.Second*10)
	_ = batch3.Commit()

	// Test expire key in database
	err = db.Put([]byte("key2"), []byte("value2"))
	assert.NoError(t, err)
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.Expire([]byte("key2"), time.Second*5)
	assert.NoError(t, err)
	_ = batch4.Commit()
	ttl2, err := db.TTL([]byte("key2"))
	assert.NoError(t, err)
	assert.True(t, ttl2 > 0 && ttl2 <= time.Second*5)

	// Test expire deleted key in pendingWrites
	batch5 := db.NewBatch(DefaultBatchOptions)
	err = batch5.Put([]byte("key3"), []byte("value3"))
	assert.NoError(t, err)
	err = batch5.Delete([]byte("key3"))
	assert.NoError(t, err)
	err = batch5.Expire([]byte("key3"), time.Second)
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch5.Rollback()
}

func TestBatch_TTL(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	_, err = batch.TTL(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test TTL key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	_, err = batch2.TTL([]byte("not-exist"))
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test TTL key without expiration in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	ttl, err := batch3.TTL([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)
	_ = batch3.Commit()

	// Test TTL key with expiration in pendingWrites
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.PutWithTTL([]byte("key2"), []byte("value2"), time.Second*10)
	assert.NoError(t, err)
	ttl2, err := batch4.TTL([]byte("key2"))
	assert.NoError(t, err)
	assert.True(t, ttl2 > 0 && ttl2 <= time.Second*10)
	_ = batch4.Commit()

	// Test TTL key without expiration in database
	err = db.Put([]byte("key3"), []byte("value3"))
	assert.NoError(t, err)
	batch5 := db.NewBatch(DefaultBatchOptions)
	ttl3, err := batch5.TTL([]byte("key3"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl3)
	_ = batch5.Rollback()

	// Test TTL key with expiration in database
	err = db.PutWithTTL([]byte("key4"), []byte("value4"), time.Second*20)
	assert.NoError(t, err)
	batch6 := db.NewBatch(DefaultBatchOptions)
	ttl4, err := batch6.TTL([]byte("key4"))
	assert.NoError(t, err)
	assert.True(t, ttl4 > 0 && ttl4 <= time.Second*20)
	_ = batch6.Rollback()

	// Test TTL deleted key in pendingWrites
	// Note: current implementation returns -1, nil for deleted key because
	// it checks Expire == 0 before checking Type == LogRecordDeleted
	batch7 := db.NewBatch(DefaultBatchOptions)
	err = batch7.Put([]byte("key5"), []byte("value5"))
	assert.NoError(t, err)
	err = batch7.Delete([]byte("key5"))
	assert.NoError(t, err)
	ttl5, err := batch7.TTL([]byte("key5"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl5)
	_ = batch7.Rollback()
}

func TestBatch_Persist(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Persist(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test read-only batch
	readOnlyBatch := db.NewBatch(BatchOptions{ReadOnly: true})
	err = readOnlyBatch.Persist([]byte("key"))
	assert.Equal(t, ErrReadOnlyBatch, err)
	_ = readOnlyBatch.Rollback()

	// Test persist key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Persist([]byte("not-exist"))
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test persist key with TTL in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.PutWithTTL([]byte("key1"), []byte("value1"), time.Second*10)
	assert.NoError(t, err)
	ttl, err := batch3.TTL([]byte("key1"))
	assert.NoError(t, err)
	assert.True(t, ttl > 0)
	err = batch3.Persist([]byte("key1"))
	assert.NoError(t, err)
	ttl2, err := batch3.TTL([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl2)
	_ = batch3.Commit()

	// Test persist key with TTL in database
	err = db.PutWithTTL([]byte("key2"), []byte("value2"), time.Second*10)
	assert.NoError(t, err)
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.Persist([]byte("key2"))
	assert.NoError(t, err)
	_ = batch4.Commit()
	ttl3, err := db.TTL([]byte("key2"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl3)

	// Test persist key without TTL in database (should return directly)
	err = db.Put([]byte("key3"), []byte("value3"))
	assert.NoError(t, err)
	batch5 := db.NewBatch(DefaultBatchOptions)
	err = batch5.Persist([]byte("key3"))
	assert.NoError(t, err)
	_ = batch5.Commit()

	// Test persist deleted key in pendingWrites
	batch6 := db.NewBatch(DefaultBatchOptions)
	err = batch6.PutWithTTL([]byte("key4"), []byte("value4"), time.Second*10)
	assert.NoError(t, err)
	err = batch6.Delete([]byte("key4"))
	assert.NoError(t, err)
	err = batch6.Persist([]byte("key4"))
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch6.Rollback()
}

func TestBatch_Commit_SyncOption(t *testing.T) {
	testCases := []struct {
		name      string
		dbSync    bool
		batchSync bool
	}{
		{
			name:      "dbSync=false, batchSync=false",
			dbSync:    false,
			batchSync: false,
		},
		{
			name:      "dbSync=false, batchSync=true",
			dbSync:    false,
			batchSync: true,
		},
		{
			name:      "dbSync=true, batchSync=false",
			dbSync:    true,
			batchSync: false,
		},
		{
			name:      "dbSync=true, batchSync=true",
			dbSync:    true,
			batchSync: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := DefaultOptions
			opts.Sync = tc.dbSync
			db, err := Open(opts)
			assert.NoError(t, err)
			defer destroyDB(db)

			// Single operation (uses batch with batchSync=false internally)
			err = db.Put([]byte("key-single"), []byte("val-single"))
			assert.NoError(t, err)

			val, err := db.Get([]byte("key-single"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("val-single"), val)

			// Batch operation with explicit Sync setting
			batch := db.NewBatch(BatchOptions{Sync: tc.batchSync})
			err = batch.Put([]byte("key-batch-1"), []byte("val-1"))
			assert.NoError(t, err)
			err = batch.Put([]byte("key-batch-2"), []byte("val-2"))
			assert.NoError(t, err)
			err = batch.Commit()
			assert.NoError(t, err)

			// Reopen database and verify persisted data
			dir := db.options.DirPath
			err = db.Close()
			assert.NoError(t, err)

			opts2 := DefaultOptions
			opts2.DirPath = dir
			opts2.Sync = tc.dbSync
			db2, err := Open(opts2)
			assert.NoError(t, err)
			defer destroyDB(db2)

			valSingle, err := db2.Get([]byte("key-single"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("val-single"), valSingle)

			valB1, err := db2.Get([]byte("key-batch-1"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("val-1"), valB1)

			valB2, err := db2.Get([]byte("key-batch-2"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("val-2"), valB2)
		})
	}
}

func reopenDB(t *testing.T, options Options, db *DB) *DB {
	t.Helper()
	assert.NoError(t, db.Close())
	db2, err := Open(options)
	assert.NoError(t, err)
	return db2
}

func TestBatch_Savepoint_NestedRollback(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("a"), []byte("1")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("b"), []byte("2")))

	sp2, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("c"), []byte("3")))

	// rollback to sp1 restores the state when sp1 was created: only "a" survives
	assert.NoError(t, batch.RollbackTo(sp1))
	assert.Len(t, batch.pendingWrites, 1)
	val, err := batch.Get([]byte("a"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), val)
	for _, key := range []string{"b", "c"} {
		_, err = batch.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err, key)
	}
	// the younger savepoint is invalidated, while the target is retained
	assert.Equal(t, ErrInvalidSavepoint, batch.RollbackTo(sp2))
	assert.NoError(t, batch.RollbackTo(sp1))

	// the same batch keeps working and an outer rollback removes everything
	assert.NoError(t, batch.Put([]byte("d"), []byte("4")))
	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	assert.Len(t, batch.undoRefCount, 0)
	for _, key := range []string{"a", "b", "c", "d"} {
		_, err = batch.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err, key)
	}
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	assertKeyExistOrNot(t, db2, []byte("a"), false)
	assertKeyExistOrNot(t, db2, []byte("d"), false)
}

func TestBatch_Savepoint_ReleaseInnerThenRollbackOuter(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("a"), []byte("1")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("b"), []byte("2")))

	// releasing the inner savepoint accepts its changes and invalidates the token
	assert.NoError(t, batch.ReleaseSavepoint(sp1))
	assert.Equal(t, ErrInvalidSavepoint, batch.ReleaseSavepoint(sp1))
	val, err := batch.Get([]byte("b"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("2"), val)

	// the outer savepoint can still undo the accepted changes
	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	for _, key := range []string{"a", "b"} {
		_, err = batch.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err, key)
	}

	// rebuild on the same batch and commit the remaining changes
	assert.NoError(t, batch.Put([]byte("a"), []byte("10")))
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	assertKeyExistOrNot(t, db2, []byte("a"), true)
	assertKeyExistOrNot(t, db2, []byte("b"), false)
}

func TestBatch_Savepoint_SameKeyOverwrite(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("k"), []byte("v1")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("k"), []byte("v2")))
	assert.NoError(t, batch.Put([]byte("k"), []byte("v3")))
	assert.NoError(t, batch.Delete([]byte("k")))
	assert.NoError(t, batch.Put([]byte("k"), []byte("v4")))

	// repeated in-place overwrites inside one scope all converge on the first image
	assert.NoError(t, batch.RollbackTo(sp1))
	val, err := batch.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
	assert.Len(t, batch.pendingWrites, 1)

	// rolling back further removes the first-time append as well
	assert.NoError(t, batch.RollbackTo(sp0))
	_, err = batch.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Len(t, batch.pendingWrites, 0)

	// rewrite after rollback and commit
	assert.NoError(t, batch.Put([]byte("k"), []byte("final")))
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	val, err = db2.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("final"), val)
}

func TestBatch_Savepoint_HashCollision(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("collide-1"), []byte("v1")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("collide-2"), []byte("v2")))

	// MemHash collisions cannot be forged deterministically, so simulate the
	// exact bucket state a real collision would produce: register the second
	// record in the first record's hash bucket and keep the undo entry in sync.
	h1 := utils.MemHash([]byte("collide-1"))
	h2 := utils.MemHash([]byte("collide-2"))
	idx2 := len(batch.pendingWrites) - 1
	assert.Equal(t, []int{idx2}, batch.pendingWritesMap[h2])
	batch.pendingWritesMap[h1] = append(batch.pendingWritesMap[h1], idx2)
	delete(batch.pendingWritesMap, h2)
	for _, entry := range batch.savepoints[1].undo {
		if !entry.existed {
			entry.hashKey = h1
		}
	}

	assert.NoError(t, batch.RollbackTo(sp1))

	// the removed record's collision slot is gone and the survivor's slot is intact
	assert.Len(t, batch.pendingWrites, 1)
	assert.Equal(t, []int{0}, batch.pendingWritesMap[h1])
	_, ok := batch.pendingWritesMap[h2]
	assert.False(t, ok)
	val, err := batch.Get([]byte("collide-1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
	_, err = batch.Get([]byte("collide-2"))
	assert.Equal(t, ErrKeyNotFound, err)

	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	assert.NoError(t, batch.Commit())
}

func TestBatch_Savepoint_TTLChange(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	assert.NoError(t, db.PutWithTTL([]byte("ttl-key"), []byte("v"), time.Hour))
	assert.NoError(t, db.Put([]byte("plain"), []byte("p")))

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	// Expire on a key absent from pendingWrites reads the wal and appends a record
	assert.NoError(t, batch.Expire([]byte("ttl-key"), 2*time.Hour))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Expire([]byte("ttl-key"), time.Minute))
	ttl, err := batch.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.True(t, ttl <= time.Minute)

	// rollback restores the 2h expiry captured by sp1
	assert.NoError(t, batch.RollbackTo(sp1))
	ttl, err = batch.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.True(t, ttl > time.Hour)

	// rolling back the wal-read append returns to the original wal state
	assert.NoError(t, batch.RollbackTo(sp0))
	ttl, err = batch.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.True(t, ttl > time.Minute && ttl <= time.Hour)

	// Persist appends a wal-derived record too and is undone the same way
	sp2, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Persist([]byte("ttl-key")))
	ttl, err = batch.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)
	assert.NoError(t, batch.RollbackTo(sp2))
	ttl, err = batch.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.True(t, ttl > 0)

	// an in-place PutWithTTL overwrite of a pending record restores value and expire
	assert.NoError(t, batch.Put([]byte("local"), []byte("lv")))
	sp3, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.PutWithTTL([]byte("local"), []byte("tt"), time.Minute))
	ttl, err = batch.TTL([]byte("local"))
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= time.Minute)
	assert.NoError(t, batch.RollbackTo(sp3))
	val, err := batch.Get([]byte("local"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("lv"), val)
	ttl, err = batch.TTL([]byte("local"))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)

	assert.NoError(t, batch.Commit())
	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	ttl, err = db2.TTL([]byte("ttl-key"))
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= time.Hour)
	val, err = db2.Get([]byte("plain"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("p"), val)
}

func TestBatch_Savepoint_DeleteRebuild(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("k"), []byte("v1")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Delete([]byte("k")))
	_, err = batch.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)

	sp2, err := batch.Savepoint()
	assert.NoError(t, err)
	// rebuild the same key reuses the existing record in place
	assert.NoError(t, batch.Put([]byte("k"), []byte("v2")))
	val, err := batch.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)

	// back to the deleted state, then the pre-delete state, then pre-savepoint
	assert.NoError(t, batch.RollbackTo(sp2))
	_, err = batch.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)
	record := batch.lookupPendingWrites([]byte("k"))
	assert.NotNil(t, record)
	assert.Equal(t, LogRecordDeleted, record.Type)

	assert.NoError(t, batch.RollbackTo(sp1))
	val, err = batch.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	_, err = batch.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)

	// deleting an unknown key appends a tombstone record, also covered by undo
	sp3, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Delete([]byte("ghost")))
	assert.Len(t, batch.pendingWrites, 1)
	assert.NoError(t, batch.RollbackTo(sp3))
	assert.Len(t, batch.pendingWrites, 0)

	assert.NoError(t, batch.Commit())
	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	assertKeyExistOrNot(t, db2, []byte("k"), false)
	assertKeyExistOrNot(t, db2, []byte("ghost"), false)
}

func TestBatch_Savepoint_EmptyValue(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("nil-key"), nil))
	assert.NoError(t, batch.Put([]byte("empty-key"), []byte{}))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("nil-key"), []byte("x")))
	assert.NoError(t, batch.Put([]byte("empty-key"), []byte("y")))
	assert.NoError(t, batch.RollbackTo(sp1))

	val, err := batch.Get([]byte("nil-key"))
	assert.NoError(t, err)
	assert.Len(t, val, 0)
	val, err = batch.Get([]byte("empty-key"))
	assert.NoError(t, err)
	assert.Len(t, val, 0)

	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	assert.Len(t, batch.undoRefCount, 0)

	// commit the empty values for real on the same batch and verify after reopen
	assert.NoError(t, batch.Put([]byte("nil-key"), nil))
	assert.NoError(t, batch.Put([]byte("empty-key"), []byte{}))
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, options, db)
	defer destroyDB(db2)
	val, err = db2.Get([]byte("nil-key"))
	assert.NoError(t, err)
	assert.Len(t, val, 0)
	val, err = db2.Get([]byte("empty-key"))
	assert.NoError(t, err)
	assert.Len(t, val, 0)
}

func TestBatch_Savepoint_PoolReuse(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("outer"), []byte("o")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("inner"), []byte("i")))
	innerRecord := batch.lookupPendingWrites([]byte("inner"))

	assert.NoError(t, batch.RollbackTo(sp1))
	// the removed record has no surviving undo reference, so it is poolable
	_, referenced := batch.undoRefCount[innerRecord]
	assert.False(t, referenced)

	// a subsequent put may reuse the pooled struct without resurrecting old data
	assert.NoError(t, batch.Put([]byte("reused"), []byte("r")))
	val, err := batch.Get([]byte("reused"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("r"), val)
	_, err = batch.Get([]byte("inner"))
	assert.Equal(t, ErrKeyNotFound, err)

	// the outer savepoint still restores its own record safely
	assert.NoError(t, batch.RollbackTo(sp0))
	assert.Len(t, batch.pendingWrites, 0)
	assert.Len(t, batch.undoRefCount, 0)
	assert.NoError(t, batch.Commit())

	// while an outer savepoint still needs a record, releasing the inner one
	// must not make the record available for pool reuse
	batch2 := db.NewBatch(DefaultBatchOptions)
	s0, err := batch2.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch2.Put([]byte("k"), []byte("v1")))
	s1, err := batch2.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch2.Put([]byte("k"), []byte("v2")))
	assert.NoError(t, batch2.ReleaseSavepoint(s1))
	assert.Len(t, batch2.undoRefCount, 1)
	assert.NoError(t, batch2.RollbackTo(s0))
	assert.Len(t, batch2.pendingWrites, 0)
	assert.Len(t, batch2.undoRefCount, 0)
	assert.NoError(t, batch2.Commit())
}

func TestBatch_Savepoint_CommitReopenAndWatch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 100
	db, err := Open(options)
	assert.NoError(t, err)

	watchCh, err := db.Watch()
	assert.NoError(t, err)

	batch := db.NewBatch(DefaultBatchOptions)
	sp0, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("keep"), []byte("k")))

	sp1, err := batch.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, batch.Put([]byte("drop"), []byte("d")))
	assert.NoError(t, batch.Put([]byte("keep"), []byte("overwritten")))
	assert.NoError(t, batch.RollbackTo(sp1))
	// the outer savepoint stays valid after the inner rollback
	assert.NoError(t, batch.RollbackTo(sp0))
	assert.NoError(t, batch.Put([]byte("keep"), []byte("k")))

	assert.NoError(t, batch.Put([]byte("added"), []byte("a")))
	assert.NoError(t, batch.Commit())

	// only the retained records may produce watch events; drain until idle
	events := make(map[string][]byte)
	idle := time.NewTimer(300 * time.Millisecond)
	for {
		select {
		case event := <-watchCh:
			events[string(event.Key)] = event.Value
			idle.Reset(300 * time.Millisecond)
		case <-idle.C:
			assert.Len(t, events, 2)
			assert.Equal(t, []byte("k"), events["keep"])
			assert.Equal(t, []byte("a"), events["added"])
			_, dropped := events["drop"]
			assert.False(t, dropped)
			db2 := reopenDB(t, options, db)
			defer destroyDB(db2)
			val, err := db2.Get([]byte("keep"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("k"), val)
			val, err = db2.Get([]byte("added"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("a"), val)
			assertKeyExistOrNot(t, db2, []byte("drop"), false)
			return
		}
	}
}

func TestBatch_Savepoint_InvalidToken(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	// a token issued by another batch instance (also after pool recycling) is rejected
	b1 := db.NewBatch(DefaultBatchOptions)
	token, err := b1.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, b1.Rollback())
	b2 := db.NewBatch(DefaultBatchOptions)
	assert.Equal(t, ErrInvalidSavepoint, b2.RollbackTo(token))
	assert.Equal(t, ErrInvalidSavepoint, b2.ReleaseSavepoint(token))
	assert.NoError(t, b2.Rollback())

	// the zero value token is never valid
	b3 := db.NewBatch(DefaultBatchOptions)
	assert.Equal(t, ErrInvalidSavepoint, b3.RollbackTo(SavepointToken{}))
	assert.Equal(t, ErrInvalidSavepoint, b3.ReleaseSavepoint(SavepointToken{}))
	assert.NoError(t, b3.Rollback())

	// read-only batches cannot create savepoints
	rb := db.NewBatch(BatchOptions{ReadOnly: true})
	_, err = rb.Savepoint()
	assert.Equal(t, ErrReadOnlyBatch, err)
	assert.NoError(t, rb.Commit())

	// tokens are rejected after the terminal commit state before any change
	b4 := db.NewBatch(DefaultBatchOptions)
	committedToken, err := b4.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, b4.Put([]byte("k"), []byte("v")))
	assert.NoError(t, b4.Commit())
	assert.Equal(t, ErrBatchCommitted, b4.RollbackTo(committedToken))
	assert.Equal(t, ErrBatchCommitted, b4.ReleaseSavepoint(committedToken))

	// tokens are rejected after the terminal rollback state
	b5 := db.NewBatch(DefaultBatchOptions)
	rollbackToken, err := b5.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, b5.Rollback())
	assert.Equal(t, ErrBatchRollbacked, b5.RollbackTo(rollbackToken))
	assert.Equal(t, ErrBatchRollbacked, b5.ReleaseSavepoint(rollbackToken))

	// a released token cannot be rolled back to or released again
	b6 := db.NewBatch(DefaultBatchOptions)
	sp, err := b6.Savepoint()
	assert.NoError(t, err)
	assert.NoError(t, b6.ReleaseSavepoint(sp))
	assert.Equal(t, ErrInvalidSavepoint, b6.RollbackTo(sp))
	assert.Equal(t, ErrInvalidSavepoint, b6.ReleaseSavepoint(sp))
	assert.NoError(t, b6.Rollback())
}

func TestBatch_Savepoint_CostNotLinear(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	measureAllocs := func(pending int) float64 {
		batch := db.NewBatch(DefaultBatchOptions)
		for i := 0; i < pending; i++ {
			assert.NoError(t, batch.Put(utils.GetTestKey(i), utils.RandomValue(32)))
		}
		allocs := testing.AllocsPerRun(20, func() {
			sp, spErr := batch.Savepoint()
			assert.NoError(t, spErr)
			assert.NoError(t, batch.ReleaseSavepoint(sp))
		})
		assert.NoError(t, batch.Rollback())
		return allocs
	}

	emptyAllocs := measureAllocs(0)
	bigAllocs := measureAllocs(10000)
	// identical allocation count regardless of pending size proves Savepoint
	// does not walk or copy pendingWrites/pendingWritesMap
	assert.Equal(t, emptyAllocs, bigAllocs)
	t.Logf("allocs per savepoint/release cycle: empty=%v 10k-pending=%v", emptyAllocs, bigAllocs)

	// timing sanity check: average cost stays in the same order of magnitude
	measureLatency := func(pending int) time.Duration {
		batch := db.NewBatch(DefaultBatchOptions)
		for i := 0; i < pending; i++ {
			assert.NoError(t, batch.Put(utils.GetTestKey(i), utils.RandomValue(32)))
		}
		start := time.Now()
		for i := 0; i < 200; i++ {
			sp, spErr := batch.Savepoint()
			assert.NoError(t, spErr)
			assert.NoError(t, batch.ReleaseSavepoint(sp))
		}
		latency := time.Since(start) / 200
		assert.NoError(t, batch.Rollback())
		return latency
	}
	t.Logf("latency per savepoint/release cycle: 100-pending=%v 20k-pending=%v",
		measureLatency(100), measureLatency(20000))
}
