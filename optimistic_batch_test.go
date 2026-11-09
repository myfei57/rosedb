package rosedb

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func openOptTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(DefaultOptions)
	assert.NoError(t, err)
	return db
}

// ---------------------------------------------------------------------------
// basic functionality
// ---------------------------------------------------------------------------

func TestOptimisticBatch_Basic(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob.Put([]byte("k1"), []byte("v1")))
	assert.NoError(t, ob.PutWithTTL([]byte("k2"), []byte("v2"), time.Hour))

	// overlay reads inside the batch
	v, err := ob.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v)
	assert.NoError(t, ob.Delete([]byte("k2")))
	_, err = ob.Get([]byte("k2"))
	assert.Equal(t, ErrKeyNotFound, err)

	assert.NoError(t, ob.Commit())

	v, err = db.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v)
	_, err = db.Get([]byte("k2"))
	assert.Equal(t, ErrKeyNotFound, err)

	// a finished batch cannot be reused
	assert.True(t, errors.Is(ob.Commit(), ErrBatchCommitted))
	assert.True(t, errors.Is(ob.Rollback(), ErrOptimisticBatchFinished))

	// data survives reopen
	dir := db.options.DirPath
	assert.NoError(t, db.Close())
	db2, err := Open(func() Options {
		o := DefaultOptions
		o.DirPath = dir
		return o
	}())
	assert.NoError(t, err)
	defer destroyDB(db2)
	v, err = db2.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v)
}

func TestOptimisticBatch_EmptyKeyAndClosed(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	assert.Equal(t, ErrKeyIsEmpty, ob.Put(nil, []byte("v")))
	assert.Equal(t, ErrKeyIsEmpty, ob.PutWithTTL(nil, []byte("v"), time.Second))
	assert.Equal(t, ErrKeyIsEmpty, ob.Delete(nil))
	assert.NoError(t, ob.Rollback())
	assert.True(t, errors.Is(ob.Put([]byte("k"), []byte("v")), ErrOptimisticBatchFinished))

	// read-only batch refuses writes
	ro := db.NewOptimisticBatch(BatchOptions{ReadOnly: true})
	_, err = ro.Get([]byte("missing"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Equal(t, ErrReadOnlyBatch, ro.Put([]byte("k"), []byte("v")))
	assert.Equal(t, ErrReadOnlyBatch, ro.Delete([]byte("k")))
	assert.NoError(t, ro.Commit())
}

// user slices are snapshotted on acceptance.
func TestOptimisticBatch_InputSlicesSnapshot(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	key := []byte("key")
	val := []byte("value")
	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob.Put(key, val))
	key[0] = 'X'
	val[0] = 'V'

	got, err := ob.Get([]byte("key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
	assert.NoError(t, ob.Commit())

	got, err = db.Get([]byte("key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
}

// repeatable reads: own overlay first, otherwise the first-observed stable
// content, regardless of later concurrent modifications.
func TestOptimisticBatch_StableReadsAndOverlay(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v0")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), got)

	// concurrent writer changes the physical value
	assert.NoError(t, db.Put([]byte("k"), []byte("v1")))

	got, err = ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), got, "first-observed content must stay stable")

	// own overlay takes precedence
	assert.NoError(t, ob.Put([]byte("k"), []byte("mine")))
	got, err = ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("mine"), got)

	// the underlying change is a conflict at commit
	assert.Equal(t, ErrOptimisticConflict, ob.Commit())
}

// ---------------------------------------------------------------------------
// barrier: two writers do not block each other during the business phase
// ---------------------------------------------------------------------------

func TestOptimisticBatch_BarrierTwoWriters(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v0")))

	ob1 := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob1.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), got)

	inBusiness := make(chan struct{})
	release := make(chan struct{})
	done1 := make(chan error, 1)
	go func() {
		close(inBusiness)
		<-release // parks here WITHOUT holding any DB lock
		_ = ob1.Put([]byte("k"), []byte("v1"))
		done1 <- ob1.Commit()
	}()

	<-inBusiness
	// let writer 1 sit in its business phase for a noticeable interval
	time.Sleep(200 * time.Millisecond)

	// writer 2 performs its entire business phase while writer 1 is parked;
	// it must not be blocked.
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	start := time.Now()
	_, err = ob2.Get([]byte("k"))
	assert.NoError(t, err)
	assert.NoError(t, ob2.Put([]byte("other"), []byte("x")))
	assert.Less(t, time.Since(start), 150*time.Millisecond,
		"business phase of writer 2 must not block on writer 1")

	close(release)
	assert.NoError(t, <-done1)

	// writer 2 observed k=v0 but writer 1 committed k=v1: conflict, and the
	// failure must be all-or-nothing, so "other" never appears.
	err = ob2.Commit()
	assert.Equal(t, ErrOptimisticConflict, err)
	_, err = db.Get([]byte("other"))
	assert.Equal(t, ErrKeyNotFound, err)

	got, err = db.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), got)
}

// ---------------------------------------------------------------------------
// conflicts
// ---------------------------------------------------------------------------

func TestOptimisticBatch_ReadWriteConflict(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v0")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("k"))
	assert.NoError(t, err)

	// unrelated plain write between read and commit
	assert.NoError(t, db.Put([]byte("k"), []byte("v1")))

	assert.NoError(t, ob.Put([]byte("side"), []byte("s")))
	err = ob.Commit()
	assert.Equal(t, ErrOptimisticConflict, err)

	// no staged key may be visible after a failed commit
	_, err = db.Get([]byte("side"))
	assert.Equal(t, ErrKeyNotFound, err)
	got, _ := db.Get([]byte("k"))
	assert.Equal(t, []byte("v1"), got)
}

func TestOptimisticBatch_WriteWriteConflict(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob1 := db.NewOptimisticBatch(DefaultBatchOptions)
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob1.Put([]byte("k"), []byte("a")))
	assert.NoError(t, ob2.Put([]byte("k"), []byte("b")))

	assert.NoError(t, ob1.Commit())
	err := ob2.Commit()
	assert.Equal(t, ErrOptimisticConflict, err)

	got, _ := db.Get([]byte("k"))
	assert.Equal(t, []byte("a"), got)
}

// a previously missing key is created and then deleted: its absence at
// commit time must NOT hide the conflict.
func TestOptimisticBatch_MissingABA(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("ghost"))
	assert.Equal(t, ErrKeyNotFound, err)

	assert.NoError(t, db.Put([]byte("ghost"), []byte("born")))
	assert.NoError(t, db.Delete([]byte("ghost")))

	assert.NoError(t, ob.Put([]byte("payload"), []byte("p")))
	err = ob.Commit()
	assert.Equal(t, ErrOptimisticConflict, err)
	_, err = db.Get([]byte("payload"))
	assert.Equal(t, ErrKeyNotFound, err)
	_, err = db.Get([]byte("ghost"))
	assert.Equal(t, ErrKeyNotFound, err)

	// delete generation metadata must be reclaimed once no batch observes it
	db.occ.mu.Lock()
	_, retained := db.occ.metas["ghost"]
	db.occ.mu.Unlock()
	assert.False(t, retained, "tombstone generation must not be retained forever")
}

// delete then recreate with the SAME value is still an ABA conflict.
func TestOptimisticBatch_DeleteRecreateSameValue(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("same")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("same"), got)

	assert.NoError(t, db.Delete([]byte("k")))
	assert.NoError(t, db.Put([]byte("k"), []byte("same")))

	err = ob.Commit()
	assert.Equal(t, ErrOptimisticConflict, err)
}

// rewriting the identical value still advances the logical version and
// conflicts (validation is not value based).
func TestOptimisticBatch_SameValueRewrite(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("k"))
	assert.NoError(t, err)

	assert.NoError(t, db.Put([]byte("k"), []byte("v")))

	err = ob.Commit() // read-only optimistic batch still validates
	assert.Equal(t, ErrOptimisticConflict, err)
}

// old-style Batch writes and single ops take part in conflict validation.
func TestOptimisticBatch_OldBatchParticipates(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v0")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("k"))
	assert.NoError(t, err)

	old := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, old.Put([]byte("k"), []byte("v1")))
	assert.NoError(t, old.Commit())

	assert.Equal(t, ErrOptimisticConflict, ob.Commit())

	// a clean optimistic commit against the new version succeeds
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob2.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), got)
	assert.NoError(t, ob2.Put([]byte("k2"), got))
	assert.NoError(t, ob2.Commit())
}

// ---------------------------------------------------------------------------
// expiry
// ---------------------------------------------------------------------------

// a valid record expiring while the batch is open is a visibility change.
func TestOptimisticBatch_ExpiryIsConflict(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.PutWithTTL([]byte("k"), []byte("v"), 300*time.Millisecond))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	time.Sleep(450 * time.Millisecond)
	assert.Equal(t, ErrOptimisticConflict, ob.Commit())
}

// expired records that are still in the physical index are observed absent.
func TestOptimisticBatch_ExpiredObservedAsAbsent(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.PutWithTTL([]byte("k"), []byte("v"), 200*time.Millisecond))
	time.Sleep(350 * time.Millisecond)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)
	// no concurrent writer: observing absence and creating the key is fine
	assert.NoError(t, ob.Put([]byte("k"), []byte("fresh")))
	assert.NoError(t, ob.Commit())
	got, err := db.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fresh"), got)
}

// TTL is computed at staging and committed as an absolute instant.
func TestOptimisticBatch_TTLAbsoluteFromStaging(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob.PutWithTTL([]byte("k"), []byte("v"), 2*time.Second))
	time.Sleep(500 * time.Millisecond)
	assert.NoError(t, ob.Commit())

	ttl, err := db.TTL([]byte("k"))
	assert.NoError(t, err)
	// remaining TTL must reflect the 500ms already elapsed since staging,
	// not a fresh 2s granted at commit time.
	assert.Less(t, ttl, 1900*time.Millisecond)
	assert.Greater(t, ttl, 1100*time.Millisecond)

	time.Sleep(1700 * time.Millisecond)
	_, err = db.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)
}

// staging a short-TTL write and committing after it expired behaves like a
// delete: nothing visible, delete generation reclaimed.
func TestOptimisticBatch_CommitAfterStagedTTLExpiry(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob.PutWithTTL([]byte("k"), []byte("v"), 150*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	assert.NoError(t, ob.Commit())

	_, err := db.Get([]byte("k"))
	assert.Equal(t, ErrKeyNotFound, err)
	db.occ.mu.Lock()
	_, retained := db.occ.metas["k"]
	db.occ.mu.Unlock()
	assert.False(t, retained)
}

// ---------------------------------------------------------------------------
// merge: physical relocation must not be a logical conflict
// ---------------------------------------------------------------------------

func TestOptimisticBatch_MergeNoConflict(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 4 * MB
	db, err := Open(opts)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 200; i++ {
		assert.NoError(t, db.Put(utils.GetTestKey(i), make([]byte, 4*KB)))
	}
	assert.NoError(t, db.Put([]byte("watch"), []byte("orig")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	got, err := ob.Get([]byte("watch"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("orig"), got)

	db.occ.mu.Lock()
	versionBefore := db.occ.metas["watch"].version
	db.occ.mu.Unlock()

	assert.NoError(t, db.Merge(true))

	db.occ.mu.Lock()
	versionAfter := db.occ.metas["watch"].version
	db.occ.mu.Unlock()
	assert.Equal(t, versionBefore, versionAfter, "merge must not advance logical versions")

	assert.NoError(t, ob.Put([]byte("new"), []byte("x")))
	assert.NoError(t, ob.Commit())

	got, err = db.Get([]byte("watch"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("orig"), got)
	got, err = db.Get([]byte("new"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("x"), got)
}

// ---------------------------------------------------------------------------
// WAL (append) failure: whole commit fails, nothing visible, refs released
// ---------------------------------------------------------------------------

func TestOptimisticBatch_AOFFailure(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	db, err := Open(opts)
	assert.NoError(t, err)
	defer destroyDB(db)

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	big := make([]byte, 256*KB)
	assert.NoError(t, ob.Put([]byte("big"), big))

	err = ob.Commit()
	assert.Error(t, err) // WAL rejects a record larger than the segment

	_, err = db.Get([]byte("big"))
	assert.Equal(t, ErrKeyNotFound, err)

	// observation references released, dead-and-gone meta reclaimed
	db.occ.mu.Lock()
	_, retained := db.occ.metas["big"]
	db.occ.mu.Unlock()
	assert.False(t, retained)

	// the failed batch is finished, and later batches still work
	assert.True(t, errors.Is(ob.Commit(), ErrBatchRollbacked))
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob2.Put([]byte("ok"), []byte("v")))
	assert.NoError(t, ob2.Commit())
	got, err := db.Get([]byte("ok"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
}

// ---------------------------------------------------------------------------
// rollback / close / instance identity
// ---------------------------------------------------------------------------

func TestOptimisticBatch_RollbackReleases(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("exist"), []byte("v")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("exist"))
	assert.NoError(t, err)
	_, err = ob.Get([]byte("missing"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.NoError(t, ob.Put([]byte("staged"), []byte("v")))
	assert.NoError(t, ob.Rollback())

	// nothing staged is visible
	_, err = db.Get([]byte("staged"))
	assert.Equal(t, ErrKeyNotFound, err)

	db.occ.mu.Lock()
	m := db.occ.metas["exist"]
	refs := -1
	if m != nil {
		refs = m.refs
	}
	_, missingKept := db.occ.metas["missing"]
	db.occ.mu.Unlock()
	assert.Equal(t, 0, refs, "observed refs must be dropped")
	assert.False(t, missingKept, "tombstone generation reclaimed after rollback")

	// db keeps working and new batches commit
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	assert.NoError(t, ob2.Put([]byte("after"), []byte("v")))
	assert.NoError(t, ob2.Commit())
}

func TestOptimisticBatch_CloseAndInstanceIdentity(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	assert.NoError(t, err)
	assert.NoError(t, db.Put([]byte("k"), []byte("v")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err = ob.Get([]byte("k"))
	assert.NoError(t, err)
	assert.NoError(t, ob.Put([]byte("k2"), []byte("v2")))

	oldInstance := db.instanceID
	assert.NoError(t, db.Close())

	// racing commit against Close: error and resources released, no panic
	err = ob.Commit()
	assert.Equal(t, ErrDBClosed, err)
	// rollback after close is a finished-batch error
	assert.True(t, errors.Is(ob.Rollback(), ErrOptimisticBatchFinished))

	// reopening produces a new instance identity
	db2, err := Open(func() Options {
		o := DefaultOptions
		o.DirPath = opts.DirPath
		return o
	}())
	assert.NoError(t, err)
	defer destroyDB(db2)
	assert.NotEqual(t, oldInstance, db2.instanceID)

	// a batch whose identity does not match the DB cannot commit
	ob2 := db2.NewOptimisticBatch(DefaultBatchOptions)
	ob2.instanceID = oldInstance
	assert.NoError(t, ob2.Put([]byte("x"), []byte("y")))
	assert.Equal(t, ErrOptimisticInstanceMismatch, ob2.Commit())
}

// hammer commits while closing to exercise the Close/Commit race under -race.
func TestOptimisticBatch_CloseCommitRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		db := openOptTestDB(t)
		_ = db.Put([]byte("k"), []byte("v"))

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					ob := db.NewOptimisticBatch(DefaultBatchOptions)
					_, _ = ob.Get([]byte("k"))
					_ = ob.Put([]byte("k"), []byte("v"))
					_ = ob.Commit()
				}
			}()
		}
		time.Sleep(10 * time.Millisecond)
		_ = db.Close()
		close(stop)
		wg.Wait()
		_ = os.RemoveAll(db.options.DirPath)
		_ = os.RemoveAll(mergeDirPath(db.options.DirPath))
	}
}

// ---------------------------------------------------------------------------
// serial point-transaction model: optimistic read-modify-write with retry
// produces exactly the serial result
// ---------------------------------------------------------------------------

func encodeCount(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeCount(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func TestOptimisticBatch_SerializableCounter(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("counter"), encodeCount(0)))

	const goroutines = 16
	const perG = 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				for {
					ob := db.NewOptimisticBatch(DefaultBatchOptions)
					cur, err := ob.Get([]byte("counter"))
					if err != nil {
						t.Errorf("get counter: %v", err)
						return
					}
					next := decodeCount(cur) + 1
					if err := ob.Put([]byte("counter"), encodeCount(next)); err != nil {
						t.Errorf("stage: %v", err)
						return
					}
					if err := ob.Commit(); err != nil {
						assert.Equal(t, ErrOptimisticConflict, err)
						continue
					}
					break
				}
			}
		}()
	}
	wg.Wait()

	v, err := db.Get([]byte("counter"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(goroutines*perG), decodeCount(v),
		"successful outcomes must match the serial point-transaction model")
}

// keys deleted through legacy write paths (single ops and old Batch) must not
// leave zero-reference tombstone metadata behind forever.
func TestOptimisticBatch_NoInfiniteTombstoneMap(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)

	for i := 0; i < 50; i++ {
		k := utils.GetTestKey(i)
		assert.NoError(t, db.Put(k, []byte("v")))
	}
	for i := 0; i < 25; i++ {
		assert.NoError(t, db.Delete(utils.GetTestKey(i)))
	}
	old := db.NewBatch(DefaultBatchOptions)
	for i := 25; i < 50; i++ {
		k := utils.GetTestKey(i)
		assert.NoError(t, old.Delete(k))
	}
	assert.NoError(t, old.Commit())

	db.occ.mu.Lock()
	n := len(db.occ.metas)
	db.occ.mu.Unlock()
	assert.Equal(t, 0, n, "deleted keys with no observers must not leave metadata")

	// an optimistic batch observing a missing key keeps the generation only
	// for the batch's lifetime
	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	db.occ.mu.Lock()
	assert.Equal(t, 1, len(db.occ.metas))
	db.occ.mu.Unlock()
	assert.NoError(t, ob.Rollback())
	db.occ.mu.Lock()
	assert.Equal(t, 0, len(db.occ.metas))
	db.occ.mu.Unlock()
}

// concurrent first-touch of the same key within one batch must balance
// references exactly: losers of the race release their extra references.
func TestOptimisticBatch_ConcurrentObserveSameKey(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("k"), []byte("v")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := ob.Get([]byte("k"))
			if err == nil {
				assert.Equal(t, []byte("v"), v)
			}
		}()
	}
	wg.Wait()
	assert.NoError(t, ob.Commit())

	db.occ.mu.Lock()
	m := db.occ.metas["k"]
	refs := 0
	if m != nil {
		refs = m.refs
	}
	db.occ.mu.Unlock()
	assert.Equal(t, 0, refs)

	// concurrent staging of distinct keys: all land in one atomic commit
	ob2 := db.NewOptimisticBatch(DefaultBatchOptions)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = ob2.Put(utils.GetTestKey(1000+i), []byte("v"))
		}(i)
	}
	wg.Wait()
	assert.NoError(t, ob2.Commit())
	for i := 0; i < 32; i++ {
		v, err := db.Get(utils.GetTestKey(1000 + i))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v"), v)
	}
}

// multiple staged keys fail as one atomic unit on a single-key conflict.
func TestOptimisticBatch_AtomicMultiKeyFailure(t *testing.T) {
	db := openOptTestDB(t)
	defer destroyDB(db)
	assert.NoError(t, db.Put([]byte("a"), []byte("0")))

	ob := db.NewOptimisticBatch(DefaultBatchOptions)
	_, err := ob.Get([]byte("a"))
	assert.NoError(t, err)

	// external change to "a"; meanwhile the batch stages several own keys
	assert.NoError(t, db.Put([]byte("a"), []byte("1")))
	assert.NoError(t, ob.Put([]byte("b"), []byte("2")))
	assert.NoError(t, ob.Put([]byte("c"), []byte("3")))

	assert.Equal(t, ErrOptimisticConflict, ob.Commit())
	for _, k := range []string{"b", "c"} {
		_, err := db.Get([]byte(k))
		assert.Equal(t, ErrKeyNotFound, err)
	}
}
