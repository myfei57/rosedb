package rosedb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBackupKeyNum = 2000
)

func testKey(i int) []byte  { return []byte(fmt.Sprintf("rosedb-backup-key-%06d", i)) }
func testVal(i int) []byte  { return []byte(fmt.Sprintf("rosedb-backup-val-%06d", i)) }
func testVal2(i int) []byte { return []byte(fmt.Sprintf("rosedb-backup-NEW-%06d", i)) }

func openBackupDir(t *testing.T, dirPath string) *DB {
	t.Helper()
	opts := DefaultOptions
	opts.DirPath = dirPath
	db, err := Open(opts)
	require.NoError(t, err)
	return db
}

func assertNoBackupTempLeft(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), backupTempPrefix),
			"backup staging directory left behind: %s", filepath.Join(parent, e.Name()))
	}
}

func readBackupManifest(t *testing.T, dirPath string) backupManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dirPath, backupFinName))
	require.NoError(t, err)
	var m backupManifest
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

func countDBKeys(t *testing.T, db *DB) int {
	t.Helper()
	n := 0
	db.Ascend(func(k, v []byte) (bool, error) {
		n++
		return true, nil
	})
	return n
}

// put the deterministic key set and return the number of keys written.
func putDeterministic(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(testKey(i), testVal(i)))
	}
}

func assertKeySet(t *testing.T, db *DB, n int, valFn func(int) []byte) {
	t.Helper()
	for i := 0; i < n; i++ {
		v, err := db.Get(testKey(i))
		if valFn == nil {
			require.ErrorIs(t, err, ErrKeyNotFound, "key %d should be absent", i)
			continue
		}
		require.NoError(t, err, "key %d should exist", i)
		assert.Equal(t, valFn(i), v)
	}
}

func assertLeaseReleased(t *testing.T, db *DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&db.activeBackups) == 0
	}, time.Second*3, time.Millisecond*10, "backup lease was not released")
}

func segmentIDsInDir(t *testing.T, dir string, suffix string) []uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var ids []uint32
	for _, e := range entries {
		var id uint32
		if _, err := fmt.Sscanf(e.Name(), "%d"+suffix, &id); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// blockingFirstFileHook returns hooks that block before the first file is
// copied until release is closed. ctx cancellation unblocks with ctx.Err().
func blockingFirstFileHook(ctx context.Context) (hooks backupHooks, fired, release chan struct{}) {
	fired = make(chan struct{})
	release = make(chan struct{})
	var once sync.Once
	hooks.beforeFile = func(name, srcPath, dstPath string) error {
		once.Do(func() { close(fired) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return hooks, fired, release
}

func TestDB_Backup_Basic(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, testBackupKeyNum)

	require.NoError(t, db.BackupTo(context.Background(), backupPath, true))
	assertLeaseReleased(t, db)

	// completion marker and manifest are published.
	st, err := os.Stat(filepath.Join(backupPath, backupFinName))
	require.NoError(t, err)
	assert.Greater(t, st.Size(), int64(0))
	_, err = os.Stat(filepath.Join(backupPath, backupManifestName))
	require.NoError(t, err)

	// flock and the merge working directory must never be copied.
	_, err = os.Stat(filepath.Join(backupPath, fileLockName))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(mergeDirPath(backupPath))
	assert.True(t, os.IsNotExist(err))

	// only captured data segments are present; nothing beyond the boundary.
	manifest := readBackupManifest(t, backupPath)
	assert.Equal(t, uint32(1), manifest.BoundarySegmentID)
	backupSegIDs := segmentIDsInDir(t, backupPath, dataFileNameSuffix)
	require.NotEmpty(t, backupSegIDs)
	assert.Equal(t, uint32(manifest.BoundarySegmentID), backupSegIDs[len(backupSegIDs)-1])
	for _, e := range manifest.Files {
		info, err := os.Stat(filepath.Join(backupPath, e.Name))
		require.NoError(t, err)
		assert.Equal(t, e.Size, info.Size())
	}

	// no hint/mergefin without a merge; an empty hint is recreated on open.
	assert.Equal(t, uint32(0), manifest.MergeFinSegmentID)
	_, err = os.Stat(wal.SegmentFileName(backupPath, hintFileNameSuffix, 1))
	assert.True(t, os.IsNotExist(err))

	// source is still usable.
	require.NoError(t, db.Put(testKey(testBackupKeyNum), testVal(testBackupKeyNum)))

	// backup opens directly and contains exactly the captured data.
	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	assert.Equal(t, testBackupKeyNum, countDBKeys(t, bak))
	assertKeySet(t, bak, testBackupKeyNum, testVal)
	_, err = bak.Get(testKey(testBackupKeyNum))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// source contains the post-boundary write.
	assert.Equal(t, testBackupKeyNum+1, countDBKeys(t, db))

	assertNoBackupTempLeft(t, filepath.Dir(backupPath))
	assertNoBackupTempLeft(t, filepath.Dir(srcDir))
}

func TestDB_Backup_Empty(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	require.NoError(t, db.BackupTo(context.Background(), backupPath, false))

	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	assert.Equal(t, 0, countDBKeys(t, bak))
	require.NoError(t, bak.Put([]byte("after-restore"), []byte("1")))
}

func TestDB_Backup_AfterMerge(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 1000)
	// delete some keys and overwrite some before merging.
	for i := 0; i < 200; i++ {
		require.NoError(t, db.Delete(testKey(i)))
	}
	for i := 200; i < 400; i++ {
		require.NoError(t, db.Put(testKey(i), testVal2(i)))
	}
	require.NoError(t, db.Merge(true))
	// post-merge writes land in newer segments; keys 0..9 come back to life.
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(testKey(i), testVal(i)))
	}

	require.NoError(t, db.BackupTo(context.Background(), backupPath, true))
	assertLeaseReleased(t, db)

	// hint and merge-finished files of the pinned generation are present.
	manifest := readBackupManifest(t, backupPath)
	assert.NotEqual(t, uint32(0), manifest.MergeFinSegmentID)
	_, err = os.Stat(wal.SegmentFileName(backupPath, hintFileNameSuffix, 1))
	require.NoError(t, err)
	_, err = os.Stat(wal.SegmentFileName(backupPath, mergeFinNameSuffix, 1))
	require.NoError(t, err)

	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	assert.Equal(t, 810, countDBKeys(t, bak))
	// keys 10..199 stay deleted; overwritten values come from the merged/new
	// files and keys 0..9 are resurrected by the post-merge writes.
	for i := 10; i < 200; i++ {
		_, err := bak.Get(testKey(i))
		assert.ErrorIs(t, err, ErrKeyNotFound, "key %d should be absent", i)
	}
	for i := 0; i < 10; i++ {
		v, err := bak.Get(testKey(i))
		require.NoError(t, err)
		assert.Equal(t, testVal(i), v)
	}
	for i := 200; i < 400; i++ {
		v, err := bak.Get(testKey(i))
		require.NoError(t, err)
		assert.Equal(t, testVal2(i), v)
	}
	for i := 400; i < 1000; i++ {
		v, err := bak.Get(testKey(i))
		require.NoError(t, err)
		assert.Equal(t, testVal(i), v)
	}
}

// The key concurrency test: block copying, overwrite/delete and merge during
// the barrier, verify writes complete, source stays usable and the backup
// contains only the captured generation.
func TestDB_Backup_ConcurrentWritesMergeAndSecondMerge(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	const n = 50000
	putDeterministic(t, db, n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooks, fired, release := blockingFirstFileHook(ctx)

	backupDone := make(chan error, 1)
	go func() {
		backupDone <- db.backupTo(ctx, backupPath, backupOptions{sync: true, hooks: hooks})
	}()

	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("backup copy barrier was never reached")
	}

	// merge runs concurrently while copying is paused.
	mergeDone := make(chan error, 1)
	go func() { mergeDone <- db.Merge(true) }()
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&db.mergeRunning) == 1
	}, 5*time.Second, 2*time.Millisecond)

	// Put/Delete must complete while the large-file copy is paused: the
	// global db lock must not be held.
	writeDone := make(chan error, 3)
	go func() { writeDone <- db.Put(testKey(0), testVal2(0)) }()
	go func() { writeDone <- db.Delete(testKey(1)) }()
	go func() { writeDone <- db.Put(testKey(n), testVal(n)) }()
	for i := 0; i < 3; i++ {
		select {
		case werr := <-writeDone:
			require.NoError(t, werr)
		case <-time.After(10 * time.Second):
			t.Fatal("Put/Delete blocked while backup copy was paused")
		}
	}

	// release the barrier; backup and the merge both finish.
	close(release)
	select {
	case berr := <-backupDone:
		require.NoError(t, berr)
	case <-time.After(30 * time.Second):
		t.Fatal("backup did not finish")
	}
	select {
	case merr := <-mergeDone:
		require.NoError(t, merr)
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent merge did not finish")
	}
	assertLeaseReleased(t, db)

	// a second (and again destructive) merge must also succeed and must not
	// have affected the already finished backup.
	require.NoError(t, db.Merge(true))

	// the backup reflects exactly the captured point: key0 old value,
	// key1 present, post-boundary key absent.
	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	assert.Equal(t, n, countDBKeys(t, bak))
	v, err := bak.Get(testKey(0))
	require.NoError(t, err)
	assert.Equal(t, testVal(0), v)
	v, err = bak.Get(testKey(1))
	require.NoError(t, err)
	assert.Equal(t, testVal(1), v)
	_, err = bak.Get(testKey(n))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// source reflects the post-boundary operations and remains fully usable.
	v, err = db.Get(testKey(0))
	require.NoError(t, err)
	assert.Equal(t, testVal2(0), v)
	require.NoError(t, db.Delete(testKey(1)))
	_, err = db.Get(testKey(1))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	v, err = db.Get(testKey(n))
	require.NoError(t, err)
	assert.Equal(t, testVal(n), v)
	require.NoError(t, db.Put([]byte("still-usable"), []byte("1")))

	// backup contains no segment beyond the captured boundary even though
	// writes and merges happened afterwards.
	manifest := readBackupManifest(t, backupPath)
	for _, segID := range segmentIDsInDir(t, backupPath, dataFileNameSuffix) {
		assert.LessOrEqual(t, segID, manifest.BoundarySegmentID)
	}

	// the backup can also be reopened again after the source kept changing.
	require.NoError(t, bak.Close())
	bak2 := openBackupDir(t, backupPath)
	defer func() { _ = bak2.Close() }()
	assert.Equal(t, n, countDBKeys(t, bak2))
}

// Continuous writer without merge: post-boundary commits never enter the
// backup, unfinished batches never become visible.
func TestDB_Backup_ContinuousWriter(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 500)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooks, fired, release := blockingFirstFileHook(ctx)

	backupDone := make(chan error, 1)
	go func() {
		backupDone <- db.backupTo(ctx, backupPath, backupOptions{sync: false, hooks: hooks})
	}()
	<-fired

	var writerErr atomic.Value
	var stop int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 500
		for atomic.LoadInt32(&stop) == 0 {
			if err := db.Put(testKey(i), testVal(i)); err != nil {
				writerErr.Store(err)
				return
			}
			i++
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(release)
	require.NoError(t, <-backupDone)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	assert.Nil(t, writerErr.Load())

	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	bakN := countDBKeys(t, bak)
	srcN := countDBKeys(t, db)
	assert.Greater(t, srcN, bakN, "post-boundary writes must not enter the backup")
	assert.GreaterOrEqual(t, bakN, 500)
	// every backup key exists in the source with the same value.
	bak.Ascend(func(k, v []byte) (bool, error) {
		srcV, err := db.Get(k)
		require.NoError(t, err)
		assert.Equal(t, v, srcV)
		return true, nil
	})
}

func TestDB_Backup_Cancel(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	parent := t.TempDir()
	backupPath := filepath.Join(parent, "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 100)

	ctx, cancel := context.WithCancel(context.Background())
	hooks, fired, release := blockingFirstFileHook(ctx)
	defer close(release)

	done := make(chan error, 1)
	go func() {
		done <- db.backupTo(ctx, backupPath, backupOptions{sync: true, hooks: hooks})
	}()
	<-fired
	require.Equal(t, int64(1), atomic.LoadInt64(&db.activeBackups))

	cancel()
	select {
	case berr := <-done:
		assert.ErrorIs(t, berr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled backup did not return")
	}

	// failure/cancel cleanup: target absent, no staging left, lease released.
	_, err = os.Stat(backupPath)
	assert.True(t, os.IsNotExist(err))
	assertNoBackupTempLeft(t, parent)
	assertLeaseReleased(t, db)

	// the database is still usable and can be backed up again.
	require.NoError(t, db.Put([]byte("after-cancel"), []byte("1")))
	require.NoError(t, db.BackupTo(context.Background(), filepath.Join(parent, "backup2"), true))
}

func TestDB_Backup_MidCopyFailure(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	parent := t.TempDir()
	sentinel := filepath.Join(parent, "sibling-sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0644))
	backupPath := filepath.Join(parent, "backup")

	opts := DefaultOptions
	opts.DirPath = srcDir
	opts.SegmentSize = 512 * KB
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	// enough data to span several data segments, so the failure happens
	// after some (but not all) pinned files have been copied.
	putDeterministic(t, db, 20000)

	injected := errors.New("injected backup io failure")
	var calls int
	var mu sync.Mutex
	hooks := backupHooks{
		beforeFile: func(name, srcPath, dstPath string) error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 2 {
				return injected
			}
			return nil
		},
	}
	err = db.backupTo(context.Background(), backupPath, backupOptions{sync: true, hooks: hooks})
	assert.ErrorIs(t, err, injected)

	_, err = os.Stat(backupPath)
	assert.True(t, os.IsNotExist(err))
	assertNoBackupTempLeft(t, parent)
	assertLeaseReleased(t, db)
	// pre-existing content in the parent is untouched.
	data, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	assert.Equal(t, "keep", string(data))
	// source is still usable.
	require.NoError(t, db.Put([]byte("after-failure"), []byte("1")))
}

func TestDB_Backup_FileSizeMismatch(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	parent := t.TempDir()
	backupPath := filepath.Join(parent, "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 100)

	hooks := backupHooks{
		// corrupt the first staged file before verification.
		afterFile: func(name, srcPath, dstPath string) error {
			if strings.HasSuffix(name, dataFileNameSuffix) {
				return os.Truncate(dstPath, 4)
			}
			return nil
		},
	}
	err = db.backupTo(context.Background(), backupPath, backupOptions{sync: false, hooks: hooks})
	assert.ErrorIs(t, err, ErrBackupIncomplete)

	_, err = os.Stat(backupPath)
	assert.True(t, os.IsNotExist(err))
	assertNoBackupTempLeft(t, parent)
	assertLeaseReleased(t, db)
}

func TestDB_Backup_SyncFailure(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	parent := t.TempDir()
	backupPath := filepath.Join(parent, "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 10)

	syncErr := errors.New("injected fsync failure")
	hooks := backupHooks{
		syncFile: func(f *os.File) error { return syncErr },
	}
	err = db.backupTo(context.Background(), backupPath, backupOptions{sync: true, hooks: hooks})
	assert.ErrorIs(t, err, syncErr)

	_, err = os.Stat(backupPath)
	assert.True(t, os.IsNotExist(err))
	assertNoBackupTempLeft(t, parent)
	assertLeaseReleased(t, db)

	// sync=false does not invoke fsync and succeeds.
	require.NoError(t, db.BackupTo(context.Background(), filepath.Join(parent, "backup-nosync"), false))
}

func TestDB_Backup_SourceCloseWaitsForCancel(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	parent := t.TempDir()
	backupPath := filepath.Join(parent, "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)

	putDeterministic(t, db, 100)

	ctx, cancel := context.WithCancel(context.Background())
	hooks, fired, release := blockingFirstFileHook(ctx)
	defer close(release)

	backupDone := make(chan error, 1)
	go func() {
		backupDone <- db.backupTo(ctx, backupPath, backupOptions{sync: true, hooks: hooks})
	}()
	<-fired

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()

	// Close must not complete while the backup lease is held.
	select {
	case cerr := <-closeDone:
		t.Fatalf("Close returned while backup is in flight: %v", cerr)
	case <-time.After(300 * time.Millisecond):
	}

	// once the backup is canceled it releases the lease; Close proceeds and
	// only then are source files closed.
	cancel()
	select {
	case berr := <-backupDone:
		assert.ErrorIs(t, berr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled backup did not return")
	}
	select {
	case cerr := <-closeDone:
		assert.NoError(t, cerr)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after backup released its lease")
	}
	assertLeaseReleased(t, db)
	_, err = os.Stat(backupPath)
	assert.True(t, os.IsNotExist(err))
	assertNoBackupTempLeft(t, parent)

	// the source can be reopened immediately (lock was released).
	db2, err := Open(opts)
	require.NoError(t, err)
	v, err := db2.Get(testKey(0))
	require.NoError(t, err)
	assert.Equal(t, testVal(0), v)
	destroyDB(db2)
}

func TestDB_Backup_TargetConflicts(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "db")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))

	// target is the source directory.
	assert.ErrorIs(t, db.BackupTo(context.Background(), srcDir, false), ErrBackupPathConflict)
	// target is inside the source directory.
	assert.ErrorIs(t, db.BackupTo(context.Background(), filepath.Join(srcDir, "sub"), false), ErrBackupPathConflict)
	// source is inside the target directory.
	assert.ErrorIs(t, db.BackupTo(context.Background(), root, false), ErrBackupPathConflict)
	// target resolves to the merge working directory or a directory inside it.
	assert.ErrorIs(t, db.BackupTo(context.Background(), mergeDirPath(srcDir), false), ErrBackupPathConflict)
	assert.ErrorIs(t, db.BackupTo(context.Background(),
		filepath.Join(mergeDirPath(srcDir), "sub"), false), ErrBackupPathConflict)

	// non-existing target with missing parent.
	assert.ErrorIs(t, db.BackupTo(context.Background(),
		filepath.Join(root, "no-such-parent", "backup"), false), ErrBackupParentMissing)

	// target occupied by a file.
	fileTarget := filepath.Join(root, "a-file")
	require.NoError(t, os.WriteFile(fileTarget, []byte("x"), 0644))
	assert.ErrorIs(t, db.BackupTo(context.Background(), fileTarget, false), ErrBackupTargetNotDir)

	// non-empty target directory: error and its content untouched.
	nonEmpty := filepath.Join(root, "non-empty")
	require.NoError(t, os.MkdirAll(filepath.Join(nonEmpty, "child"), 0755))
	sentinel := filepath.Join(nonEmpty, "child", "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0644))
	assert.ErrorIs(t, db.BackupTo(context.Background(), nonEmpty, false), ErrBackupTargetNotEmpty)
	data, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	assert.Equal(t, "keep", string(data))
	assertNoBackupTempLeft(t, root)

	// an existing EMPTY target is accepted.
	emptyTarget := filepath.Join(root, "empty")
	require.NoError(t, os.MkdirAll(emptyTarget, 0755))
	require.NoError(t, db.BackupTo(context.Background(), emptyTarget, false))
	bak := openBackupDir(t, emptyTarget)
	assert.Equal(t, 1, countDBKeys(t, bak))
	_ = bak.Close()

	// a second backup into the now non-empty target is rejected without
	// removing or overwriting the first backup.
	assert.ErrorIs(t, db.BackupTo(context.Background(), emptyTarget, false), ErrBackupTargetNotEmpty)
	_, err = os.Stat(filepath.Join(emptyTarget, backupFinName))
	require.NoError(t, err)
}

func TestDB_Backup_TTLNotRetimed(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	require.NoError(t, db.PutWithTTL([]byte("ttl-key"), []byte("v"), 1500*time.Millisecond))
	require.NoError(t, db.Put([]byte("keep-key"), []byte("v")))

	// raw log bytes are copied, so the absolute expire timestamp is kept.
	require.NoError(t, db.BackupTo(context.Background(), backupPath, true))

	bak := openBackupDir(t, backupPath)
	v, err := bak.Get([]byte("ttl-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	_ = bak.Close()

	time.Sleep(1700 * time.Millisecond)
	bak2 := openBackupDir(t, backupPath)
	defer func() { _ = bak2.Close() }()
	_, err = bak2.Get([]byte("ttl-key"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	v, err = bak2.Get([]byte("keep-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

func TestDB_Backup_ContextCanceledBeforeStart(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.BackupTo(ctx, filepath.Join(t.TempDir(), "backup"), true)
	assert.ErrorIs(t, err, context.Canceled)
	assertLeaseReleased(t, db)
}

func TestDB_Backup_MergeRunningWaits(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	backupPath := filepath.Join(t.TempDir(), "backup")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	putDeterministic(t, db, 20000)

	mergeDone := make(chan error, 1)
	go func() { mergeDone <- db.Merge(true) }()
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&db.mergeRunning) == 1
	}, 5*time.Second, 2*time.Millisecond)

	// backup started during a merge waits for it, then captures the new
	// generation and reopens with exactly the merged content.
	backupDone := make(chan error, 1)
	go func() { backupDone <- db.BackupTo(context.Background(), backupPath, true) }()

	require.NoError(t, <-mergeDone)
	require.NoError(t, <-backupDone)
	assertLeaseReleased(t, db)

	bak := openBackupDir(t, backupPath)
	defer func() { _ = bak.Close() }()
	assert.Equal(t, 20000, countDBKeys(t, bak))
	assertKeySet(t, bak, 20000, testVal)
}

func TestDB_Backup_EmptyPathRejected(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "db")
	opts := DefaultOptions
	opts.DirPath = srcDir
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	assert.Error(t, db.BackupTo(context.Background(), "  ", false))
}
