package rosedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
)

// reopenDB closes the database and reopens it with the same options.
func reopenDB(t *testing.T, db *DB) *DB {
	t.Helper()
	dir := db.options.DirPath
	opts := db.options
	require.NoError(t, db.Close())
	db2, err := Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db2.Close()
		_ = os.RemoveAll(dir)
		_ = os.RemoveAll(mergeDirPath(dir))
	})
	return db2
}

func rangeKey(i int) []byte {
	return []byte(fmt.Sprintf("range-key-%03d", i))
}

// ---------------------------------------------------------------------------
// Put -> range delete -> Put on the same key
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_PutRangePutSameKey(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 10; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("v0")))
	}

	batch := db.NewBatch(DefaultBatchOptions)
	// write inside the batch first, then delete the range, then revive one key.
	assert.NoError(t, batch.Put(rangeKey(10), []byte("v10")))
	assert.NoError(t, batch.DeleteRange(rangeKey(2), rangeKey(8)))

	// keys inside the half-open interval are hidden, boundaries are honored.
	_, err = batch.Get(rangeKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	_, err = batch.Get(rangeKey(7))
	assert.Equal(t, ErrKeyNotFound, err)
	exist, err := batch.Exist(rangeKey(5))
	assert.NoError(t, err)
	assert.False(t, exist)
	// end key is exclusive, start key is inclusive.
	v, err := batch.Get(rangeKey(8))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), v)
	v, err = batch.Get(rangeKey(1))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), v)
	// the in-batch write before the marker is covered as well.
	_, err = batch.Get(rangeKey(10))
	assert.NoError(t, err)

	// revive a covered key with a later Put: it must not stay trapped before
	// the range marker.
	assert.NoError(t, batch.Put(rangeKey(5), []byte("revived")))
	v, err = batch.Get(rangeKey(5))
	assert.NoError(t, err)
	assert.Equal(t, []byte("revived"), v)
	exist, err = batch.Exist(rangeKey(5))
	assert.NoError(t, err)
	assert.True(t, exist)

	assert.NoError(t, batch.Commit())

	// verify live index state after commit.
	_, err = db.Get(rangeKey(3))
	assert.Equal(t, ErrKeyNotFound, err)
	v, err = db.Get(rangeKey(5))
	assert.NoError(t, err)
	assert.Equal(t, []byte("revived"), v)
	v, err = db.Get(rangeKey(8))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), v)

	// recovery must produce the same result.
	db2 := reopenDB(t, db)
	_, err = db2.Get(rangeKey(3))
	assert.Equal(t, ErrKeyNotFound, err)
	v, err = db2.Get(rangeKey(5))
	assert.NoError(t, err)
	assert.Equal(t, []byte("revived"), v)
	v, err = db2.Get(rangeKey(8))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v0"), v)
	v, err = db2.Get(rangeKey(10))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v10"), v)
	// survivors: committed 0,1,8,9, the in-batch write 10 (outside the
	// interval) and the revived key 5.
	assert.Equal(t, 6, db2.Stat().KeysNum)
}

// ---------------------------------------------------------------------------
// Persist / Expire after a range delete
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_PersistExpireAfter(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 5; i++ {
		assert.NoError(t, db.PutWithTTL(rangeKey(i), []byte("v"), time.Hour))
	}

	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(0), rangeKey(5)))

	// ttl operations on range-covered keys fail like missing keys.
	assert.Equal(t, ErrKeyNotFound, batch.Expire(rangeKey(2), time.Minute))
	assert.Equal(t, ErrKeyNotFound, batch.Persist(rangeKey(2)))
	_, err = batch.TTL(rangeKey(2))
	assert.Equal(t, ErrKeyNotFound, err)

	// a later Put revives the key and ttl operations work again.
	assert.NoError(t, batch.PutWithTTL(rangeKey(2), []byte("v2"), time.Hour))
	ttl, err := batch.TTL(rangeKey(2))
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= time.Hour)

	assert.NoError(t, batch.Expire(rangeKey(2), time.Minute))
	ttl, err = batch.TTL(rangeKey(2))
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= time.Minute)

	assert.NoError(t, batch.Persist(rangeKey(2)))
	ttl, err = batch.TTL(rangeKey(2))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)

	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, db)
	ttl, err = db2.TTL(rangeKey(2))
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)
	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}
		_, err = db2.Get(rangeKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

// ---------------------------------------------------------------------------
// Duplicate and overlapping ranges
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_Overlapping(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 20; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("v")))
	}

	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(2), rangeKey(10)))
	// overlapping and duplicate ranges are allowed.
	assert.NoError(t, batch.DeleteRange(rangeKey(5), rangeKey(15)))
	assert.NoError(t, batch.DeleteRange(rangeKey(2), rangeKey(10)))
	// revive a key inside the union of ranges.
	assert.NoError(t, batch.Put(rangeKey(7), []byte("alive")))
	// delete again after the revive, overlapping the prior markers.
	assert.NoError(t, batch.DeleteRange(rangeKey(7), rangeKey(8)))
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, db)
	for i := 0; i < 20; i++ {
		_, err = db2.Get(rangeKey(i))
		if i >= 2 && i < 15 {
			assert.Equal(t, ErrKeyNotFound, err, "key %d should be deleted", i)
		} else {
			assert.NoError(t, err, "key %d should survive", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Unbounded sides, empty byte boundary and 0xFF edges
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_UnboundedAndBinaryBoundaries(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	// keys covering the whole lexicographic space (the empty key is not
	// insertable through Put, so it only appears as a finite boundary).
	keys := [][]byte{
		{0x00},                   //
		{0x00, 0xff},             //
		[]byte("a"),              //
		[]byte("b"),              //
		{0x7f},                   //
		{0xff},                   //
		{0xff, 0x00},             //
		{0xff, 0xff},             //
		{0xff, 0xff, 0xff, 0xff}, //
	}
	for _, k := range keys {
		assert.NoError(t, db.Put(k, []byte("v")))
	}

	// finite empty lower bound must not be treated as unbounded:
	// [{}, {0x00}) contains no insertable key, nothing is removed and the
	// {0x00} key (excluded by the half-open upper bound) survives.
	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange([]byte{}, []byte{0x00}))
	assert.NoError(t, batch.Commit())
	assert.Equal(t, len(keys), db.Stat().KeysNum)
	v, err := db.Get([]byte{0x00})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), v)

	// unbounded upper bound: everything >= "b" disappears, including 0xFF keys.
	batch = db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange([]byte("b"), nil))
	assert.NoError(t, batch.Commit())
	for _, k := range keys[4:] {
		_, err = db.Get(k)
		assert.Equal(t, ErrKeyNotFound, err, "key %x should be deleted", k)
	}
	v, err = db.Get([]byte("a"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), v)

	// unbounded lower bound: everything strictly smaller than {0x7f} disappears.
	batch = db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(nil, []byte{0x7f}))
	assert.NoError(t, batch.Commit())
	for _, k := range [][]byte{{0x00}, {0x00, 0xff}, []byte("a"), []byte("b")} {
		_, err = db.Get(k)
		assert.Equal(t, ErrKeyNotFound, err, "key %x should be deleted", k)
	}

	// fully unbounded range removes the remainder and revives nothing old.
	batch = db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(nil, nil))
	assert.NoError(t, batch.Commit())
	assert.Equal(t, 0, db.Stat().KeysNum)

	// the distinction survives recovery.
	db2 := reopenDB(t, db)
	assert.Equal(t, 0, db2.Stat().KeysNum)
}

// ---------------------------------------------------------------------------
// Invalid, reversed and empty intervals; read-only rejection; rollback
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_InvalidAndEmpty(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	assert.NoError(t, db.Put(rangeKey(1), []byte("v")))

	// read only batch cannot delete ranges.
	ro := db.NewBatch(BatchOptions{ReadOnly: true})
	assert.Equal(t, ErrReadOnlyBatch, ro.DeleteRange(rangeKey(0), rangeKey(2)))
	assert.NoError(t, ro.Rollback())

	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.Put(rangeKey(2), []byte("v2")))

	// reversed boundaries are rejected before mutating the batch.
	assert.Equal(t, ErrInvalidRange, batch.DeleteRange(rangeKey(9), rangeKey(0)))
	assert.Len(t, batch.pendingWrites, 1)

	// empty interval [x, x) is a no-op with no record appended.
	assert.NoError(t, batch.DeleteRange(rangeKey(5), rangeKey(5)))
	assert.Len(t, batch.pendingWrites, 1)
	assert.NoError(t, batch.Commit())

	v, err := db.Get(rangeKey(2))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)

	// rollback of a range marker must not touch the live index.
	batch2 := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch2.DeleteRange(nil, nil))
	assert.NoError(t, batch2.Rollback())
	v, err = db.Get(rangeKey(1))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), v)

	db2 := reopenDB(t, db)
	v, err = db2.Get(rangeKey(1))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

// ---------------------------------------------------------------------------
// Marker encoding: round trip, binary safety and corruption rejection
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_Encoding(t *testing.T) {
	cases := []struct {
		name                         string
		start, end                   []byte
		startUnbounded, endUnbounded bool
	}{
		{"finite", []byte("a"), []byte("z"), false, false},
		{"empty finite boundaries", []byte{}, []byte{0x00}, false, false},
		{"binary 0xff boundaries", []byte{0x00, 0xff}, []byte{0xff, 0xff, 0xff}, false, false},
		{"unbounded start", nil, []byte("z"), true, false},
		{"unbounded end", []byte("a"), nil, false, true},
		{"fully unbounded", nil, nil, true, true},
	}
	header := make([]byte, maxLogRecordHeaderSize)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := bytebufferpool.Get()
			defer bytebufferpool.Put(buf)
			rec := encodeRangeDeleteRecord(123456789, c.start, c.end,
				c.startUnbounded, c.endUnbounded, header, buf)
			assert.Equal(t, LogRecordRangeDeleted, rec.Type)
			assert.Equal(t, uint64(123456789), rec.BatchId)
			assert.Equal(t, c.startUnbounded, rec.StartUnbounded())
			assert.Equal(t, c.endUnbounded, rec.EndUnbounded())

			decoded, err := decodeRangeDeleteRecord(buf.Bytes())
			assert.NoError(t, err)
			assert.Equal(t, rec.Type, decoded.Type)
			assert.Equal(t, rec.BatchId, decoded.BatchId)
			assert.Equal(t, rec.Expire, decoded.Expire)
			assert.True(t, bytes.Equal(rec.Key, decoded.Key))
			assert.True(t, bytes.Equal(rec.Value, decoded.Value))

			// an empty finite boundary must round trip distinctly from unbounded.
			if !c.startUnbounded {
				assert.False(t, decoded.StartUnbounded())
			}
		})
	}

	// RangeContains semantics, including keys made purely of 0xFF bytes.
	marker := &LogRecord{Type: LogRecordRangeDeleted, Key: []byte{0x00}, Value: []byte{0xff}, Expire: 0}
	assert.True(t, marker.RangeContains([]byte{0x00}))
	assert.True(t, marker.RangeContains([]byte{0x7f}))
	assert.True(t, marker.RangeContains([]byte{0xfe, 0xff}))
	assert.False(t, marker.RangeContains([]byte{}))
	assert.False(t, marker.RangeContains([]byte{0xff}))
	assert.False(t, marker.RangeContains([]byte{0xff, 0x00}))

	full := &LogRecord{Type: LogRecordRangeDeleted, Expire: rangeStartUnbounded | rangeEndUnbounded}
	assert.True(t, full.RangeContains(nil))
	assert.True(t, full.RangeContains([]byte{}))
	assert.True(t, full.RangeContains([]byte{0xff, 0xff}))

	// corrupted frames must return errors instead of panicking.
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	encodeRangeDeleteRecord(1, []byte("a"), []byte("z"), false, false, header, buf)
	raw := append([]byte(nil), buf.Bytes()...)

	_, err := decodeRangeDeleteRecord(nil)
	assert.Error(t, err)
	_, err = decodeRangeDeleteRecord(raw[:len(raw)-1]) // truncated
	assert.Error(t, err)

	badType := append([]byte(nil), raw...)
	badType[0] = LogRecordNormal
	_, err = decodeRangeDeleteRecord(badType)
	assert.Error(t, err)

	// tamper the flags varint with an unknown flag bit.
	badFlags := append([]byte(nil), raw...)
	// header layout: type(1) + batchId varint + startSize + endSize + flags.
	// locate the flags byte by decoding the first four fields manually.
	idx := 1
	_, n := binary.Uvarint(badFlags[idx:])
	idx += n
	_, n = binary.Varint(badFlags[idx:])
	idx += n
	_, n = binary.Varint(badFlags[idx:])
	idx += n
	badFlags[idx] = 0x7f // unknown bits, still a valid single-byte varint
	_, err = decodeRangeDeleteRecord(badFlags)
	assert.Error(t, err)

	// boundary length overflowing the buffer must be rejected.
	overflow := []byte{LogRecordRangeDeleted}
	overflow = binary.AppendUvarint(overflow, 1)
	overflow = binary.AppendVarint(overflow, 100) // start key claims 100 bytes
	overflow = binary.AppendVarint(overflow, 0)
	overflow = binary.AppendVarint(overflow, 0)
	overflow = append(overflow, 'a')
	_, err = decodeRangeDeleteRecord(overflow)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Recovery: interleaved markers and point records, cross segment batch
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_RecoveryOrdering(t *testing.T) {
	options := DefaultOptions
	options.SegmentSize = 64 * KB
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 40; i++ {
		assert.NoError(t, db.Put(rangeKey(i), utils.RandomValue(2*KB)))
	}
	// the seed data alone must already span multiple segments.
	assert.Greater(t, db.dataFiles.ActiveSegmentID(), wal.SegmentID(1))

	// one big batch spanning multiple segments:
	// range delete [10,30), revive key 20, point delete key 5, put key 40.
	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(10), rangeKey(30)))
	assert.NoError(t, batch.Put(rangeKey(20), []byte("revived-20")))
	assert.NoError(t, batch.Delete(rangeKey(5)))
	assert.NoError(t, batch.Put(rangeKey(40), []byte("new-40")))
	assert.NoError(t, batch.Commit())

	db2 := reopenDB(t, db)
	for i := 0; i <= 40; i++ {
		_, err = db2.Get(rangeKey(i))
		switch {
		case i == 5 || (i >= 10 && i < 30 && i != 20):
			assert.Equal(t, ErrKeyNotFound, err, "key %d must be absent", i)
		default:
			assert.NoError(t, err, "key %d must be present", i)
		}
	}
	v, err := db2.Get(rangeKey(20))
	assert.NoError(t, err)
	assert.Equal(t, []byte("revived-20"), v)
	v, err = db2.Get(rangeKey(40))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new-40"), v)
}

// An unfinished batch whose records (including a fully unbounded range
// marker) span several segments must never delete old committed data when
// the database is reopened.
func TestBatch_DeleteRange_UnfinishedBatchCrossSegment(t *testing.T) {
	options := DefaultOptions
	options.SegmentSize = 256 * KB
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	const seeded = 20
	for i := 0; i < seeded; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("committed")))
	}
	startSegment := db.dataFiles.ActiveSegmentID()

	// emulate a crashed batch: raw wal records WITHOUT BatchFinished,
	// spread over multiple WriteAll calls so segments rotate.
	const fakeBatchId = uint64(999000111)
	bigValue := utils.RandomValue(60 * KB)
	for round := 0; round < 12; round++ {
		buf := bytebufferpool.Get()
		rec := &LogRecord{
			Key:     []byte(fmt.Sprintf("unfinished-%02d", round)),
			Value:   bigValue,
			Type:    LogRecordNormal,
			BatchId: fakeBatchId,
		}
		db.dataFiles.PendingWrites(encodeLogRecord(rec, db.encodeHeader, buf))
		_, err = db.dataFiles.WriteAll()
		require.NoError(t, err)
		bytebufferpool.Put(buf)
	}
	// the range marker would delete every committed key if it applied early.
	buf := bytebufferpool.Get()
	encodeRangeDeleteRecord(fakeBatchId, nil, nil, true, true, db.encodeHeader, buf)
	db.dataFiles.PendingWrites(buf.Bytes())
	_, err = db.dataFiles.WriteAll()
	require.NoError(t, err)
	bytebufferpool.Put(buf)

	assert.Greater(t, db.dataFiles.ActiveSegmentID(), startSegment)

	db2 := reopenDB(t, db)
	assert.Equal(t, seeded, db2.Stat().KeysNum)
	for i := 0; i < seeded; i++ {
		v, err := db2.Get(rangeKey(i))
		assert.NoError(t, err)
		assert.Equal(t, []byte("committed"), v)
	}
	for round := 0; round < 12; round++ {
		_, err = db2.Get([]byte(fmt.Sprintf("unfinished-%02d", round)))
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

// ---------------------------------------------------------------------------
// WAL marker count must not grow with the number of affected keys
// ---------------------------------------------------------------------------

func countWalChunks(t *testing.T, db *DB) (total, rangeMarkers, batchFinished int, markerPayload []byte) {
	t.Helper()
	reader := db.dataFiles.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		total++
		switch chunk[0] {
		case LogRecordRangeDeleted:
			rangeMarkers++
			markerPayload = append(markerPayload[:0], chunk...)
		case LogRecordBatchFinished:
			batchFinished++
		}
	}
	return total, rangeMarkers, batchFinished, markerPayload
}

func TestBatch_DeleteRange_WALMarkerCount(t *testing.T) {
	markerSizes := make(map[int]int)
	for _, n := range []int{100, 500} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			options := DefaultOptions
			db, err := Open(options)
			require.NoError(t, err)
			defer destroyDB(db)

			for i := 0; i < n; i++ {
				require.NoError(t, db.Put(rangeKey(i), []byte("v")))
			}

			batch := db.NewBatch(DefaultBatchOptions)
			require.NoError(t, batch.DeleteRange(rangeKey(0), rangeKey(n)))
			require.NoError(t, batch.Commit())

			_, markers, finished, payload := countWalChunks(t, db)
			// exactly one range marker regardless of how many keys match,
			// and one BatchFinished per committed batch (n puts + 1 range).
			assert.Equal(t, 1, markers)
			assert.Equal(t, n+1, finished)
			markerSizes[n] = len(payload)

			db2 := reopenDB(t, db)
			assert.Equal(t, 0, db2.Stat().KeysNum)
			_, markers2, _, _ := countWalChunks(t, db2)
			assert.Equal(t, 1, markers2)
		})
	}
	// boundaries have the same width, so the marker frame size must not grow
	// with the number of affected keys.
	assert.Equal(t, markerSizes[100], markerSizes[500])
}

// ---------------------------------------------------------------------------
// Merge: historical values must not revive, newer values must survive,
// and the hint file / reopen path must agree.
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_Merge(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("old")))
	}

	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(10), rangeKey(90)))
	assert.NoError(t, batch.Put(rangeKey(50), []byte("revived")))
	assert.NoError(t, batch.Commit())

	assert.NoError(t, db.Merge(false))

	db2 := reopenDB(t, db)
	for i := 0; i < 100; i++ {
		v, err := db2.Get(rangeKey(i))
		switch {
		case i == 50:
			assert.NoError(t, err)
			assert.Equal(t, []byte("revived"), v)
		case i >= 10 && i < 90:
			assert.Equal(t, ErrKeyNotFound, err, "key %d should stay deleted", i)
		default:
			assert.NoError(t, err)
			assert.Equal(t, []byte("old"), v)
		}
	}
	expected := 100 - 80 + 1
	assert.Equal(t, expected, db2.Stat().KeysNum)

	// reopening again exercises the hint-based index path; the logical
	// result must be identical.
	db3 := reopenDB(t, db2)
	assert.Equal(t, expected, db3.Stat().KeysNum)
	v, err := db3.Get(rangeKey(50))
	assert.NoError(t, err)
	assert.Equal(t, []byte("revived"), v)
	_, err = db3.Get(rangeKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// no range marker should remain in the merged data files.
	_, markers, _, _ := countWalChunks(t, db3)
	assert.Equal(t, 0, markers)
}

func TestBatch_DeleteRange_MergeWithConcurrentWrites(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 1000; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("old")))
	}
	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(0), rangeKey(500)))
	assert.NoError(t, batch.Commit())

	var wg sync.WaitGroup
	newKeys := sync.Map{}
	wg.Add(4)
	for g := 0; g < 4; g++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 200; i++ {
				k := []byte(fmt.Sprintf("concurrent-%d", r.Int63()))
				assert.NoError(t, db.Put(k, []byte("fresh")))
				newKeys.Store(string(k), struct{}{})
			}
		}()
	}
	assert.NoError(t, db.Merge(true))
	wg.Wait()

	var freshCount int
	newKeys.Range(func(_, _ any) bool {
		freshCount++
		return true
	})

	db2 := reopenDB(t, db)
	for i := 0; i < 500; i++ {
		_, err = db2.Get(rangeKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 500; i < 1000; i++ {
		_, err = db2.Get(rangeKey(i))
		assert.NoError(t, err)
	}
	var got int
	db2.index.Ascend(func(k []byte, _ *wal.ChunkPosition) (bool, error) {
		if bytes.HasPrefix(k, []byte("concurrent-")) {
			got++
		}
		return true, nil
	})
	assert.Equal(t, freshCount, got)
	assert.Equal(t, 500+freshCount, db2.Stat().KeysNum)
}

// ---------------------------------------------------------------------------
// Watch events emitted for range deletions
// ---------------------------------------------------------------------------

func TestBatch_DeleteRange_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 100
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	watchCh, err := db.Watch()
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		assert.NoError(t, db.Put(rangeKey(i), []byte("v")))
	}
	// drain the put events; the watcher goroutine forwards asynchronously,
	// so wait until all ten put events have been observed.
	drainDeadline := time.After(3 * time.Second)
	drained := 0
	for drained < 10 {
		select {
		case e := <-watchCh:
			assert.Equal(t, WatchActionPut, e.Action)
			drained++
		case <-drainDeadline:
			t.Fatalf("timed out draining put events, got %d", drained)
		}
	}

	batch := db.NewBatch(DefaultBatchOptions)
	assert.NoError(t, batch.DeleteRange(rangeKey(2), rangeKey(5)))
	assert.NoError(t, batch.Commit())

	got := make(map[string]bool)
	for i := 0; i < 3; i++ {
		select {
		case e := <-watchCh:
			assert.Equal(t, WatchActionDelete, e.Action)
			got[string(e.Key)] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for watch event %d", i)
		}
	}
	assert.Equal(t, map[string]bool{
		string(rangeKey(2)): true,
		string(rangeKey(3)): true,
		string(rangeKey(4)): true,
	}, got)
}

// ---------------------------------------------------------------------------
// Randomized comparison against an independent ordered map model
// ---------------------------------------------------------------------------

type modelEntry struct {
	value  []byte
	expire int64 // 0 means no expiration
}

// referenceModel applies the exact same operation sequence as the batch but
// keeps an independent ordered map, so any ordering/dedup mistake in the
// batch implementation shows up as a divergence.
type referenceModel struct {
	data map[string]*modelEntry
}

func newReferenceModel() *referenceModel {
	return &referenceModel{data: make(map[string]*modelEntry)}
}

func modelInRange(k, start, end []byte, startUnbounded, endUnbounded bool) bool {
	if !startUnbounded && bytes.Compare(k, start) < 0 {
		return false
	}
	if !endUnbounded && bytes.Compare(k, end) >= 0 {
		return false
	}
	return true
}

func (m *referenceModel) put(k, v []byte, ttl time.Duration) {
	e := &modelEntry{value: append([]byte(nil), v...)}
	if ttl > 0 {
		e.expire = time.Now().Add(ttl).UnixNano()
	}
	m.data[string(k)] = e
}

func (m *referenceModel) delete(k []byte) { delete(m.data, string(k)) }

func (m *referenceModel) deleteRange(start, end []byte, su, eu bool) {
	for k := range m.data {
		if modelInRange([]byte(k), start, end, su, eu) {
			delete(m.data, k)
		}
	}
}

func (m *referenceModel) expire(k []byte, ttl time.Duration) error {
	e, ok := m.data[string(k)]
	if !ok {
		return ErrKeyNotFound
	}
	e.expire = time.Now().Add(ttl).UnixNano()
	return nil
}

func (m *referenceModel) persist(k []byte) error {
	e, ok := m.data[string(k)]
	if !ok {
		return ErrKeyNotFound
	}
	e.expire = 0
	return nil
}

func (m *referenceModel) sortedKeys() [][]byte {
	keys := make([][]byte, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, []byte(k))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	return keys
}

func TestBatch_DeleteRange_ModelComparison(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	model := newReferenceModel()

	// binary-heavy key universe; empty key can only serve as a boundary.
	universe := [][]byte{
		{0x00}, {0x00, 0xff}, {0x01}, []byte("apple"), []byte("banana"),
		[]byte("mango"), {0x7f}, {0x7f, 0x00}, {0xff}, {0xff, 0xff},
	}
	boundaries := append([][]byte{nil}, universe...)
	r := rand.New(rand.NewSource(20240517))
	const batches = 12

	for b := 0; b < batches; b++ {
		batch := db.NewBatch(DefaultBatchOptions)
		ops := 40
		for op := 0; op < ops; op++ {
			k := universe[r.Intn(len(universe))]
			switch r.Intn(9) {
			case 0, 1: // Put
				v := []byte(fmt.Sprintf("v-%d-%d", b, op))
				assert.NoError(t, batch.Put(k, v))
				model.put(k, v, 0)
			case 2: // PutWithTTL
				v := []byte(fmt.Sprintf("ttl-%d-%d", b, op))
				assert.NoError(t, batch.PutWithTTL(k, v, 10*time.Hour))
				model.put(k, v, 10*time.Hour)
			case 3: // Delete
				assert.NoError(t, batch.Delete(k))
				model.delete(k)
			case 4: // DeleteRange
				s := boundaries[r.Intn(len(boundaries))]
				e := boundaries[r.Intn(len(boundaries))]
				su, eu := s == nil, e == nil
				err := batch.DeleteRange(s, e)
				if !su && !eu && bytes.Compare(s, e) > 0 {
					assert.Equal(t, ErrInvalidRange, err)
					continue
				}
				assert.NoError(t, err)
				// equal finite boundaries form an empty interval and the
				// model delete naturally removes nothing in that case.
				model.deleteRange(s, e, su, eu)
			case 5: // Expire
				err := batch.Expire(k, time.Hour)
				_, inModel := model.data[string(k)]
				if !inModel {
					assert.Equal(t, ErrKeyNotFound, err)
				} else {
					assert.NoError(t, err)
					assert.NoError(t, model.expire(k, time.Hour))
				}
			case 6: // Persist
				err := batch.Persist(k)
				_, inModel := model.data[string(k)]
				if !inModel {
					assert.Equal(t, ErrKeyNotFound, err)
				} else {
					assert.NoError(t, err)
					assert.NoError(t, model.persist(k))
				}
			case 7: // Get comparison
				got, gErr := batch.Get(k)
				me, inModel := model.data[string(k)]
				if !inModel {
					assert.Equal(t, ErrKeyNotFound, gErr, "batch=%d op=%d key=%x", b, op, k)
				} else {
					assert.NoError(t, gErr)
					assert.Equal(t, me.value, got)
				}
			case 8: // Exist + TTL comparison
				got, gErr := batch.Exist(k)
				_, inModel := model.data[string(k)]
				assert.NoError(t, gErr)
				assert.Equal(t, inModel, got)
				if inModel {
					ttl, tErr := batch.TTL(k)
					assert.NoError(t, tErr)
					me := model.data[string(k)]
					if me.expire == 0 {
						assert.Equal(t, time.Duration(-1), ttl)
					} else {
						assert.True(t, ttl > 0)
					}
				}
			}
		}
		assert.NoError(t, batch.Commit())

		// every few batches, reopen and keep comparing against the model.
		if b%3 == 2 {
			db = reopenDB(t, db)
		}
		for _, k := range universe {
			got, gErr := db.Get(k)
			me, inModel := model.data[string(k)]
			if !inModel {
				assert.Equal(t, ErrKeyNotFound, gErr, "key=%x", k)
			} else {
				assert.NoError(t, gErr)
				assert.Equal(t, me.value, got)
			}
		}
	}

	// final reopen and full-keyspace comparison.
	db2 := reopenDB(t, db)
	assert.Equal(t, len(model.data), db2.Stat().KeysNum)
	for _, k := range model.sortedKeys() {
		v, err := db2.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, model.data[string(k)].value, v)
	}
}
