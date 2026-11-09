package rosedb

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/rosedblabs/wal"
	"github.com/valyala/bytebufferpool"
)

// LogRecordType is the type of the log record.
type LogRecordType = byte

const (
	// LogRecordNormal is the normal log record type.
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted is the deleted log record type.
	LogRecordDeleted
	// LogRecordBatchFinished is the batch finished log record type.
	LogRecordBatchFinished
	// LogRecordRangeDeleted is the range-delete marker log record type.
	// A single marker expresses the deletion of every key inside the
	// lexicographic half-open interval [startKey, endKey), so the size of
	// the written log is independent of the number of affected keys.
	//
	// Format compatibility: binaries older than the version that introduced
	// this type only know type 0/1/2 and cannot interpret data files that
	// contain a type 3 record. New binaries can still read all old records,
	// and the on-disk frame (header layout and varint encoding) is unchanged,
	// so frame boundaries remain decodable by either version.
	LogRecordRangeDeleted
)

// Flags carried by a LogRecordRangeDeleted record. They are stored in the
// LogRecord.Expire field (which is unused for range markers) and encoded as
// the last varint of the standard record header.
//
// Unboundedness is always expressed through these flags, never inferred from
// a zero-length boundary: an empty but non-nil key is a finite boundary that
// compares smaller than every other key.
const (
	// rangeStartUnbounded means the lower bound is negative infinity.
	rangeStartUnbounded int64 = 1 << 0
	// rangeEndUnbounded means the upper bound is positive infinity.
	rangeEndUnbounded int64 = 1 << 1
	// rangeKnownFlags masks every flag bit that has a meaning.
	rangeKnownFlags = rangeStartUnbounded | rangeEndUnbounded
)

// type batchId keySize valueSize expire
//
//	1  +  10  +   5   +   5   +    10  = 31
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2 + 1

// LogRecord is the log record of the key/value pair.
// It contains the key, the value, the record type and the batch id
// It will be encoded to byte slice and written to the wal.
type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchId uint64
	Expire  int64
}

// IsExpired checks whether the log record is expired.
func (lr *LogRecord) IsExpired(now int64) bool {
	return lr.Expire > 0 && lr.Expire <= now
}

// StartUnbounded reports whether the range marker has no lower bound
// (negative infinity). Only meaningful for LogRecordRangeDeleted records.
func (lr *LogRecord) StartUnbounded() bool {
	return lr.Expire&rangeStartUnbounded != 0
}

// EndUnbounded reports whether the range marker has no upper bound
// (positive infinity). Only meaningful for LogRecordRangeDeleted records.
func (lr *LogRecord) EndUnbounded() bool {
	return lr.Expire&rangeEndUnbounded != 0
}

// RangeContains reports whether key falls into the half-open interval
// [startKey, endKey) expressed by the range-delete marker.
// Arbitrary binary keys (including empty and 0xFF containing ones) are
// compared by raw lexicographic byte order.
func (lr *LogRecord) RangeContains(key []byte) bool {
	if !lr.StartUnbounded() && bytes.Compare(key, lr.Key) < 0 {
		return false
	}
	if !lr.EndUnbounded() && bytes.Compare(key, lr.Value) >= 0 {
		return false
	}
	return true
}

// encodeRangeDeleteRecord encodes a single range-delete marker.
//
// The marker reuses the standard record frame:
//
//	type | batchId | startKeySize | endKeySize | flags | startKey | endKey
//
// where the key/value size fields carry the two boundary lengths and the
// expire field carries the unbounded flags. Nil boundaries together with the
// matching flags represent infinity; a non-nil empty slice remains a finite,
// distinct boundary.
func encodeRangeDeleteRecord(batchId uint64, startKey, endKey []byte,
	startUnbounded, endUnbounded bool, header []byte, buf *bytebufferpool.ByteBuffer,
) *LogRecord {
	var flags int64
	if startUnbounded {
		flags |= rangeStartUnbounded
	}
	if endUnbounded {
		flags |= rangeEndUnbounded
	}
	record := &LogRecord{
		Key:     startKey,
		Value:   endKey,
		Type:    LogRecordRangeDeleted,
		BatchId: batchId,
		Expire:  flags,
	}
	encodeLogRecord(record, header, buf)
	return record
}

// decodeRangeDeleteRecord decodes and strictly validates a range-delete
// marker. It rejects truncated frames, malformed varints, negative or
// overflowing boundary lengths, unknown flag bits and mismatched
// unbounded/length combinations instead of panicking on hostile input.
func decodeRangeDeleteRecord(buf []byte) (*LogRecord, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if buf[0] != LogRecordRangeDeleted {
		return nil, ErrInvalidLogRecord
	}

	var index uint64 = 1
	readUvarint := func() (uint64, error) {
		v, n := binary.Uvarint(buf[index:])
		if n <= 0 {
			return 0, ErrInvalidLogRecord
		}
		index += uint64(n)
		if index > uint64(len(buf)) {
			return 0, ErrInvalidLogRecord
		}
		return v, nil
	}
	readVarint := func() (int64, error) {
		v, n := binary.Varint(buf[index:])
		if n <= 0 {
			return 0, ErrInvalidLogRecord
		}
		index += uint64(n)
		if index > uint64(len(buf)) {
			return 0, ErrInvalidLogRecord
		}
		return v, nil
	}

	batchId, err := readUvarint()
	if err != nil {
		return nil, err
	}
	startSize, err := readVarint()
	if err != nil {
		return nil, err
	}
	endSize, err := readVarint()
	if err != nil {
		return nil, err
	}
	flags, err := readVarint()
	if err != nil {
		return nil, err
	}

	if startSize < 0 || endSize < 0 {
		return nil, ErrInvalidLogRecord
	}
	if flags&^rangeKnownFlags != 0 {
		return nil, ErrInvalidLogRecord
	}
	startUnbounded := flags&rangeStartUnbounded != 0
	endUnbounded := flags&rangeEndUnbounded != 0
	if (startUnbounded && startSize != 0) || (endUnbounded && endSize != 0) {
		return nil, ErrInvalidLogRecord
	}

	// Guard against integer overflow before indexing the buffer.
	remaining := uint64(len(buf)) - index
	if uint64(startSize) > remaining || uint64(endSize) > remaining-uint64(startSize) {
		return nil, ErrInvalidLogRecord
	}

	startKey := make([]byte, startSize)
	copy(startKey, buf[index:index+uint64(startSize)])
	index += uint64(startSize)
	endKey := make([]byte, endSize)
	copy(endKey, buf[index:index+uint64(endSize)])
	index += uint64(endSize)
	if index != uint64(len(buf)) {
		return nil, ErrInvalidLogRecord
	}

	return &LogRecord{
		Key:     startKey,
		Value:   endKey,
		Type:    LogRecordRangeDeleted,
		BatchId: batchId,
		Expire:  flags,
	}, nil
}

// IndexRecord is the index record of the key.
// It contains the key, the record type and the position of the record in the wal.
// Only used in start up to rebuild the index.
type IndexRecord struct {
	key        []byte
	endKey     []byte // only used by LogRecordRangeDeleted: the exclusive upper bound
	recordType LogRecordType
	position   *wal.ChunkPosition
	// only used by LogRecordRangeDeleted
	startUnbounded bool
	endUnbounded   bool
}

// +-------------+-------------+-------------+--------------+---------------+---------+--------------+
// |    type     |  batch id   |   key size  |   value size |     expire    |  key    |      value   |
// +-------------+-------------+-------------+--------------+---------------+--------+--------------+
//
//	1 byte	      varint(max 10) varint(max 5)  varint(max 5) varint(max 10)  varint      varint
func encodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	header[0] = logRecord.Type
	index := 1

	// batch id
	index += binary.PutUvarint(header[index:], logRecord.BatchId)
	// key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// expire
	index += binary.PutVarint(header[index:], logRecord.Expire)

	// copy header
	_, _ = buf.Write(header[:index])
	// copy key
	_, _ = buf.Write(logRecord.Key)
	// copy value
	_, _ = buf.Write(logRecord.Value)

	return buf.Bytes()
}

// decodeLogRecord decodes the log record from the given byte slice.
func decodeLogRecord(buf []byte) *LogRecord {
	recordType := buf[0]

	var index uint32 = 1
	// batch id
	batchId, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// key size
	keySize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// value size
	valueSize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// expire
	expire, n := binary.Varint(buf[index:])
	index += uint32(n)

	// copy key
	key := make([]byte, keySize)
	copy(key, buf[index:index+uint32(keySize)])
	index += uint32(keySize)

	// copy value
	value := make([]byte, valueSize)
	copy(value, buf[index:index+uint32(valueSize)])

	return &LogRecord{
		Key: key, Value: value, Expire: expire,
		BatchId: batchId, Type: recordType,
	}
}

func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// see binary.MaxVarintLen64 and binary.MaxVarintLen32
	buf := make([]byte, 25)
	idx := 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

func decodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	idx := 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func encodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
