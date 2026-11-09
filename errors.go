package rosedb

import "errors"

var (
	ErrKeyIsEmpty      = errors.New("the key is empty")
	ErrKeyNotFound     = errors.New("key not found in database")
	ErrDatabaseIsUsing = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch   = errors.New("the batch is read only")
	ErrBatchCommitted  = errors.New("the batch is committed")
	ErrBatchRollbacked = errors.New("the batch is rollbacked")
	ErrDBClosed        = errors.New("the database is closed")
	ErrMergeRunning    = errors.New("the merge operation is running")
	ErrWatchDisabled   = errors.New("the watch is disabled")
	// ErrInvalidRange is returned when a range-delete boundary is illegal,
	// for example a finite start key strictly greater than the end key.
	ErrInvalidRange = errors.New("invalid delete range: start key must not be greater than end key")
	// ErrInvalidLogRecord is returned when a log record cannot be decoded or
	// fails its structural validation.
	ErrInvalidLogRecord = errors.New("invalid log record")
)
