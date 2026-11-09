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

	// ErrOptimisticConflict is returned by OptimisticBatch.Commit when the
	// logical state of any observed or written key changed since the batch
	// first observed it. The whole commit fails atomically: no staged key
	// becomes visible.
	ErrOptimisticConflict = errors.New("optimistic batch commit conflict")
	// ErrOptimisticBatchFinished is returned when an OptimisticBatch method is
	// invoked after the batch has already been committed or rollbacked.
	ErrOptimisticBatchFinished = errors.New("the optimistic batch is already committed or rollbacked")
	// ErrOptimisticInstanceMismatch is returned when an OptimisticBatch is
	// committed against a different DB instance than the one that created it
	// (e.g. the database was closed and reopened).
	ErrOptimisticInstanceMismatch = errors.New("the optimistic batch belongs to another database instance")
)
