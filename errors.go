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

	ErrBackupPathConflict   = errors.New("the backup path must not be the database directory, inside it, or contain it")
	ErrBackupTargetNotEmpty = errors.New("the backup target directory is not empty")
	ErrBackupTargetNotDir   = errors.New("the backup target path exists but is not a directory")
	ErrBackupParentMissing  = errors.New("the parent directory of the backup target path does not exist")
	ErrBackupIncomplete     = errors.New("the backup is incomplete")
)
