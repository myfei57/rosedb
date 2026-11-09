package rosedb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rosedblabs/wal"
)

const (
	// backupFinName is the completion marker published as the very last step
	// of a successful backup. A directory without this file is never a
	// complete backup and BackupTo never reports success for it.
	backupFinName = "BACKUPFIN"
	// backupManifestName is the verification checklist written before the
	// completion marker, listing every captured file with its exact size.
	backupManifestName = "BACKUPMANIFEST"
	// backupTempPrefix is used for the sibling staging directory created in
	// the target's parent directory.
	backupTempPrefix = ".rosedb-backup-tmp-"
	// backupManifestVersion is the manifest format version.
	backupManifestVersion = 1
	// backupCopyBufSize is the chunk size used while copying large files.
	// Cancellation is checked at least once per chunk.
	backupCopyBufSize = 1 * MB
	// backupMergeWaitInterval is how long BackupTo waits between checks
	// while a Merge is running before capturing its boundary.
	backupMergeWaitInterval = 50 * time.Millisecond
)

// backupManifest is the verification checklist of a backup.
// BACKUPMANIFEST and BACKUPFIN contain exactly these JSON bytes.
type backupManifest struct {
	// Version is the manifest format version.
	Version int `json:"version"`
	// BoundarySegmentID is the last data segment included in the backup.
	// Newer segments containing commits after the boundary are excluded.
	BoundarySegmentID uint32 `json:"boundary_segment_id"`
	// MergeFinSegmentID is the segment id recorded by MERGEFIN, 0 if the
	// database has never been merged.
	MergeFinSegmentID uint32 `json:"merge_fin_segment_id"`
	// Files lists every captured file with its exact size and sha256.
	Files []backupManifestEntry `json:"files"`
}

type backupManifestEntry struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// backupOptions is the internal backup configuration. The public entry point
// is DB.BackupTo; the hooks are only used by tests inside the package.
type backupOptions struct {
	// sync controls whether copied files are fsynced before the backup is
	// published. See BackupTo for the exact durability guarantees.
	sync  bool
	hooks backupHooks
}

// backupHooks injects barriers and failures into the copy phase.
// All hooks are invoked without holding db.mu.
type backupHooks struct {
	// beforeFile is called before each captured file is copied, in order.
	// Returning an error aborts the backup. Tests use it as a copy barrier.
	beforeFile func(name, srcPath, dstPath string) error
	// afterFile is called after each file was copied, verified for size and
	// (optionally) synced. Tests use it to corrupt staging content or fail
	// mid-backup.
	afterFile func(name, srcPath, dstPath string) error
	// syncFile overrides the fsync operation used when sync is enabled.
	// Tests use it to inject fsync failures.
	syncFile func(f *os.File) error
}

// backupFile is a pinned file of the captured generation: an independent
// read-only descriptor plus its immutable on-disk size.
type backupFile struct {
	name string
	path string
	size int64
	fd   *os.File
}

// backupCapture is the fixed file generation backing a backup.
type backupCapture struct {
	// files are the files to copy, sorted by name: data segments, and the
	// hint/merge-finished pair when a completed merge exists.
	files []*backupFile
	// boundarySegID is the active data segment id at the moment the
	// boundary was rotated (the last segment included in the backup).
	boundarySegID wal.SegmentID
	// mergeFinSegID is the merged segment id recorded by MERGEFIN, 0 if no
	// completed merge exists.
	mergeFinSegID wal.SegmentID
}

func (c *backupCapture) close() {
	for _, f := range c.files {
		_ = f.fd.Close()
	}
}

// BackupTo creates a logically consistent online backup of the database that
// can be opened directly with Open.
//
// The backup corresponds to a completed batch boundary. During a short
// protected phase BackupTo waits for any running merge to finish, rotates the
// active data segment so that it is sealed exactly at a batch boundary, and
// pins one consistent generation of data segments together with the matching
// hint and merge-finished files. The large files are afterwards copied without
// holding the database lock, so Put/Delete and other operations keep working.
// Commits made after the boundary go into newer segments and are never mixed
// into the backup, and unfinished batches can never become visible.
//
// The pinned generation is protected by a lease until copying finishes: a
// concurrent Merge(true), another merge, or DB.Close cannot delete, replace or
// close the captured files. Leases are always released, including on
// cancellation or failure.
//
// Only raw log bytes are copied, so already-expired records may remain in old
// logs but their absolute expiry timestamps are preserved verbatim; expiry is
// never recomputed from the backup time.
//
// Durability:
//   - The boundary rotation fsyncs the sealed active segment in the source
//     database, so the captured boundary is durable in the source regardless
//     of the sync argument.
//   - If sync is true, every copied file and the completion marker is fsynced
//     before success is reported, and the backup directory is fsynced on
//     platforms that support it. Any fsync error aborts the backup and is
//     returned to the caller. Closing a file is never treated as a successful
//     fsync.
//   - If sync is false, the backup has the same durability semantics as a
//     normal write without Sync: it survives process crashes but may be lost
//     in the backup directory on an operating-system crash or power failure.
//
// Target directory rules:
//   - backupDirPath must not exist, or must be an existing empty directory.
//   - It must not be the database directory, a directory inside it, or a
//     directory containing it (symlinks are resolved before the check), and it
//     must not be the merge working directory.
//   - Content is first written to a temporary sibling directory together with
//     a BACKUPMANIFEST checklist. Only after every file has been copied and
//     verified (existence and exact size) is the BACKUPFIN completion marker
//     written and the backup published atomically. An output without
//     BACKUPFIN, with missing files or with size mismatches is never reported
//     as success.
//   - On failure or cancellation only files and directories created by this
//     invocation are removed; other backups, the database directory and any
//     pre-existing content are never overwritten or deleted.
//
// The context cancels a backup while it is waiting or copying; already
// captured leases and temporary content are released/removed on cancellation.
func (db *DB) BackupTo(ctx context.Context, backupDirPath string, sync bool) error {
	return db.backupTo(ctx, backupDirPath, backupOptions{sync: sync})
}

func (db *DB) backupTo(ctx context.Context, backupDirPath string, opts backupOptions) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(backupDirPath) == "" {
		return errors.New("backup dir path is empty")
	}

	// Serialize backups of this DB: protects the "absent or empty target"
	// check and the publication against two backups racing for one target.
	db.backupPublishMu.Lock()
	defer db.backupPublishMu.Unlock()

	// ---------- validate target path ----------
	srcResolved, err := resolvePath(db.options.DirPath)
	if err != nil {
		return err
	}
	tgtResolved, err := resolvePath(backupDirPath)
	if err != nil {
		return err
	}
	if samePath(srcResolved, tgtResolved) ||
		sameOrWithin(srcResolved, tgtResolved) ||
		sameOrWithin(tgtResolved, srcResolved) {
		return ErrBackupPathConflict
	}
	// The merge working directory is removed/recreated by Merge; backing up
	// into it (or a directory inside it) would mix a changing directory into
	// the output and let Merge delete our staging content.
	mergeWorkDir := mustResolvePath(mergeDirPath(srcResolved))
	if samePath(tgtResolved, mergeWorkDir) || sameOrWithin(mergeWorkDir, tgtResolved) {
		return ErrBackupPathConflict
	}

	targetExisted := false
	switch info, statErr := os.Stat(tgtResolved); {
	case statErr == nil:
		if !info.IsDir() {
			return ErrBackupTargetNotDir
		}
		entries, rerr := os.ReadDir(tgtResolved)
		if rerr != nil {
			return rerr
		}
		if len(entries) != 0 {
			return ErrBackupTargetNotEmpty
		}
		targetExisted = true
	case os.IsNotExist(statErr):
		// Only create the final directory itself; deeper missing parents are
		// not created, so failure cleanup cannot own unrelated directories.
		parent := filepath.Dir(tgtResolved)
		pinfo, perr := os.Stat(parent)
		if perr != nil || !pinfo.IsDir() {
			return ErrBackupParentMissing
		}
	default:
		return statErr
	}

	// ---------- create sibling staging directory ----------
	parent := filepath.Dir(tgtResolved)
	stage, err := os.MkdirTemp(parent, backupTempPrefix+"*")
	if err != nil {
		return err
	}
	_ = os.Chmod(stage, 0755)

	// Failure cleanup: only remove the staging dir and content created by us.
	published := false
	var movedNames []string
	defer func() {
		if published {
			return
		}
		_ = os.RemoveAll(stage)
		if !targetExisted {
			// The target directory is only created by the final atomic
			// rename, which either fully succeeds or does not happen.
			if info, e := os.Stat(tgtResolved); e == nil && info.IsDir() {
				_ = os.Remove(tgtResolved)
			}
		} else {
			for _, name := range movedNames {
				_ = os.Remove(filepath.Join(tgtResolved, name))
			}
		}
	}()

	// ---------- capture the consistency boundary ----------
	capture, err := db.captureBoundary(ctx)
	if err != nil {
		return err
	}
	defer func() {
		capture.close()
		atomic.AddInt64(&db.activeBackups, -1)
		db.backupMu.RUnlock()
	}()

	// ---------- copy pinned files without db.mu ----------
	manifest := backupManifest{
		Version:           backupManifestVersion,
		BoundarySegmentID: capture.boundarySegID,
		MergeFinSegmentID: capture.mergeFinSegID,
		Files:             make([]backupManifestEntry, 0, len(capture.files)),
	}
	for _, f := range capture.files {
		if err = ctx.Err(); err != nil {
			return err
		}
		dstPath := filepath.Join(stage, f.name)
		if opts.hooks.beforeFile != nil {
			if err = opts.hooks.beforeFile(f.name, f.path, dstPath); err != nil {
				return err
			}
		}
		size, sum, cerr := copyBackupFile(ctx, f.fd, f.size, dstPath, opts.sync, opts.hooks.syncFile)
		if cerr != nil {
			return cerr
		}
		if size != f.size {
			return fmt.Errorf("%w: %s has size %d, expected %d",
				ErrBackupIncomplete, f.name, size, f.size)
		}
		if opts.hooks.afterFile != nil {
			if err = opts.hooks.afterFile(f.name, f.path, dstPath); err != nil {
				return err
			}
		}
		manifest.Files = append(manifest.Files, backupManifestEntry{
			Name: f.name, Size: size, SHA256: hex.EncodeToString(sum),
		})
	}

	// Verify every staged file exists with the exact captured size.
	for _, entry := range manifest.Files {
		info, verr := os.Stat(filepath.Join(stage, entry.Name))
		if verr != nil {
			return fmt.Errorf("%w: %s: %v", ErrBackupIncomplete, entry.Name, verr)
		}
		if info.Size() != entry.Size {
			return fmt.Errorf("%w: %s has size %d, expected %d",
				ErrBackupIncomplete, entry.Name, info.Size(), entry.Size)
		}
	}

	// Write the checklist, then the completion marker as the last file.
	if err = ctx.Err(); err != nil {
		return err
	}
	manifestBytes, err := json.MarshalIndent(&manifest, "", "  ")
	if err != nil {
		return err
	}
	if err = writeBackupMetaFile(filepath.Join(stage, backupManifestName), manifestBytes, opts); err != nil {
		return err
	}
	if err = writeBackupMetaFile(filepath.Join(stage, backupFinName), manifestBytes, opts); err != nil {
		return err
	}
	if opts.sync {
		if err = syncBackupDir(stage); err != nil {
			return err
		}
	}
	// The marker must exist; a missing marker can never be a success.
	if _, err = os.Stat(filepath.Join(stage, backupFinName)); err != nil {
		return fmt.Errorf("%w: completion marker missing: %v", ErrBackupIncomplete, err)
	}

	// ---------- publish ----------
	if !targetExisted {
		// Atomic publication: the target appears fully formed in one rename.
		if err = os.Rename(stage, tgtResolved); err != nil {
			return err
		}
		if opts.sync {
			_ = syncBackupDir(parent)
		}
		published = true
		return nil
	}

	// Move the captured files first, manifest next and the marker last,
	// so an observer never sees the marker without the complete content.
	publishNames := make([]string, 0, len(manifest.Files)+2)
	for _, entry := range manifest.Files {
		publishNames = append(publishNames, entry.Name)
	}
	publishNames = append(publishNames, backupManifestName, backupFinName)
	for _, name := range publishNames {
		if err = os.Rename(filepath.Join(stage, name), filepath.Join(tgtResolved, name)); err != nil {
			return err
		}
		movedNames = append(movedNames, name)
	}
	_ = os.Remove(stage)
	if opts.sync {
		_ = syncBackupDir(tgtResolved)
		_ = syncBackupDir(parent)
	}
	published = true
	return nil
}

// captureBoundary waits for any running merge, then takes the generation lease
// and pins the backup boundary. db.mu is held while pinning and released
// before this method returns; the backupMu read lease stays held by the caller.
func (db *DB) captureBoundary(ctx context.Context) (_ *backupCapture, err error) {
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		if db.closed {
			db.mu.Unlock()
			return nil, ErrDBClosed
		}
		// A merge replaces files; capture only when no merge is running so
		// that data segments, hint and merge-finished info belong to one
		// generation.
		if atomic.LoadUint32(&db.mergeRunning) == 1 {
			db.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backupMergeWaitInterval):
			}
			continue
		}

		// Acquire the generation lease while holding db.mu. Merge(true) and
		// Close take backupMu in write mode (also under db.mu), so the
		// pinned generation cannot be replaced while the backup runs.
		db.backupMu.RLock()
		capture, cerr := db.pinBackupGeneration()
		if cerr != nil {
			db.backupMu.RUnlock()
			db.mu.Unlock()
			return nil, cerr
		}
		atomic.AddInt64(&db.activeBackups, 1)
		db.mu.Unlock()
		return capture, nil
	}
}

// pinBackupGeneration rotates the active segment (sealing a completed batch
// boundary) and opens independent read-only descriptors for exactly the files
// of the current generation. Must be called with db.mu and backupMu(R) held.
func (db *DB) pinBackupGeneration() (_ *backupCapture, err error) {
	// db.mu is held: no Put/Delete/batch commit is in flight, so the active
	// segment ends at a completed batch boundary. Rotation syncs the sealed
	// segment and makes all subsequent writes land in a newer segment.
	boundarySegID := db.dataFiles.ActiveSegmentID()
	if err = db.dataFiles.OpenNewActiveSegment(); err != nil {
		return nil, err
	}

	dir := db.options.DirPath
	segIDs, err := listSegmentFileIDs(dir, dataFileNameSuffix)
	if err != nil {
		return nil, err
	}

	capture := &backupCapture{boundarySegID: boundarySegID}
	for _, id := range segIDs {
		// Only segments up to the boundary; the new active segment and any
		// later segments hold post-boundary commits and must be excluded.
		if id > boundarySegID {
			continue
		}
		f, oerr := openBackupFile(dir, dataFileNameSuffix, id)
		if oerr != nil {
			capture.close()
			return nil, oerr
		}
		capture.files = append(capture.files, f)
	}
	if len(capture.files) == 0 {
		capture.close()
		return nil, fmt.Errorf("%w: no data segment found for the captured boundary", ErrBackupIncomplete)
	}

	// The hint and merge-finished files are only valid as the same
	// generation. MERGEFIN present means both must be copied; absent means
	// neither is copied (an empty hint file exists on every opened database).
	mergeFinPath := wal.SegmentFileName(dir, mergeFinNameSuffix, 1)
	if _, err = os.Stat(mergeFinPath); err == nil {
		mergeFinSegID, merr := getMergeFinSegmentId(dir)
		if merr != nil {
			capture.close()
			return nil, merr
		}
		capture.mergeFinSegID = mergeFinSegID

		hintPath := wal.SegmentFileName(dir, hintFileNameSuffix, 1)
		if _, err = os.Stat(hintPath); err != nil {
			capture.close()
			return nil, fmt.Errorf("%w: merge finished file exists but hint file is missing: %v",
				ErrBackupIncomplete, err)
		}
		// The hint file points into the merged segment, which must be there.
		if _, err = os.Stat(wal.SegmentFileName(dir, dataFileNameSuffix, 1)); err != nil {
			capture.close()
			return nil, fmt.Errorf("%w: merged data segment 1 is missing: %v",
				ErrBackupIncomplete, err)
		}
		hintFile, oerr := openBackupFile(dir, hintFileNameSuffix, 1)
		if oerr != nil {
			capture.close()
			return nil, oerr
		}
		mergeFinFile, oerr := openBackupFile(dir, mergeFinNameSuffix, 1)
		if oerr != nil {
			_ = hintFile.fd.Close()
			capture.close()
			return nil, oerr
		}
		capture.files = append(capture.files, hintFile, mergeFinFile)
	} else if !os.IsNotExist(err) {
		capture.close()
		return nil, err
	}

	sort.Slice(capture.files, func(i, j int) bool {
		return capture.files[i].name < capture.files[j].name
	})
	return capture, nil
}

func openBackupFile(dir, suffix string, id wal.SegmentID) (*backupFile, error) {
	path := wal.SegmentFileName(dir, suffix, id)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// Our own descriptor keeps the pinned inode readable even if a later
	// merge or close removes/renames the path after the lease is released.
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &backupFile{
		name: filepath.Base(path),
		path: path,
		size: info.Size(),
		fd:   fd,
	}, nil
}

func listSegmentFileIDs(dir, suffix string) ([]wal.SegmentID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []wal.SegmentID
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(entry.Name(), "%d"+suffix, &id); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// copyBackupFile copies one pinned file into the staging directory in chunks
// and returns the number of bytes written together with the sha256 digest.
// The file is fsynced before close when doSync is true; sync failures are
// returned and Close success is never equated with durability.
func copyBackupFile(ctx context.Context, src *os.File, expectedSize int64, dstPath string,
	doSync bool, syncHook func(*os.File) error) (int64, []byte, error) {

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, nil, err
	}
	h := sha256.New()
	buf := make([]byte, backupCopyBufSize)
	var total int64

	fail := func(e error) (int64, []byte, error) {
		_ = dst.Close()
		return total, nil, e
	}
	for {
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		default:
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			if werr != nil {
				return fail(werr)
			}
			if wn != n {
				return fail(io.ErrShortWrite)
			}
			_, _ = h.Write(buf[:wn])
			total += int64(wn)
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return fail(rerr)
		}
	}

	if total != expectedSize {
		return fail(fmt.Errorf("%w: copied %d bytes, expected %d",
			ErrBackupIncomplete, total, expectedSize))
	}
	if doSync {
		if syncHook != nil {
			err = syncHook(dst)
		} else {
			err = dst.Sync()
		}
		if err != nil {
			return fail(err)
		}
	}
	if err = dst.Close(); err != nil {
		return total, nil, err
	}
	return total, h.Sum(nil), nil
}

func writeBackupMetaFile(path string, data []byte, opts backupOptions) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if opts.sync {
		if opts.hooks.syncFile != nil {
			err = opts.hooks.syncFile(f)
		} else {
			err = f.Sync()
		}
		if err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

// syncBackupDir fsyncs a directory so that file creation, rename and removal
// are persisted. Directory fsync is not supported by the platform on Windows,
// where it is a documented no-op; regular files are still fsynced.
func syncBackupDir(dir string) error {
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// resolvePath makes the path absolute and resolves symlinks of the path or of
// its deepest existing ancestor.
func resolvePath(p string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(p))
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(abs); err == nil {
		resolved, rerr := filepath.EvalSymlinks(abs)
		if rerr != nil {
			return "", rerr
		}
		return filepath.Clean(resolved), nil
	}

	parent := filepath.Dir(abs)
	tail := []string{filepath.Base(abs)}
	for {
		if _, err := os.Lstat(parent); err == nil {
			resolved, rerr := filepath.EvalSymlinks(parent)
			if rerr != nil {
				return "", rerr
			}
			return filepath.Clean(filepath.Join(append([]string{resolved}, tail...)...)), nil
		}
		next := filepath.Dir(parent)
		if next == parent {
			return normalizePath(abs), nil
		}
		tail = append([]string{filepath.Base(parent)}, tail...)
		parent = next
	}
}

func mustResolvePath(p string) string {
	r, err := resolvePath(p)
	if err != nil {
		return filepath.Clean(p)
	}
	return r
}

// normalizePath lower-cases the Windows volume letter so path comparisons are
// independent of drive-letter casing.
func normalizePath(p string) string {
	if runtime.GOOS == "windows" {
		vol := filepath.VolumeName(p)
		if vol != "" {
			return strings.ToLower(vol) + p[len(vol):]
		}
	}
	return p
}

func samePath(a, b string) bool {
	return normalizePath(a) == normalizePath(b)
}

// sameOrWithin reports whether target is base or located inside base.
func sameOrWithin(base, target string) bool {
	rel, err := filepath.Rel(normalizePath(base), normalizePath(target))
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
