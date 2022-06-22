package mount

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/filecoin-project/dagstore/shard"

	"github.com/filecoin-project/dagstore/throttle"
	logging "github.com/ipfs/go-log/v2"
)

var (
	readBufferSize = 32 * 1024
	log            = logging.Logger("dagstore/upgrader")
)

type TransientSpaceManager interface {
	Reserve(ctx context.Context, k shard.Key, count int64, toReserve int64) (reserved int64, err error)
	Release(ctx context.Context, k shard.Key, n int64) error
}

// Upgrader is a bridge to upgrade any Mount into one with full-featured
// Reader capabilities, whether the original mount is of remote or local kind.
// It does this by managing a local transient copy.
//
// If the underlying mount is fully-featured, the Upgrader has no effect, and
// simply passes through to the underlying mount.
type Upgrader struct {
	rootdir     string
	underlying  Mount
	throttler   throttle.Throttler
	key         string
	passthrough bool

	// paths: pathComplete is the path of transients that are
	// completely downloaded; pathPartial is the path where in-progress
	// downloads are placed. Once fully downloaded, the file is renamed to
	// pathComplete.
	pathComplete string
	pathPartial  string

	lk            sync.Mutex
	path          string // guarded by lk
	ready         bool   // guarded by lk
	transientSize int64  // guarded by lk

	// once guards deduplicates concurrent refetch requests; the caller that
	// gets to run stores the result in onceErr, for other concurrent callers to
	// consume it.
	once    *sync.Once // guarded by lk
	onceErr error      // NOT guarded by lk; access coordinated by sync.Once

	fetches int32 // guarded by atomic

	tsm TransientSpaceManager
}

var _ Mount = (*Upgrader)(nil)

// Upgrade constructs a new Upgrader for the underlying Mount. If provided, it
// will reuse the file in path `initial` as the initial transient copy. Whenever
// a new transient copy has to be created, it will be created under `rootdir`.
func Upgrade(underlying Mount, throttler throttle.Throttler, rootdir, key string, initial string, tsm TransientSpaceManager) (*Upgrader, error) {
	ret := &Upgrader{
		underlying:   underlying,
		key:          key,
		rootdir:      rootdir,
		once:         new(sync.Once),
		throttler:    throttler,
		pathComplete: filepath.Join(rootdir, "transient-"+key+".complete"),
		pathPartial:  filepath.Join(rootdir, "transient-"+key+".partial"),
		tsm:          tsm,
	}
	if ret.rootdir == "" {
		ret.rootdir = os.TempDir() // use the OS' default temp dir.
	}

	switch info := underlying.Info(); {
	case !info.AccessSequential:
		return nil, fmt.Errorf("underlying mount must support sequential access")
	case info.AccessSeek && info.AccessRandom:
		ret.passthrough = true
		return ret, nil
	}

	if initial != "" {
		if fi, err := os.Stat(initial); err == nil {
			log.Debugw("initialized with existing transient that's alive", "shard", key, "path", initial)
			ret.path = initial
			ret.ready = true
			ret.transientSize = fi.Size()
			return ret, nil
		}
	}

	return ret, nil
}

func (u *Upgrader) Fetch(ctx context.Context) (Reader, error) {
	if u.passthrough {
		log.Debugw("fully capable mount; fetching from underlying", "shard", u.key)
		return u.underlying.Fetch(ctx)
	}

	// determine if the transient is still alive.
	// if not, delete it, get the current sync.Once and trigger a refresh.
	// after it's done, open the resulting transient.
	u.lk.Lock()
	if u.ready {
		log.Debugw("transient local copy exists; check liveness", "shard", u.key, "path", u.path)
		if _, err := os.Stat(u.path); err == nil {
			log.Debugw("transient copy alive; not refetching", "shard", u.key, "path", u.path)
			defer u.lk.Unlock()
			return os.Open(u.path)
		} else {
			u.ready = false
			log.Debugw("transient copy dead; removing and refetching", "shard", u.key, "path", u.path, "error", err)
			if err := os.Remove(u.path); err != nil {
				log.Warnw("refetch: failed to remove transient; garbage left behind", "shard", u.key, "dead_path", u.path, "error", err)
			}
		}
		// TODO add size check.
	}
	// transient appears to be dead, refetch.
	// get the current sync under the lock, use it to deduplicate concurrent fetches.
	once := u.once
	u.lk.Unlock()

	once.Do(func() {
		// do the refetch; abort and remove/reset the partial if it fails.
		// perform outside the lock as this is a long-running operation.
		// u.onceErr is only written by the goroutine that gets to run sync.Once
		// and it's only read after it finishes.

		u.onceErr = u.refetch(ctx, u.pathPartial)
		if u.onceErr != nil {
			log.Warnw("failed to refetch", "shard", u.key, "error", u.onceErr)
			return
		}

		var fi os.FileInfo
		fi, u.onceErr = os.Stat(u.pathPartial)
		if u.onceErr != nil {
			log.Warnw("failed to stat refetched file", "shard", u.key, "error", u.onceErr)
			return
		}

		// rename the partial file to a non-partial file.
		// set the new transient path under a lock, and recycle the sync.Once.
		// if the target file exists, os.Rename replaces it.
		if err := os.Rename(u.pathPartial, u.pathComplete); err != nil {
			log.Warnw("failed to rename partial transient", "shard", u.key, "from_path", u.pathPartial, "to_path", u.pathComplete, "error", err)
		}

		u.lk.Lock()
		u.path = u.pathComplete
		u.transientSize = fi.Size()
		u.ready = true
		u.once = new(sync.Once)
		u.lk.Unlock()

		log.Debugw("transient path updated after refetching", "shard", u.key, "new_path", u.pathComplete)
	})

	// There's a tiny, tiny possibility of a race here if a new refetch with
	// the recycled sync.Once comes in an updates onceErr before the waiters
	// have read it. Not worth making perfect now, but we should revisit
	// this recipe. Can probably use a sync.Cond with atomic counters.
	// TODO revisit this.
	err := u.onceErr
	if err != nil {
		return nil, fmt.Errorf("mount fetch failed: %w", err)
	}

	log.Debugw("refetched successfully", "shard", u.key, "path", u.pathComplete)
	return os.Open(u.pathComplete)
}

func (u *Upgrader) Info() Info {
	return Info{
		Kind:             KindLocal,
		AccessSequential: true,
		AccessSeek:       true,
		AccessRandom:     true,
	}
}

func (u *Upgrader) Stat(ctx context.Context) (Stat, error) {
	if u.path != "" {
		if stat, err := os.Stat(u.path); err == nil {
			ret := Stat{Exists: true, Size: stat.Size()}
			return ret, nil
		}
	}
	return u.underlying.Stat(ctx)
}

// TransientPath returns the local path of the transient file, if one exists.
func (u *Upgrader) TransientPath() string {
	u.lk.Lock()
	defer u.lk.Unlock()

	return u.path
}

// TimesFetched returns the number of times that the underlying has
// been fetched.
func (u *Upgrader) TimesFetched() int {
	return int(atomic.LoadInt32(&u.fetches))
}

// Underlying returns the underlying mount.
func (u *Upgrader) Underlying() Mount {
	return u.underlying
}

func (u *Upgrader) Serialize() *url.URL {
	return u.underlying.Serialize()
}

func (u *Upgrader) Deserialize(url *url.URL) error {
	return u.underlying.Deserialize(url)
}

// TODO implement
func (u *Upgrader) Close() error {
	log.Warnf("Upgrader.Close() not implemented yet; call will no-op")
	return nil
}

func (u *Upgrader) refetch(ctx context.Context, path string) error {
	log.Debugw("actually refetching", "shard", u.key)

	// sanity check on underlying mount.
	stat, err := u.underlying.Stat(ctx)
	if err != nil {
		return fmt.Errorf("underlying mount stat returned error: %w", err)
	} else if !stat.Exists {
		return fmt.Errorf("underlying mount no longer exists")
	}

	// throttle only if the file is ready; if it's not ready, we would be
	// throttling and then idling.
	t := u.throttler
	if !stat.Ready {
		log.Debugw("underlying mount is not ready; will skip throttling", "shard", u.key)
		t = throttle.Noop()
	} else {
		log.Debugw("underlying mount is ready; will throttle fetch and copy", "shard", u.key)
	}

	err = t.Do(ctx, func(ctx context.Context) error {
		return u.download(ctx, path)
	})
	if err != nil {
		return fmt.Errorf("failed to fetch and copy underlying mount to transient file: %w", err)
	}

	return nil
}

// DeleteTransient deletes the transient associated with this Upgrader, if
// one exists. It is the caller's responsibility to ensure the transient is
// not in use. If the tracked transient is gone, this will reset the internal
// state to "" (no transient) to enable recovery.
// This method should ONLY be called from the dagstore event loop.
func (u *Upgrader) DeleteTransient() (release int64, err error) {
	u.lk.Lock()
	defer u.lk.Unlock()

	if u.path == "" {
		log.Debugw("transient is empty; nothing to remove", "shard", u.key)
		return 0, nil // nothing to do.
	}

	// refuse to delete the transient if it's not being managed by us (i.e. in
	// our transients root directory).
	if _, err := filepath.Rel(u.rootdir, u.path); err != nil {
		log.Debugw("transient is not owned by us; nothing to remove", "shard", u.key)
		return 0, nil
	}

	// remove the transient and clear it always, even if os.Remove
	// returns an error. This allows us to recover from errors like the user
	// deleting the transient we're currently tracking.
	err = os.Remove(u.path)
	u.path = ""
	u.ready = false
	size := u.transientSize
	u.transientSize = 0
	log.Debugw("deleted existing transient", "shard", u.key, "path", u.path, "error", err)
	return size, err
}

func (u *Upgrader) download(ctx context.Context, dstPath string) error {
	st, err := u.underlying.Stat(ctx)
	if err != nil {
		return fmt.Errorf("failed to stat underlying: %w", err)
	}

	// create the destination file we need to download the mount contents to.
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer dst.Close()

	from, err := u.underlying.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch underlying mount: %w", err)
	}

	totalReserved, downloadErr := func() (int64, error) {
		reserveCount := int64(0)
		totalReserved := int64(0)
		for {
			if ctx.Err() != nil {
				return totalReserved, ctx.Err()
			}
			reserved, err := u.tsm.Reserve(ctx, shard.KeyFromString(u.key), reserveCount, st.Size)
			if err != nil {
				return totalReserved, fmt.Errorf("failed to make a reservation: %w", err)
			}
			reserveCount++
			totalReserved = totalReserved + reserved

			hasMore, err := downloadNBytes(ctx, from, reserved, dst)
			if err != nil {
				return totalReserved, fmt.Errorf("failed to download: %w", err)
			}

			fi, err := dst.Stat()
			if err != nil {
				return totalReserved, fmt.Errorf("failed to stat output file: %w", err)
			}

			// if we've already read as many bytes as the the size indicated by the underlying mount,
			// short-circuit and return here instead if doing one more round to see an EOF.
			if st.Size != 0 && fi.Size() == st.Size {
				return totalReserved, nil
			}

			if !hasMore {
				return totalReserved, nil
			}
		}
	}()

	releaseOnError := func(rerr error) error {
		// if we fail to remove the file, something has gone wrong and we shouldn't release the bytes to be on the safe side.
		if err := os.Remove(dstPath); err != nil {
			log.Errorw("failed to remove transient for failed download, will not release reservation", "path", dstPath, "error", err)
			return rerr
		}
		if err := u.tsm.Release(ctx, shard.KeyFromString(u.key), totalReserved); err != nil {
			log.Errorw("failed to release reservation", "error", err)
		}
		return rerr
	}

	// if there was an error downloading, remove the downloaded file and release all the bytes we'd reserved.
	if downloadErr != nil {
		return releaseOnError(downloadErr)
	}

	// if the download finished successfully and we've reserved more bytes than we've downloaded,
	// release (totalReserved - file size) bytes as we've not used those bytes.
	fi, statErr := dst.Stat()
	if statErr != nil {
		return releaseOnError(statErr)
	}
	if totalReserved > fi.Size() {
		return u.tsm.Release(ctx, shard.KeyFromString(u.key), totalReserved-fi.Size())
	}

	return nil
}

// downloadNBytes copies and writes at most `n` bytes from the given reader to the given output file.
// It returns true if there are still more bytes to be read from the reader and false otherwise.
// It will return true if and only if err == nil.
// If it returns false and err == nil it means that we've successfully exhausted the reader.
func downloadNBytes(ctx context.Context, rd Reader, n int64, out *os.File) (hasMore bool, err error) {
	remaining := n
	buf := make([]byte, readBufferSize)
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if remaining <= 0 {
			return true, nil
		}

		if len(buf) > int(remaining) {
			buf = buf[0:remaining]
		}

		read, rerr := rd.Read(buf)
		remaining = remaining - int64(read)
		if read > 0 {
			written, werr := out.Write(buf[0:read])
			if werr != nil {
				return false, fmt.Errorf("failed to write: %w", werr)
			}

			if written < 0 || read != written {
				return false, fmt.Errorf("read-write mismatch writing to the output file, read=%d, written=%d", read, written)
			}
		}

		if rerr == io.EOF {
			return false, nil
		}
		if rerr != nil {
			return false, fmt.Errorf("error while reading from the underlying mount: %w", err)
		}
	}
}
