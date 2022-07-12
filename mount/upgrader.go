package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/xerrors"

	"github.com/jpillora/backoff"

	"github.com/filecoin-project/dagstore/shard"

	"github.com/filecoin-project/dagstore/throttle"
	logging "github.com/ipfs/go-log/v2"
)

var (
	readBufferSize = 64 * 1024 // 64 K
	log            = logging.Logger("dagstore/upgrader")

	// 5s, 7s, 11s, 16s, 25s, 38s, 1m
	minReservationBackOff  = 5 * time.Second // minimum backoff time before making a new reservation attempt
	maxReservationBackOff  = 1 * time.Minute //  maximum backoff time before making a new reservation attempt
	factor                 = 1.5             // factor by which to increase the backoff time between reservation requests
	maxReservationAttempts = 7               // maximum number of reservation attempts to make before giving up

	// ErrNotEnoughSpaceInTransientsDir is returned when we are unable to allocate a requested reservation
	// for a transient because we do not have enough space in the transients directory.
	ErrNotEnoughSpaceInTransientsDir = errors.New("not enough space in the transient directory")
)

// TransientAllocator manages allocations for downloading a transient whose size is not known upfront.
// It reserves space for the transient with the allocator when the transient is being downloaded and releases unused reservations
// back to the allocator after the transient download finishes or errors out.
type TransientAllocator interface {
	// Reserve attempts to reserve `toReserve` bytes for the transient and returns the number
	// of bytes actually reserved or any error encountered when trying to make the reservation.
	// The `nPrevReservations` param denotes the number of previous successful reservations
	// the caller has made for an ongoing download.
	// Note: reserved != 0 if and only if err == nil.
	Reserve(ctx context.Context, k shard.Key, nPrevReservations int64, toReserve int64) (reserved int64, err error)

	// Release releases back `toRelease` bytes to the allocator.
	// The bytes that are released here are the ones that were previously obtained by making a `Reserve` call to the allocator
	// but were not used either because the transient download errored out or because the size of the downloaded transient
	// was less than the number of bytes actually reserved.
	Release(ctx context.Context, k shard.Key, toRelease int64) error
}

// TransientDownloader manages the process of downloading a transient locally from a Mount
// when we are upgrading a Mount that does NOT have random access capabilities.
type TransientDownloader interface {
	// Download copies the transient from the given underlying Mount to the given output path.
	Download(ctx context.Context, underlying Mount, outpath string) error
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
	downloader  TransientDownloader

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
}

var _ Mount = (*Upgrader)(nil)

// Upgrade constructs a new Upgrader for the underlying Mount. If provided, it
// will reuse the file in path `initial` as the initial transient copy. Whenever
// a new transient copy has to be created, it will be created under `rootdir`.
func Upgrade(underlying Mount, throttler throttle.Throttler, rootdir, key string, initial string, downloader TransientDownloader) (*Upgrader, error) {
	ret := &Upgrader{
		underlying:   underlying,
		key:          key,
		downloader:   downloader,
		rootdir:      rootdir,
		once:         new(sync.Once),
		throttler:    throttler,
		pathComplete: filepath.Join(rootdir, "transient-"+key+".complete"),
		pathPartial:  filepath.Join(rootdir, "transient-"+key+".partial"),
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

func (u *Upgrader) FetchNoDownload(ctx context.Context) (Reader, error) {
	if u.passthrough {
		log.Debugw("fully capable mount; fetching from underlying", "shard", u.key)
		return u.underlying.Fetch(ctx)
	}

	// return a reader if the transient is still alive, otherwise return an error.
	u.lk.Lock()
	defer u.lk.Unlock()

	if !u.ready {
		return nil, ErrTransientNotFound
	}

	log.Debugw("transient local copy exists; check liveness", "shard", u.key, "path", u.path)
	var err error
	if _, err = os.Stat(u.path); err == nil {
		log.Debugw("transient copy alive; not refetching", "shard", u.key, "path", u.path)
		return os.Open(u.path)
	}
	u.ready = false
	log.Debugw("transient copy dead; removing and refetching", "shard", u.key, "path", u.path, "error", err)
	if err := os.Remove(u.path); err != nil {
		log.Warnw("refetch: failed to remove transient; garbage left behind", "shard", u.key, "dead_path", u.path, "error", err)
	}

	return nil, ErrTransientNotFound
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
			// refetch can and should be retried if it errors out because of not enough
			// space in the transients directory.
			if xerrors.Is(u.onceErr, ErrNotEnoughSpaceInTransientsDir) {
				u.lk.Lock()
				u.once = new(sync.Once)
				u.lk.Unlock()
			}

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

func (u *Upgrader) refetch(ctx context.Context, outpath string) error {
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
		return u.downloader.Download(ctx, u.underlying, outpath)
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

// SimpleDownloader simply copies the transient to a given output path from the given underlying mount.
// It does not make any reservation requests when downloading the transient.
// Use this when automated GC is disabled.
type SimpleDownloader struct{}

func (s *SimpleDownloader) Download(ctx context.Context, underlying Mount, outpath string) error {
	// fetch from underlying and copy.
	from, err := underlying.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch from underlying mount: %w", err)
	}
	defer from.Close()

	into, err := os.Create(outpath)
	if err != nil {
		return fmt.Errorf("failed to create out file: %w", err)
	}
	defer into.Close()

	_, err = io.Copy(into, from)
	return err
}

// ReservationGatedDownloaderOpt are options for configuring the ReservationGatedDownloader.
type ReservationGatedDownloaderOpt func(r *ReservationGatedDownloader)

// ReservationBackOffRetryOpt configures the backoff-retry parameters for the reservation attempts when downloading transients.
func ReservationBackOffRetryOpt(minBackoff, maxBackoff time.Duration, factor, maxReservationAttempts float64) ReservationGatedDownloaderOpt {
	return func(r *ReservationGatedDownloader) {
		r.minBackOffWait = minBackoff
		r.maxBackoffWait = maxBackoff
		r.backOffFactor = factor
		r.maxReservationAttempts = maxReservationAttempts
	}
}

type ReservationGatedDownloader struct {
	key                    shard.Key
	knownTransientSize     int64
	allocator              TransientAllocator
	minBackOffWait         time.Duration
	maxBackoffWait         time.Duration
	backOffFactor          float64
	maxReservationAttempts float64
}

func NewReservationGatedDownloader(key shard.Key, knownTransientSize int64, allocator TransientAllocator, opts ...ReservationGatedDownloaderOpt) *ReservationGatedDownloader {
	r := &ReservationGatedDownloader{
		key:                    key,
		knownTransientSize:     knownTransientSize,
		allocator:              allocator,
		minBackOffWait:         minReservationBackOff,
		maxBackoffWait:         maxReservationBackOff,
		backOffFactor:          factor,
		maxReservationAttempts: float64(maxReservationAttempts),
	}

	for _, o := range opts {
		o(r)
	}

	return r
}

func (r *ReservationGatedDownloader) Download(ctx context.Context, underlying Mount, outpath string) error {
	// Do we already know the size of the transient to be downloaded upfront ?
	// If we do, this will allows us to request a reservation of that size to the allocator.
	// 1. Stat the Mount to see if it has a fixed known size
	// 2. If the Mount does not have the size, check if we have a memoized transient size from a previous successful download.
	// 3. Otherwise, we do not know the size of the transient upfront and we will have to rely on the allocator
	//    to give us reservations based on it it's internal configuration.
	var toReserve int64
	{
		st, err := underlying.Stat(ctx)
		if err != nil {
			return fmt.Errorf("failed to stat underlying: %w", err)
		}
		toReserve = st.Size
		if toReserve == 0 {
			toReserve = r.knownTransientSize
		}
	}
	transientSizeKnown := toReserve != 0

	// create the destination file we need to download the mount contents to.
	dst, err := os.Create(outpath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer dst.Close()

	from, err := underlying.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch underlying mount: %w", err)
	}

	// reserveWithBackoff attempts to make a reservation with the allocator using back-off retry mechanism
	// in case of a failue.
	reserveWithBackoff := func(nPrevReservations int64) (reserved int64, err error) {
		// back-off retry before failing
		backoff := &backoff.Backoff{
			Min:    r.minBackOffWait,
			Max:    r.maxBackoffWait,
			Factor: r.backOffFactor,
			Jitter: true,
		}
		for {
			reserved, err := r.allocator.Reserve(ctx, r.key, nPrevReservations, toReserve)
			if err == nil {
				return reserved, nil
			}
			nAttempts := backoff.Attempt() + 1
			if nAttempts >= r.maxReservationAttempts {
				return reserved, err
			}

			dur := backoff.Duration()
			t := time.NewTimer(dur)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done():
				return reserved, ctx.Err()
			}
		}
	}

	totalReserved := int64(0)

	downloadErr := func() error {
		nPrevReservations := int64(0)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			reserved, err := reserveWithBackoff(nPrevReservations)
			if err != nil {
				return fmt.Errorf("failed to make a reservation: %w", err)
			}
			nPrevReservations++
			totalReserved = totalReserved + reserved

			hasMore, err := downloadNBytes(ctx, from, reserved, dst)
			if err != nil {
				return fmt.Errorf("failed to download: %w", err)
			}

			// if we know the size of the transient upfront and we've already read as many bytes,
			// short-circuit and return here instead of doing one more round to see an EOF.
			fi, err := dst.Stat()
			if err != nil {
				return fmt.Errorf("failed to stat output file: %w", err)
			}
			if transientSizeKnown && fi.Size() == toReserve {
				return nil
			}
			if !hasMore {
				return nil
			}
		}
	}()

	// releaseOnError removes the downloaded file and releases ALL bytes we reserved for this download
	// if the download fails with an error.
	releaseOnError := func(rerr error) error {
		// if we fail to remove the file, something has gone wrong and we shouldn't release the bytes to be on the safe side.
		if err := os.Remove(outpath); err != nil {
			log.Errorw("failed to remove transient for failed download, will not release reservation", "path", outpath, "error", err)
			return rerr
		}
		if err := r.allocator.Release(ctx, r.key, totalReserved); err != nil {
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
		return r.allocator.Release(ctx, r.key, totalReserved-fi.Size())
	}

	return nil
}

// downloadNBytes copies and writes at most `n` bytes from the given reader to the given output file.
// It returns true if there are still more bytes to be read from the reader and false otherwise.
// It will return true if and only if err == nil.
// If it returns false and err == nil,	 it means that we've successfully exhausted the reader.
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
