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

	"github.com/filecoin-project/dagstore/throttle"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dagstore/upgrader")

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

	lk    sync.Mutex
	path  string // guarded by lk
	ready bool   // guarded by lk
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
func Upgrade(underlying Mount, throttler throttle.Throttler, rootdir, key string, initial string) (*Upgrader, error) {
	ret := &Upgrader{
		underlying:   underlying,
		key:          key,
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
		if _, err := os.Stat(initial); err == nil {
			log.Debugw("initialized with existing transient that's alive", "shard", key, "path", initial)
			ret.path = initial
			ret.ready = true
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
		// Create a new file in the partial location.
		// os.Create truncates existing files.
		var partial *os.File
		partial, u.onceErr = os.Create(u.pathPartial)
		if u.onceErr != nil {
			return
		}
		defer partial.Close()

		// do the refetch; abort and remove/reset the partial if it fails.
		// perform outside the lock as this is a long-running operation.
		// u.onceErr is only written by the goroutine that gets to run sync.Once
		// and it's only read after it finishes.

		u.onceErr = u.refetch(ctx, partial)
		if u.onceErr != nil {
			log.Warnw("failed to refetch", "shard", u.key, "error", u.onceErr)
			if err := os.Remove(u.pathPartial); err != nil {
				log.Warnw("failed to remove partial transient", "shard", u.key, "path", u.pathPartial, "error", err)
			}
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

func (u *Upgrader) refetch(ctx context.Context, into *os.File) error {
	log.Debugw("actually refetching", "shard", u.key, "path", into.Name())

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
		// fetch from underlying and copy.
		from, err := u.underlying.Fetch(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch from underlying mount: %w", err)
		}
		defer from.Close()

		_, err = io.Copy(into, from)
		return err
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
func (u *Upgrader) DeleteTransient() error {
	u.lk.Lock()
	defer u.lk.Unlock()

	if u.path == "" {
		log.Debugw("transient is empty; nothing to remove", "shard", u.key)
		return nil // nothing to do.
	}

	// refuse to delete the transient if it's not being managed by us (i.e. in
	// our transients root directory).
	if _, err := filepath.Rel(u.rootdir, u.path); err != nil {
		log.Debugw("transient is not owned by us; nothing to remove", "shard", u.key)
		return nil
	}

	// remove the transient and clear it always, even if os.Remove
	// returns an error. This allows us to recover from errors like the user
	// deleting the transient we're currently tracking.
	err := os.Remove(u.path)
	u.path = ""
	u.ready = false
	log.Debugw("deleted existing transient", "shard", u.key, "path", u.path, "error", err)
	return err
}
