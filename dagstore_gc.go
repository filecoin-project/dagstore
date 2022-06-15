package dagstore

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/filecoin-project/dagstore/shard"
)

// GCResult is the result of performing a GC operation. It holds the results
// from deleting unused transients.
type GCResult struct {
	// Shards includes an entry for every shard whose transient was reclaimed.
	// Nil error values indicate success.
	Shards map[shard.Key]error
}

// ShardFailures returns the number of shards whose transient reclaim failed.
func (e *GCResult) ShardFailures() int {
	var failures int
	for _, err := range e.Shards {
		if err != nil {
			failures++
		}
	}
	return failures
}

// gc performs DAGStore GC. Refer to DAGStore#GC for more information.
//
// The event loops gives it exclusive execution rights, so while GC is running,
// no other events are being processed.
func (d *DAGStore) gc(resCh chan *GCResult) {
	res := &GCResult{
		Shards: make(map[shard.Key]error),
	}

	// determine which shards can be reclaimed.
	d.lk.RLock()
	var reclaim []*Shard
	for _, s := range d.shards {
		s.lk.RLock()
		if nAcq := len(s.wAcquire); (s.state == ShardStateAvailable || s.state == ShardStateErrored) && nAcq == 0 {
			reclaim = append(reclaim, s)
		}
		s.lk.RUnlock()
	}
	d.lk.RUnlock()

	// attempt to delete transients of reclaimed shards and free up required amount space.
	for _, s := range reclaim {
		// only read lock: we're not modifying state, and the mount has its own lock.
		s.lk.RLock()
		err := s.mount.DeleteTransient()
		if err != nil {
			log.Warnw("failed to delete transient", "shard", s.key, "error", err)
		}

		// record the error so we can return it.
		res.Shards[s.key] = err

		// flush the shard state to the datastore.
		if err := s.persist(d.ctx, d.config.Datastore); err != nil {
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
		s.lk.RUnlock()
	}

	select {
	case resCh <- res:
	case <-d.ctx.Done():
	}
}

// gc performs DAGStore GC. Refer to DAGStore#GC for more information.
//
// The event loops gives it exclusive execution rights, so while GC is running,
// no other events are being processed.
func (d *DAGStore) lruGC(target float64) {
	// sort shards by LRU
	d.lk.RLock()
	var reclaim []*Shard
	for _, s := range d.shards {
		s.lk.RLock()
		reclaim = append(reclaim, s)
		s.lk.RUnlock()
	}
	d.lk.RUnlock()

	sort.Slice(reclaim, func(i, j int) bool {
		reclaim[i].lk.RLock()
		defer reclaim[i].lk.RUnlock()

		reclaim[j].lk.RLock()
		defer reclaim[j].lk.RUnlock()

		return reclaim[i].lastAccessedAt.Before(reclaim[j].lastAccessedAt)
	})

	// TODO What should we do about shards that are currently being initialised/acquired ?
	// Removing the transient here will fail those operations
	// The problem is those operations happen async outside the event loop.
	// I think as a start, we should only enable automated LRU GC if client
	// is okay with ongoing ops failing because of it i.e. "forced" mode.
	// If client is NOT okay with the "forced" mode, client can fallback to the existing safe GC mechanism
	// where we do NOT mess with shards that are being initialised or served.

	// attempt to delete transients of reclaimed shards and free up required amount space.
	for _, s := range reclaim {
		// break when transient directory achieves required size
		size, err := d.transientDirSize()
		if err != nil {
			continue
		}
		if float64(size) < target {
			break
		}

		// only read lock: we're not modifying state, and the mount has its own lock.
		// Is it okay to have a 'force' mode where it is okay to remove those transients and let the ops fail ?
		s.lk.RLock()
		err = s.mount.DeleteTransient()
		if err != nil {
			log.Warnw("failed to delete transient", "shard", s.key, "error", err)
		}

		// flush the shard state to the datastore.
		if err := s.persist(d.ctx, d.config.Datastore); err != nil {
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
		s.lk.RUnlock()
	}
}

func (d *DAGStore) transientDirSize() (int64, error) {
	var size int64
	err := filepath.Walk(d.config.TransientsDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

// clearOrphaned removes files that are not referenced by any mount.
//
// This is only safe to be called from the constructor, before we have
// queued tasks.
func (d *DAGStore) clearOrphaned() error {
	referenced := make(map[string]struct{})

	for _, s := range d.shards {
		t := s.mount.TransientPath()
		referenced[t] = struct{}{}
	}

	// Walk the transients dir and delete unreferenced files.
	err := filepath.WalkDir(d.config.TransientsDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if _, ok := referenced[path]; !ok {
			if err := os.Remove(path); err != nil {
				log.Warnw("failed to delete orphaned file", "path", path, "error", err)
			} else {
				log.Infow("deleted orphaned file", "path", path)
			}
		}
		return nil
	})

	return err
}
