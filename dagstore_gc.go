package dagstore

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/filecoin-project/dagstore/shard"
)

// GCResult is the result of performing a GC operation. It holds the results
// from deleting unused transients.
type GCResult struct {
	// Shards includes an entry for every shard whose transient was reclaimed.
	// Nil error values indicate success.
	Shards map[shard.Key]error
	// TransientDirSizeAfterGC is the size of the transients directory after a round of manual GC.
	TransientDirSizeAfterGC int64
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

// gcUptoTarget GC's transients till the size of the transients directory
// goes below the given target. It relies on the configured Garbage Collector to return a list of shards
// whose transients can be GC'd prioritized in the order they should be GC'd in.
// This method can only be called from the event loop.
func (d *DAGStore) gcUptoTarget(target float64) {
	reclaimable := d.garbageCollector.Reclaimable()

	d.lk.RLock()
	defer d.lk.RUnlock()

	var reclaimed []shard.Key

	// attempt to delete transients of reclaimed shards.
	for _, sk := range reclaimable {
		if float64(d.totalTransientDirSize) <= target {
			break
		}

		s := d.shards[sk]

		// only read lock: we're not modifying state, and the mount has its own lock.
		s.lk.RLock()
		freed, err := s.mount.DeleteTransient()
		if err != nil {
			log.Warnw("failed to delete transient", "shard", s.key, "error", err)
		}
		d.totalTransientDirSize -= freed
		reclaimed = append(reclaimed, sk)

		// flush the shard state to the datastore.
		if err := s.persist(d.ctx, d.config.Datastore); err != nil {
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
		s.lk.RUnlock()
	}

	d.garbageCollector.NotifyReclaimed(reclaimed)
}

// manualGC performs DAGStore GC. Refer to DAGStore#GC for more information.
//
// The event loops gives it exclusive execution rights, so while GC is running,
// no other events are being processed.
func (d *DAGStore) manualGC(resCh chan *GCResult) {
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

	// attempt to delete transients of reclaimed shards.
	for _, s := range reclaim {
		// only read lock: we're not modifying state, and the mount has its own lock.
		s.lk.RLock()
		freed, err := s.mount.DeleteTransient()
		if err != nil {
			log.Warnw("failed to delete transient", "shard", s.key, "error", err)
		}
		d.totalTransientDirSize -= freed

		// record the error so we can return it.
		res.Shards[s.key] = err

		// flush the shard state to the datastore.
		if err := s.persist(d.ctx, d.config.Datastore); err != nil {
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
		s.lk.RUnlock()
	}

	res.TransientDirSizeAfterGC = d.totalTransientDirSize

	select {
	case resCh <- res:
	case <-d.ctx.Done():
	}
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
