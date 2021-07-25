package dagstore

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/filecoin-project/dagstore/shard"
)

// GCResult is the result of performing a GC operation. It holds the results
// from deleting unused transients, and the result from reconciling the
// transients dir with files referenced from mounts.
type GCResult struct {
	// Shards includes an entry for every shard whose transient was reclaimed.
	// Nil error values indicate success.
	Shards map[shard.Key]error
	// Reconcile stores the result from the reconciliation phase.
	Reconcile error
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

	// A. Delete transients for unused and errored shards.

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
		s.lk.Lock()
		err := s.mount.DeleteTransient()
		if err != nil {
			log.Warnw("failed to delete transient", "shard", s.key, "error", err)
		}

		// record the error so we can return it.
		res.Shards[s.key] = err

		// flush the shard state to the datastore.
		if err := s.persist(d.config.Datastore); err != nil {
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
		s.lk.Unlock()
	}

	// B. Remove files that aren't referenced by the mounts.

	referenced := make(map[string]struct{})
	d.lk.RLock()
	for _, s := range d.shards {
		t, p := s.mount.TransientPath(), s.mount.PartialPath()
		referenced[t] = struct{}{}
		referenced[p] = struct{}{}
	}
	d.lk.RUnlock()

	// Walk the transients dir and delete unreferenced files.
	res.Reconcile = filepath.WalkDir(d.config.TransientsDir, func(path string, d fs.DirEntry, err error) error {
		if _, ok := referenced[path]; !ok {
			if err := os.Remove(path); err != nil {
				log.Warnw("failed to garbage collec file", "path", path, "error", err)
			} else {
				log.Infow("garbage collected file", "path", path)
			}
		}
		return nil
	})

	select {
	case resCh <- res:
	case <-d.ctx.Done():
	}
}
