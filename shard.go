package dagstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"

	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"
)

// TODO fix visibility of methods.
// TODO implement refcounting incr/decr
// TODO implement transient copy abstraction / scrap area
// TODO ensure everything is thread-safe
// TODO figure out if we want to use goroutines anywhere
// TODO figure out how to heal/treat failed/unavailable shards -- notify application?

type ShardState int

const (
	// ShardStateNew indicates that a shard has just been registered and is
	// about to be processed.
	ShardStateNew ShardState = iota

	// ShardStateFetching indicates that we're fetching the shard data from
	// a remote mount.
	ShardStateFetching

	// ShardStateFetched indicates that we're fetching the shard data from
	// a remote mount.
	ShardStateFetched

	// ShardStateIndexing indicates that we are indexing the shard.
	ShardStateIndexing

	// ShardStateActive indicates that the shard has been initialized and is
	// active for serving queries.
	ShardStateActive

	// ShardStateErrored indicates that an unexpected error was encountered
	// during a shard operation, and therefore the shard needs to be recovered.
	ShardStateErrored

	// ShardStateUnknown indicates that it's not possible to determine the state
	// of the shard, because an internal error occurred.
	ShardStateUnknown
)

type ShardAvailability int

const (
	// ShardAvailabilityUnknown is the initial availability state.
	ShardAvailabilityUnknown ShardAvailability = iota

	// ShardAvailabilityLocal indicates that the system knows about this shard and
	// is capable of serving data from it instantaneously, because (a) an index
	// exists and is loaded, and (b) data is locally accessible (e.g. it doesn't
	// need to be fetched from a remote mount).
	ShardAvailabilityLocal

	// ShardAvailabilityRemote indicates that the system knows about this shard,
	// but is not capable of serving data from it because the shard is being
	// initialized, or the mount is not available locally, but still accessible
	// with work (e.g. fetching from a remote location).
	ShardAvailabilityRemote

	// ShardAvailabilityGone represents that this shard is gone from its mount,
	// and therefore is no longer available.
	ShardAvailabilityGone

	// ShardAvailabilityErrored indicates that it's not possible to determine
	// the availability of the shard, because the mount returned an error.
	ShardAvailabilityErrored
)

// Shard encapsulates the state of a shard inside the DAG store.
type Shard struct {
	// immutable.
	key   shard.Key
	mount mount.Mount

	// guarded by lock.
	lk              sync.RWMutex
	state           ShardState
	availability    ShardAvailability
	stateErr        error    // populated if shard state is errored.
	availabilityErr error    // populated if shard availability is errored.
	refs            uint32   // count of DAG accessors currently open
	transient       *os.File // transient file
}

func (s *Shard) initializeShard() (index.FullIndex, error) {
	// must only be called when shard is new.
	s.lk.Lock()
	if s.state != ShardStateNew {
		s.lk.Unlock()
		return nil, fmt.Errorf("expected shard state to be new")
	}
	s.lk.Unlock()

	err := s.ensureLocal()
	if err != nil {
		s.failShard(err)
		return nil, fmt.Errorf("failed to ensure a local copy of shard: %w", err)
	}

	idx, err := s.initializeIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to index shard: %w", err)
	}
	return idx, nil
}

// ensureLocal ensures that a shard is available locally.
func (s *Shard) ensureLocal() error {
	// refresh the availability of the shard.
	availability, err := s.refreshAvailabilityLocked()

	switch availability {
	case ShardAvailabilityLocal:
		log.Infow("init: shard mount is local; no need to fetch", "shard", s.key)

	case ShardAvailabilityRemote:
		log.Infow("init: shard mount is remote; fetching", "shard", s.key)

		s.setState(ShardStateFetching)

		if err := s.fetchRemote(); err != nil {
			log.Warnw("init: failed to fetch shard from mount", "shard", s.key, "mount", s.mount.Info().URL, "err", err)
			err := fmt.Errorf("failed to ensureLocal shard: %w", err)
			s.failShard(err)
			return err
		}

		s.setState(ShardStateFetched)

		log.Infow("init: successfully fetched shard from mount", "shard", s.key, "mount", s.mount.Info().URL)
		availability, err = s.refreshAvailability()

	case ShardAvailabilityGone:
		log.Warnw("init: shard mount is remote, but remote is gone; erroring", "shard", s.key)
		err := fmt.Errorf("%w: remote mount is gone", ErrShardInitializationFailed)
		s.failShard(err)
		return err

	case ShardAvailabilityUnknown:
		// TODO shard state?
		log.Warnw("init: shard mount is remote, but failed to fetch availability", "shard", s.key)

	}

	// at this point we have guaranteed that we have a local copy, either
	// because the mount is local, or because there's a local copy available.
	return err
}

func (s *Shard) setState(state ShardState) {
	s.lk.Lock()
	s.state = state
	s.lk.Unlock()
}

// initializeIndex must be called when the shard availability is local.
//
// TODO: allow to call this on any shard availability.
func (s *Shard) initializeIndex() (index.FullIndex, error) {
	s.lk.RLock()
	if s.state == ShardStateErrored {
		err := fmt.Errorf("shard errored: %w", s.stateErr)
		s.lk.RUnlock()
		return nil, err
	}
	if s.availability != ShardAvailabilityLocal {
		s.lk.RUnlock()
		// TODO add a string representation of current availability in error message
		return nil, fmt.Errorf("cannot perform indexing on shard that is not available locally")
	}
	s.lk.RUnlock()

	var reader io.ReadSeekCloser
	if s.transient != nil {
		_, err := s.transient.Seek(0, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to rewind transient file to beginning: %w", err)
		}
		reader = s.transient
	} else {
		if !s.mount.Info().Seekable {
			return nil, fmt.Errorf("indexing with local mounts requires seekable streams")
		}
		var err error
		reader, err = s.mount.FetchSeek(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
	}

	// read the CAR version.
	ver, err := car.ReadVersion(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read car version: %w", err)
	}

	switch ver {
	case 2:
		// this a carv2, we need to look at the index to find out if it
		// carries an inline index or not.
		var header car.Header
		if _, err := header.ReadFrom(reader); err != nil {
			return nil, fmt.Errorf("failed to read carv2 header: %w", err)
		}
		if header.HasIndex() {
			// this CARv2 has an index, let's extract it.
			offset := int64(header.IndexOffset)
			_, err := reader.Seek(offset, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to seek to carv2 index (offset: %d): %w", offset, err)
			}
			idx, err := carindex.ReadFrom(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to read carv2 index: %w", err)
			}
			return idx, nil // TODO integrate https://github.com/filecoin-project/dagstore/pull/19
		} else {
			return nil, fmt.Errorf("processing of non-indexed carv2 not implemented yet") // TODO implement
		}
	case 1:
		return nil, fmt.Errorf("processing of carv1 not implemented yet") // TODO implement
	default:
		return nil, fmt.Errorf("unrecognized car version: %d", ver)
	}
}

// CleanTransient cleans up local transient copies. It will reject to proceed if
// there are active readers.
func (s *Shard) CleanTransient() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	// check if we have readers and refuse to clean if so.
	if s.refs != 0 {
		return fmt.Errorf("failed to delete shard: %w", ErrShardInUse)
	}

	if s.transient == nil {
		// nothing to do.
		return nil
	}

	// we can safely remove the transient.
	_ = s.transient.Close()
	err := os.Remove(s.transient.Name())
	if err == nil {
		s.transient = nil
	}

	// refresh the availability.
	_, _ = s.refreshAvailability()
	return nil
}

// refreshAvailabilityLocked calls refreshAvailability under the shard lock.
func (s *Shard) refreshAvailabilityLocked() (ret ShardAvailability, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.refreshAvailability()
}

// refreshAvailability refreshes the shard's availability. It must be called
// under a lock.
func (s *Shard) refreshAvailability() (ret ShardAvailability, err error) {
	source := s.mount.Info().Source
	switch source {
	case mount.SourceRemote:
		if s.transient != nil { // we may have a transient copy, check if ok.
			if _, err = s.transient.Stat(); err == nil {
				// local file exists, shard has local availability.
				ret = ShardAvailabilityLocal
				break
			}
		}
		// otherwise check remote.
		ret, err = s.checkRemote()
	case mount.SourceLocal:
		var stat mount.Stat
		stat, err = s.mount.Stat()
		if err == nil && stat.Exists {
			ret = ShardAvailabilityLocal
		} else if err == nil && !stat.Exists {
			ret = ShardAvailabilityGone
		} else {
			ret = ShardAvailabilityUnknown
		}
	default:
		panic("unrecognized mount source")
	}

	s.lk.Lock()
	s.availability = ret
	s.availabilityErr = err
	s.lk.Unlock()

	return ret, err
}

func (s *Shard) checkRemote() (ShardAvailability, error) {
	stat, err := s.mount.Stat()
	if err != nil {
		return ShardAvailabilityUnknown, err
	}
	if stat.Exists {
		return ShardAvailabilityRemote, nil
	}
	return ShardAvailabilityGone, nil
}

func (s *Shard) fetchRemote() error {
	to, err := os.CreateTemp("dagstore", "transient")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	from, err := s.mount.Fetch(context.Background())
	if err != nil {
		return fmt.Errorf("failed to fetch from mount: %w", err)
	}

	n, err := io.Copy(to, from)
	if err != nil {
		return fmt.Errorf("failed to copy from remote mount: %w", err)
	}

	// save the transient file to shard state.
	s.transient = to

	log.Infow("copied remote mount into local transient copy",
		"mount", s.mount.Info().URL, "transient", to.Name(), "bytes_written", n)
	return nil
}

func (s *Shard) failShard(err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	s.state = ShardStateErrored
	s.stateErr = err
}
