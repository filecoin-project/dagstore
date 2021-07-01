package dagstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"
)

// TODO implement ReleaseShard when the ShardAccessor is closed.
// TODO ability to recover from a failed init.
// TODO managing transients

var log = logging.Logger("dagstore")

var (
	// ErrShardUnknown is the error returned when the requested shard is
	// not known to the DAG store.
	ErrShardUnknown = errors.New("shard not found")

	// ErrShardExists is the error returned upon registering a duplicate shard.
	ErrShardExists = errors.New("shard already exists")

	// ErrShardInitializationFailed is returned when shard initialization fails.
	ErrShardInitializationFailed = errors.New("shard initialization failed")

	// ErrShardInUse is returned when the user attempts to destroy a shard that
	// is in use.
	ErrShardInUse = errors.New("shard in use")
)

// DAGStore is the central object of the DAG store.
type DAGStore struct {
	lk      sync.RWMutex
	mounts  *mount.Registry
	shards  map[shard.Key]*Shard
	config  Config
	indices index.FullIndexRepo
	// scratch *ScratchSpace

	// event loop channel.
	taskCh chan *Task
	// overflow is an unbounded list where tasks are queued when the taskCh is backlogged.
	overflow []*Task

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

// TODO implement transient local abstraction / scratch area
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

	// ShardStateAvailable indicates that the shard has been initialized and is
	// active for serving queries.
	ShardStateAvailable

	ShardStateServing

	// ShardStateErrored indicates that an unexpected error was encountered
	// during a shard operation, and therefore the shard needs to be recovered.
	ShardStateErrored

	// ShardStateUnknown indicates that it's not possible to determine the state
	// of the shard, because an internal error occurred.
	ShardStateUnknown
)

// Shard encapsulates the state of a shard inside the DAG store.
type Shard struct {
	sync.RWMutex

	// immutable.
	key   shard.Key
	mount *mount.Upgrader

	wRegister chan ShardResult
	wAcquire  []chan ShardResult
	wDestroy  chan ShardResult // TODO implement destroy wait

	state   ShardState
	err     error // populated if shard state is errored.
	indexed bool

	refs uint32 // count of DAG accessors currently open

}

func (s *Shard) failShard(err error) {
	s.state = ShardStateErrored
	s.err = err
}

type OpType int

const (
	OpShardRegister OpType = iota
	OpShardFetch
	OpShardFetchDone
	OpShardIndex
	OpShardIndexDone
	OpShardMakeAvailable
	OpShardDestroy
	OpShardAcquire
	OpShardFail
	OpShardRelease
)

func (o OpType) String() string {
	return [...]string{
		"OpShardRegister",
		"OpShardFetch",
		"OpShardFetchDone",
		"OpShardIndex",
		"OpShardIndexDone",
		"OpShardMakeAvailable",
		"OpShardDestroy",
		"OpShardAcquire",
		"OpShardFail",
		"OpShardRelease"}[o]
}

// Task represents an operation to be performed on a shard or the DAG store.
type Task struct {
	Op    OpType
	Shard *Shard
	Resp  chan ShardResult
	Index carindex.Index
	Error error
}

// ShardResult encapsulates a result from an asynchronous operation.
type ShardResult struct {
	Key      shard.Key
	Error    error
	Accessor *ShardAccessor
}

type Config struct {
	// ScrapRoot is the path to the scratch space, where local copies of
	// remote mounts are saved.
	ScratchSpaceDir string

	// IndexDir is the path where indices are stored.
	IndexDir string

	// Datastore is the datastore where shard state will be persisted.
	Datastore ds.Datastore

	// MountTypes are the recognized mount types, bound to their corresponding
	// URL schemes.
	MountTypes map[string]mount.Type
}

// NewDAGStore constructs a new DAG store with the supplied configuration.
func NewDAGStore(cfg Config) (*DAGStore, error) {
	// validate and manage scratch root directory.
	if cfg.ScratchSpaceDir == "" {
		return nil, fmt.Errorf("missing scratch area root path")
	}
	if err := ensureDir(cfg.ScratchSpaceDir); err != nil {
		return nil, fmt.Errorf("failed to create scratch root dir: %w", err)
	}

	// instantiate the index repo.
	var indices index.FullIndexRepo
	if cfg.IndexDir == "" {
		log.Info("using in-memory index store")
		indices = index.NewMemoryRepo()
	} else {
		err := ensureDir(cfg.IndexDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create index root dir: %w", err)
		}
		indices, err = index.NewFSRepo(cfg.IndexDir)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate full index repo: %w", err)
		}
	}

	// handle the datastore.
	if cfg.Datastore == nil {
		log.Warnf("no datastore provided; falling back to in-mem datastore; shard state will not survive restarts")
		cfg.Datastore = ds.NewMapDatastore()
	}

	// create the registry and register all mount types.
	mounts := mount.NewRegistry()
	for scheme, typ := range cfg.MountTypes {
		if err := mounts.Register(scheme, typ); err != nil {
			return nil, fmt.Errorf("failed to register mount factory: %w", err)
		}
	}

	// TODO: recover persisted shard state from the Datastore.
	// TODO: instantiate scratch space with persisted paths.
	// scratch, err := NewScratchSpace(cfg.ScratchSpaceDir, map[shard.Key]string{}, strconv.Itoa(int(time.Now().Unix())))
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create scratch space: %w", err)
	// }

	ctx, cancel := context.WithCancel(context.Background())
	dagst := &DAGStore{
		mounts:   mounts,
		config:   cfg,
		indices:  indices,
		shards:   make(map[shard.Key]*Shard),
		taskCh:   make(chan *Task, 1024),
		ctx:      ctx,
		cancelFn: cancel,
		// scratch: scratch,
	}

	dagst.wg.Add(1)
	go dagst.control()

	return dagst, nil
}

type RegisterOpts struct {
	// ExistingTransient can be supplied when registering a shard to indicate that
	// there's already an existing local transient local that can be used for
	// indexing.
	ExistingTransient string
}

// RegisterShard initiates the registration of a new shard.
//
// This method performs preliminary validation and returns an error if it fails.
//
// A nil return value implies that the request for registration has
// been accepted, and the caller should monitor the out channel for a result.
func (d *DAGStore) RegisterShard(key shard.Key, mnt mount.Mount, out chan ShardResult, opts RegisterOpts) error {
	d.lk.Lock()
	if _, ok := d.shards[key]; ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardExists)
	}

	upgraded, err := mount.Upgrade(mnt, opts.ExistingTransient)
	if err != nil {
		d.lk.Unlock()
		return err
	}

	// add the shard to the shard catalogue, and drop the lock.
	shrd := &Shard{
		key:       key,
		state:     ShardStateNew,
		mount:     upgraded,
		wRegister: out,
	}
	d.shards[key] = shrd
	d.lk.Unlock()

	tsk := &Task{Op: OpShardRegister, Shard: shrd, Resp: out}
	return d.queueTask(tsk, false)
}

type DestroyOpts struct {
}

func (d *DAGStore) DestroyShard(key shard.Key, out chan ShardResult, _ DestroyOpts) error {
	d.lk.Lock()
	shrd, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return ErrShardUnknown // TODO: encode shard key
	}
	d.lk.Unlock()

	tsk := &Task{Op: OpShardDestroy, Shard: shrd, Resp: out}
	return d.queueTask(tsk, false)
}

type AcquireOpts struct {
}

// AcquireShard acquires access to the specified shard, and returns a
// ShardAccessor, an object that enables various patterns of access to the data
// contained within the shard.
//
// If the shard is not active, it will be reactivated. That could mean
func (d *DAGStore) AcquireShard(key shard.Key, out chan ShardResult, _ AcquireOpts) error {
	d.lk.Lock()
	shrd, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardUnknown)
	}
	d.lk.Unlock()

	tsk := &Task{Op: OpShardAcquire, Shard: shrd, Resp: out}
	return d.queueTask(tsk, false)
}

func (d *DAGStore) control() {
	defer d.wg.Done()

	var tsk *Task
	for {
		select {
		case tsk = <-d.taskCh:
		case <-d.ctx.Done():
			return
		}

		log.Infow("processing task", "op", tsk.Op, "shard", tsk.Shard.key)

		if len(d.overflow) > 0 {
			log.Info("moving an overflow item to task queue")
			select {
			case d.taskCh <- d.overflow[0]:
				d.overflow = d.overflow[1:]
			default:
				log.Info("could not move overflow item into task queue")
			}
		}

		s := tsk.Shard
		s.Lock()

		switch tsk.Op {
		case OpShardRegister:
			if s.state != ShardStateNew {
				err := fmt.Errorf("%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
				_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
				break
			}
			// queue a fetch.
			_ = d.queueTask(&Task{Op: OpShardFetch, Shard: tsk.Shard}, true)

		case OpShardFetch:
			s.state = ShardStateFetching

			go func(ctx context.Context, upgrader *mount.Upgrader) {
				// ensure a copy is available locally.
				reader, err := upgrader.Fetch(ctx)
				if err != nil {
					err = fmt.Errorf("failed to acquire reader of mount: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
					return
				}
				_ = reader.Close()
				_ = d.queueTask(&Task{Op: OpShardFetchDone, Shard: tsk.Shard}, true)
			}(d.ctx, s.mount)

		case OpShardFetchDone:
			s.state = ShardStateFetched
			if !s.indexed {
				// shard isn't indexed yet, so let's index.
				_ = d.queueTask(&Task{Op: OpShardIndex, Shard: tsk.Shard}, true)
				break
			}
			// shard is indexed, we're ready to serve requests.
			_ = d.queueTask(&Task{Op: OpShardMakeAvailable, Shard: tsk.Shard}, true)

		case OpShardIndex:
			s.state = ShardStateIndexing
			go func(ctx context.Context, mnt mount.Mount) {
				reader, err := mnt.Fetch(ctx)
				if err != nil {
					err = fmt.Errorf("failed to acquire reader of mount: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
					return
				}
				defer reader.Close()

				idx, err := loadIndex(reader)
				if err != nil {
					err = fmt.Errorf("failed to index shard: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
					return
				}
				_ = d.queueTask(&Task{Op: OpShardIndexDone, Shard: tsk.Shard, Index: idx}, true)
			}(d.ctx, s.mount)

		case OpShardIndexDone:
			err := d.indices.AddFullIndex(s.key, tsk.Index)
			if err != nil {
				err = fmt.Errorf("failed to add index for shard: %w", err)
				_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
				break
			}
			_ = d.queueTask(&Task{Op: OpShardMakeAvailable, Shard: tsk.Shard}, true)

		case OpShardMakeAvailable:
			if s.wRegister != nil {
				res := ShardResult{Key: s.key}
				go func(ch chan ShardResult) { ch <- res }(s.wRegister)
				s.wRegister = nil
			}

			s.state = ShardStateAvailable

			// trigger queued acquisition waiters.
			for _, acqCh := range s.wAcquire {
				_ = d.queueTask(&Task{Op: OpShardAcquire, Shard: tsk.Shard, Resp: acqCh}, true)
			}
			s.wAcquire = s.wAcquire[:0]

		case OpShardAcquire:
			if s.state != ShardStateAvailable && s.state != ShardStateServing {
				// shard state isn't active yet; make this acquirer wait.
				s.wAcquire = append(s.wAcquire, tsk.Resp)
				break
			}

			s.state = ShardStateServing
			s.refs++
			go func(ctx context.Context, tsk *Task, k shard.Key, mnt mount.Mount) {
				reader, err := mnt.Fetch(ctx)
				if err != nil {
					err = fmt.Errorf("failed to acquire reader of mount: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
					tsk.Resp <- ShardResult{Key: s.key, Error: err}
					return
				}

				idx, err := d.indices.GetFullIndex(k)
				if err != nil {
					err = fmt.Errorf("failed to recover index for shard %s: %w", k, err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, true)
					tsk.Resp <- ShardResult{Key: s.key, Error: err}
					return
				}

				sa, err := NewShardAccessor(s.key, reader, idx)
				tsk.Resp <- ShardResult{Key: s.key, Accessor: sa, Error: err}
			}(d.ctx, tsk, s.key, s.mount)

		case OpShardRelease:
			if (s.state != ShardStateServing && s.state != ShardStateErrored) || s.refs <= 0 {
				log.Warnf("ignored illegal request to release shard")
				break
			}
			s.refs--

		case OpShardFail:
			s.state = ShardStateErrored
			s.err = tsk.Error

			// can't block the event loop, so launch a goroutine to notify.
			if s.wRegister != nil {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to register shard: %w", tsk.Error),
				}
				go func(ch chan ShardResult) { ch <- res }(s.wRegister)
			}

			// fail waiting acquirers.
			// can't block the event loop, so launch a goroutine per acquirer.
			if len(s.wAcquire) > 0 {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to acquire shard: %w", tsk.Error),
				}
				for _, acqCh := range s.wAcquire {
					go func(ch chan ShardResult) { ch <- res }(acqCh)
				}
				s.wAcquire = s.wAcquire[:0] // empty acquirers.
			}

			// TODO trigger retries?

		case OpShardDestroy:
			if s.state == ShardStateServing || s.refs > 0 {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to destroy shard; active references: %d", s.refs),
				}
				go func(ch chan ShardResult) { ch <- res }(tsk.Resp)
				break
			}

			d.lk.Lock()
			delete(d.shards, s.key)
			d.lk.Unlock()
			// TODO are we guaranteed that there are no queued items for this shard?
		}

		s.Unlock()
	}
}

func (d *DAGStore) Close() error {
	d.cancelFn()
	d.wg.Wait()
	return nil
}

func (d *DAGStore) queueTask(tsk *Task, overflow bool) error {
	select {
	case <-d.ctx.Done():
		return fmt.Errorf("dag store closed")
	case d.taskCh <- tsk:
		return nil
	default:
	}
	if overflow {
		d.overflow = append(d.overflow, tsk)
		return nil
	}
	log.Warnf("dag store backlogged")
	return fmt.Errorf("dag store backlogged; cannot accept request")
}

func loadIndex(reader mount.Reader) (carindex.Index, error) {
	carreader, err := car.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read car: %w", err)
	}

	if carreader.Header.HasIndex() {
		ir := carreader.IndexReader()
		idx, err := carindex.ReadFrom(ir)
		if err != nil {
			return nil, fmt.Errorf("failed to read carv index: %w", err)
		}
		return idx, nil
	}
	return nil, fmt.Errorf("processing of unindexed cars unimplemented")

	//
	// // read the CAR version.
	// ver, err := car.ReadVersion(reader)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read car version: %w", err)
	// }
	//
	// switch ver {
	// case 2:
	// 	// this a carv2, we need to look at the index to find out if it
	// 	// carries an inline index or not.
	// 	var header car.Header
	// 	if _, err := header.ReadFrom(reader); err != nil {
	// 		return nil, fmt.Errorf("failed to read carv2 header: %w", err)
	// 	}
	// 	if header.HasIndex() {
	// 		// this CARv2 has an index, let's extract it.
	// 		offset := int64(header.IndexOffset)
	// 		_, err := reader.Seek(offset, 0)
	// 		if err != nil {
	// 			return nil, fmt.Errorf("failed to seek to carv2 index (offset: %d): %w", offset, err)
	// 		}
	// 		idx, err := carindex.ReadFrom(reader)
	// 		if err != nil {
	// 			return nil, fmt.Errorf("failed to read carv2 index: %w", err)
	// 		}
	// 		return idx, nil
	// 	} else {
	// 		return nil, fmt.Errorf("processing of non-indexed carv2 not implemented yet") // TODO implement
	// 	}
	// case 1:
	// 	return nil, fmt.Errorf("processing of carv1 not implemented yet") // TODO implement
	// default:
	// 	return nil, fmt.Errorf("unrecognized car version: %d", ver)
	// }
}

// ensureDir checks whether the specified path is a directory, and if not it
// attempts to create it.
func ensureDir(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		// We need to create the directory.
		return os.MkdirAll(path, os.ModePerm)
	}

	if !fi.IsDir() {
		return fmt.Errorf("path %s exists, and it is not a directory", path)
	}
	return nil
}
