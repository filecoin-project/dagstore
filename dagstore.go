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
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
)

var (
	// StoreNamespace is the namespace under which shard state will be persisted.
	StoreNamespace = ds.NewKey("dagstore")
)

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
	store   ds.Datastore

	// externalCh receives external tasks.
	externalCh chan *task
	// internalCh receives internal tasks to the event loop.
	internalCh chan *task
	// completionCh receives tasks queued up as a result of async completions.
	completionCh chan *task
	// dispatchCh is serviced by the dispatcher goroutine.
	dispatchCh chan *dispatch

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
	traceCh  chan<- Trace
}

type dispatch struct {
	w   *waiter
	res *ShardResult
}

// Task represents an operation to be performed on a shard or the DAG store.
type task struct {
	*waiter
	op    OpType
	shard *Shard
	err   error
}

// ShardResult encapsulates a result from an asynchronous operation.
type ShardResult struct {
	Key      shard.Key
	Error    error
	Accessor *ShardAccessor
}

type Config struct {
	// TransientsDir is the path to directory where local transient files will
	// be created for remote mounts.
	TransientsDir string

	// IndexDir is the path where indices are stored.
	IndexDir string

	// Datastore is the datastore where shard state will be persisted.
	Datastore ds.Datastore

	// MountRegistry contains the set of recognized mount types.
	MountRegistry *mount.Registry

	// TraceCh is a channel where the caller desires to be notified of every
	// shard operation. Publishing to this channel blocks the event loop, so the
	// caller must ensure the channel is serviced appropriately.
	TraceCh chan<- Trace
}

// NewDAGStore constructs a new DAG store with the supplied configuration.
func NewDAGStore(cfg Config) (*DAGStore, error) {
	// validate and manage scratch root directory.
	if cfg.TransientsDir == "" {
		return nil, fmt.Errorf("missing scratch area root path")
	}
	if err := ensureDir(cfg.TransientsDir); err != nil {
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
		cfg.Datastore = dssync.MutexWrap(ds.NewMapDatastore()) // TODO can probably remove mutex wrap, since access is single-threaded
	}

	// namespace all store operations.
	cfg.Datastore = namespace.Wrap(cfg.Datastore, StoreNamespace)

	if cfg.MountRegistry == nil {
		cfg.MountRegistry = mount.NewRegistry()
	}

	ctx, cancel := context.WithCancel(context.Background())
	dagst := &DAGStore{
		mounts:       cfg.MountRegistry,
		config:       cfg,
		indices:      indices,
		shards:       make(map[shard.Key]*Shard),
		store:        cfg.Datastore,
		externalCh:   make(chan *task, 128),     // len=128, concurrent external tasks that can be queued up before exercising backpressure.
		internalCh:   make(chan *task, 1),       // len=1, because eventloop will only ever stage another internal event.
		completionCh: make(chan *task, 64),      // len=64, hitting this limit will just make async tasks wait.
		dispatchCh:   make(chan *dispatch, 128), // len=128, same as externalCh (input channel).
		ctx:          ctx,
		cancelFn:     cancel,
		traceCh:      cfg.TraceCh,
	}

	if err := dagst.restoreState(); err != nil {
		// TODO add a lenient mode.
		return nil, fmt.Errorf("failed to restore dagstore state: %w", err)
	}

	// reset in-progress states.
	for _, s := range dagst.shards {
		// reset to available, as we have no active acquirers at start.
		if s.state == ShardStateServing {
			s.state = ShardStateAvailable
		}

		// Note: An available shard whose index has disappeared across restarts
		// will fail on the first acquisition.

		// handle shards that were initializing when we shut down.
		if s.state == ShardStateInitializing {
			// if we already have the index for the shard, there's nothing else to do.
			if istat, err := dagst.indices.StatFullIndex(s.key); err == nil && istat.Exists {
				s.state = ShardStateAvailable
			} else {
				// restart the registration.
				s.state = ShardStateNew
				_ = dagst.queueTask(&task{op: OpShardRegister, shard: s, waiter: &waiter{ctx: ctx}}, dagst.externalCh)
			}
		}
	}

	// spawn the control goroutine.
	dagst.wg.Add(1)
	go dagst.control()

	// spawn the dispatcher goroutine, responsible for pumping async results
	// back to the caller.
	dagst.wg.Add(1)
	go dagst.dispatcher()

	return dagst, nil
}

type RegisterOpts struct {
	// ExistingTransient can be supplied when registering a shard to indicate that
	// there's already an existing local transient copy that can be used for
	// indexing.
	ExistingTransient string
}

// RegisterShard initiates the registration of a new shard.
//
// This method returns an error synchronously if preliminary validation fails.
// Otherwise, it queues the shard for registration. The caller should monitor
// supplied channel for a result.
func (d *DAGStore) RegisterShard(ctx context.Context, key shard.Key, mnt mount.Mount, out chan ShardResult, opts RegisterOpts) error {
	d.lk.Lock()
	if _, ok := d.shards[key]; ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardExists)
	}

	// wrap the original mount in an upgrader.
	upgraded, err := mount.Upgrade(mnt, d.config.TransientsDir, key.String(), opts.ExistingTransient)
	if err != nil {
		d.lk.Unlock()
		return err
	}

	w := &waiter{outCh: out, ctx: ctx}

	// add the shard to the shard catalogue, and drop the lock.
	s := &Shard{
		d:         d,
		key:       key,
		state:     ShardStateNew,
		mount:     upgraded,
		wRegister: w,
	}
	d.shards[key] = s
	d.lk.Unlock()

	tsk := &task{op: OpShardRegister, shard: s, waiter: w}
	return d.queueTask(tsk, d.externalCh)
}

type DestroyOpts struct {
}

func (d *DAGStore) DestroyShard(ctx context.Context, key shard.Key, out chan ShardResult, _ DestroyOpts) error {
	d.lk.Lock()
	s, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return ErrShardUnknown // TODO: encode shard key
	}
	d.lk.Unlock()

	tsk := &task{op: OpShardDestroy, shard: s, waiter: &waiter{ctx: ctx, outCh: out}}
	return d.queueTask(tsk, d.externalCh)
}

type AcquireOpts struct {
}

// AcquireShard acquires access to the specified shard, and returns a
// ShardAccessor, an object that enables various patterns of access to the data
// contained within the shard.
//
// This operation may resolve near-instantaneosly if the shard is available
// locally. If not, the shard data may be fetched from its mount.
//
// This method returns an error synchronously if preliminary validation fails.
// Otherwise, it queues the shard for acquisition. The caller should monitor
// supplied channel for a result.
func (d *DAGStore) AcquireShard(ctx context.Context, key shard.Key, out chan ShardResult, _ AcquireOpts) error {
	d.lk.Lock()
	s, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardUnknown)
	}
	d.lk.Unlock()

	tsk := &task{op: OpShardAcquire, shard: s, waiter: &waiter{ctx: ctx, outCh: out}}
	return d.queueTask(tsk, d.externalCh)
}

type Trace struct {
	Key   shard.Key
	Op    OpType
	After ShardInfo
}

type ShardInfo struct {
	ShardState
	Error error
	refs  uint32
}

// GetShardInfo returns the current state of shard with key k.
//
// If the shard is not known, ErrShardUnknown is returned.
func (d *DAGStore) GetShardInfo(k shard.Key) (ShardInfo, error) {
	d.lk.RLock()
	defer d.lk.RUnlock()
	s, ok := d.shards[k]
	if !ok {
		return ShardInfo{}, ErrShardUnknown
	}

	s.lk.RLock()
	info := ShardInfo{ShardState: s.state, Error: s.err, refs: s.refs}
	s.lk.RUnlock()
	return info, nil
}

type AllShardsInfo map[shard.Key]ShardInfo

// AllShardsInfo returns the current state of all registered shards, as well as
// any errors.
func (d *DAGStore) AllShardsInfo() AllShardsInfo {
	d.lk.RLock()
	defer d.lk.RUnlock()

	ret := make(AllShardsInfo, len(d.shards))
	for k, s := range d.shards {
		s.lk.RLock()
		info := ShardInfo{ShardState: s.state, Error: s.err, refs: s.refs}
		s.lk.RUnlock()
		ret[k] = info
	}
	return ret
}

func (d *DAGStore) Close() error {
	d.cancelFn()
	d.wg.Wait()
	_ = d.store.Sync(ds.Key{})
	return nil
}

func (d *DAGStore) queueTask(tsk *task, ch chan<- *task) error {
	select {
	case <-d.ctx.Done():
		return fmt.Errorf("dag store closed")
	case ch <- tsk:
		return nil
	}
}

func (d *DAGStore) restoreState() error {
	results, err := d.store.Query(query.Query{})
	if err != nil {
		return fmt.Errorf("failed to recover dagstore state from store: %w", err)
	}
	for {
		res, ok := results.NextSync()
		if !ok {
			return nil
		}
		s := &Shard{d: d}
		if err := s.UnmarshalJSON(res.Value); err != nil {
			log.Warnf("failed to recover state of shard %s: %s; skipping", shard.KeyFromString(res.Key), err)
			continue
		}
		d.shards[s.key] = s
	}
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

// failShard queues a shard failure (does not fail it immediately). It is
// suitable for usage both outside and inside the event loop, depending on the
// channel passed.
func (d *DAGStore) failShard(s *Shard, ch chan *task, format string, args ...interface{}) error {
	// TODO failShard will fire the failure notification to the application soon.
	err := fmt.Errorf(format, args...)
	return d.queueTask(&task{op: OpShardFail, shard: s, err: err}, ch)
}
