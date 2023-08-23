package dagstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	mh "github.com/multiformats/go-multihash"

	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/dagstore/throttle"
)

var (
	// StoreNamespace is the namespace under which shard state will be persisted.
	StoreNamespace = ds.NewKey("dagstore")
)

// RecoverOnStartPolicy specifies the recovery policy for failed
// shards on DAGStore start.
type RecoverOnStartPolicy int

const (
	// DoNotRecover will not recover any failed shards on start. Recovery
	// must be performed manually.
	DoNotRecover RecoverOnStartPolicy = iota

	// RecoverOnAcquire will automatically queue a recovery for a failed shard
	// on the first acquire attempt, and will park that acquire while recovery
	// is in progress.
	RecoverOnAcquire

	// RecoverNow will eagerly trigger a recovery for all failed shards
	// upon start.
	RecoverNow
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
	indexer ShardIndexer

	// TopLevelIndex is the top level (cid -> []shards) index that maps a cid to all the shards that is present in.
	TopLevelIndex index.Inverted

	// Channels owned by us.
	//
	// externalCh receives external tasks.
	externalCh chan *task
	// internalCh receives internal tasks to the event loop.
	internalCh chan *task
	// completionCh receives tasks queued up as a result of async completions.
	completionCh chan *task
	// dispatchResultsCh is a buffered channel for dispatching results back to
	// the application. Serviced by a dispatcher goroutine.
	// Note: This pattern decouples the event loop from the application, so a
	// failure to consume immediately won't block the event loop.
	dispatchResultsCh chan *dispatch
	// dispatchFailuresCh is a buffered channel for dispatching shard failures
	// back to the application. Serviced by a dispatcher goroutine.
	// See note in dispatchResultsCh for background.
	dispatchFailuresCh chan *dispatch
	// gcCh is where requests for GC are sent.
	gcCh chan chan *GCResult

	// Channels not owned by us.
	//
	// traceCh is where traces on shard operations will be sent, if non-nil.
	traceCh chan<- Trace
	// failureCh is where shard failures will be notified, if non-nil.
	failureCh chan<- ShardResult

	// Throttling.
	//
	throttleReaadyFetch throttle.Throttler
	throttleIndex       throttle.Throttler

	// Lifecycle.
	//
	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

var _ Interface = (*DAGStore)(nil)

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

type ShardIndexer func(context.Context, shard.Key, mount.Reader) (carindex.Index, error)

type Config struct {
	// TransientsDir is the path to directory where local transient files will
	// be created for remote mounts.
	TransientsDir string

	// IndexRepo is the full index repo to use.
	IndexRepo index.FullIndexRepo

	TopLevelIndex index.Inverted

	// Datastore is the datastore where shard state will be persisted.
	Datastore ds.Datastore

	// MountRegistry contains the set of recognized mount types.
	MountRegistry *mount.Registry

	// TraceCh is a channel where the caller desires to be notified of every
	// shard operation. Publishing to this channel blocks the event loop, so the
	// caller must ensure the channel is serviced appropriately.
	//
	// Note: Not actively consuming from this channel will make the event
	// loop block.
	TraceCh chan<- Trace

	// FailureCh is a channel to be notified every time that a shard moves to
	// ShardStateErrored. A nil value will send no failure notifications.
	// Failure events can be used to evaluate the error and call
	// DAGStore.RecoverShard if deemed recoverable.
	//
	// Note: Not actively consuming from this channel will make the event
	// loop block.
	FailureCh chan<- ShardResult

	// MaxConcurrentIndex is the maximum indexing jobs that can
	// run concurrently. 0 (default) disables throttling.
	MaxConcurrentIndex int

	// MaxConcurrentReadyFetches is the maximum number of fetches that will
	// run concurrently for mounts that are reporting themselves as ready for
	// immediate fetch. 0 (default) disables throttling.
	MaxConcurrentReadyFetches int

	// RecoverOnStart specifies whether failed shards should be recovered
	// on start.
	RecoverOnStart RecoverOnStartPolicy

	// ShardIndexer sets a custom callback for determining the index
	// mapping of CID->Offset that should be registered for a shard.
	ShardIndexer ShardIndexer
}

// NewDAGStore constructs a new DAG store with the supplied configuration.
//
// You must call Start for processing to begin.
func NewDAGStore(cfg Config) (*DAGStore, error) {
	// validate and manage scratch root directory.
	if cfg.TransientsDir == "" {
		return nil, fmt.Errorf("missing scratch area root path")
	}
	if err := ensureDir(cfg.TransientsDir); err != nil {
		return nil, fmt.Errorf("failed to create scratch root dir: %w", err)
	}

	// instantiate the index repo.
	if cfg.IndexRepo == nil {
		log.Info("using in-memory index store")
		cfg.IndexRepo = index.NewMemoryRepo()
	}

	if cfg.TopLevelIndex == nil {
		log.Info("using in-memory inverted index")
		cfg.TopLevelIndex = index.NewInverted(dssync.MutexWrap(ds.NewMapDatastore()))
	}
	// default indexer
	if cfg.ShardIndexer == nil {
		cfg.ShardIndexer = func(_ context.Context, _ shard.Key, r mount.Reader) (carindex.Index, error) {
			return car.ReadOrGenerateIndex(r, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
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
		mounts:              cfg.MountRegistry,
		config:              cfg,
		indices:             cfg.IndexRepo,
		TopLevelIndex:       cfg.TopLevelIndex,
		shards:              make(map[shard.Key]*Shard),
		store:               cfg.Datastore,
		indexer:             cfg.ShardIndexer,
		externalCh:          make(chan *task, 128),     // len=128, concurrent external tasks that can be queued up before exercising backpressure.
		internalCh:          make(chan *task, 1),       // len=1, because eventloop will only ever stage another internal event.
		completionCh:        make(chan *task, 64),      // len=64, hitting this limit will just make async tasks wait.
		dispatchResultsCh:   make(chan *dispatch, 128), // len=128, same as externalCh.
		gcCh:                make(chan chan *GCResult, 8),
		traceCh:             cfg.TraceCh,
		failureCh:           cfg.FailureCh,
		throttleIndex:       throttle.Noop(),
		throttleReaadyFetch: throttle.Noop(),
		ctx:                 ctx,
		cancelFn:            cancel,
	}

	if max := cfg.MaxConcurrentIndex; max > 0 {
		dagst.throttleIndex = throttle.Fixed(max)
	}

	if max := cfg.MaxConcurrentReadyFetches; max > 0 {
		dagst.throttleReaadyFetch = throttle.Fixed(max)
	}

	return dagst, nil
}

// Start starts a DAG store.
func (d *DAGStore) Start(ctx context.Context) error {
	d.lk.Lock()
	if err := d.restoreState(); err != nil {
		// TODO add a lenient mode.
		return fmt.Errorf("failed to restore dagstore state: %w", err)
	}

	if err := d.clearOrphaned(); err != nil {
		log.Warnf("failed to clear orphaned files on startup: %s", err)
	}

	// Reset in-progress states.
	//
	// Queue shards whose registration needs to be restarted. Release those
	// ops after we spawn the control goroutine. Otherwise, having more shards
	// in this state than the externalCh buffer size would exceed the channel
	// buffer, and we'd block forever.
	var toRegister, toRecover []*Shard
	for _, s := range d.shards {
		switch s.state {
		case ShardStateErrored:
			switch d.config.RecoverOnStart {
			case DoNotRecover:
				log.Infow("start: skipping recovery of shard in errored state", "shard", s.key, "error", s.err)
			case RecoverOnAcquire:
				log.Infow("start: failed shard will recover on next acquire", "shard", s.key, "error", s.err)
				s.recoverOnNextAcquire = true
			case RecoverNow:
				log.Infow("start: recovering failed shard immediately", "shard", s.key, "error", s.err)
				toRecover = append(toRecover, s)
			}

		case ShardStateServing:
			// reset to available, as we have no active acquirers at start.
			s.state = ShardStateAvailable
		case ShardStateAvailable:
			// Noop: An available shard whose index has disappeared across restarts
			// will fail on the first acquisition.
		case ShardStateInitializing:
			// handle shards that were initializing when we shut down.
			// if we already have the index for the shard, there's nothing else to do.
			if istat, err := d.indices.StatFullIndex(s.key); err == nil && istat.Exists {
				s.state = ShardStateAvailable
			} else {
				// reset back to new, and queue the OpShardRegister.
				s.state = ShardStateNew
				toRegister = append(toRegister, s)
			}
		}
	}
	d.lk.Unlock()

	// spawn the control goroutine.
	d.wg.Add(1)
	go d.control()

	// spawn the dispatcher goroutine for responses, responsible for pumping
	// async results back to the caller.
	d.wg.Add(1)
	go d.dispatcher(d.dispatchResultsCh)

	// application has provided a failure channel; spawn the dispatcher.
	if d.failureCh != nil {
		d.dispatchFailuresCh = make(chan *dispatch, 128) // len=128, same as externalCh.
		d.wg.Add(1)
		go d.dispatcher(d.dispatchFailuresCh)
	}

	// release the queued registrations before we return.
	for _, s := range toRegister {
		_ = d.queueTask(&task{op: OpShardRegister, shard: s, waiter: &waiter{ctx: ctx}}, d.externalCh)
	}

	// queue shard recovery for shards in the errored state before we return.
	for _, s := range toRecover {
		_ = d.queueTask(&task{op: OpShardRecover, shard: s, waiter: &waiter{ctx: ctx}}, d.externalCh)
	}

	return nil
}

func (d *DAGStore) GetIterableIndex(key shard.Key) (carindex.IterableIndex, error) {
	fi, err := d.indices.GetFullIndex(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get iterable index: %w", err)
	}

	ii, ok := fi.(carindex.IterableIndex)
	if !ok {
		return nil, errors.New("index for shard is not iterable")
	}

	return ii, nil
}

func (d *DAGStore) ShardsContainingMultihash(ctx context.Context, h mh.Multihash) ([]shard.Key, error) {
	return d.TopLevelIndex.GetShardsForMultihash(ctx, h)
}

type RegisterOpts struct {
	// ExistingTransient can be supplied when registering a shard to indicate
	// that there's already an existing local transient copy that can be used
	// for indexing.
	ExistingTransient string

	// LazyInitialization defers shard indexing to the first access instead of
	// performing it at registration time. Use this option when fetching the
	// asset is expensive.
	//
	// When true, the registration channel will fire as soon as the DAG store
	// has acknowledged the inclusion of the shard, without waiting for any
	// indexing to happen.
	LazyInitialization bool
}

// RegisterShard initiates the registration of a new shard.
//
// This method returns an error synchronously if preliminary validation fails.
// Otherwise, it queues the shard for registration. The caller should monitor
// supplied channel for a result.
func (d *DAGStore) RegisterShard(
	ctx context.Context, key shard.Key, mnt mount.Mount, out chan ShardResult, opts RegisterOpts,
) error {
	d.lk.Lock()
	if _, ok := d.shards[key]; ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardExists)
	}

	// wrap the original mount in an upgrader.
	upgraded, err := mount.Upgrade(mnt, d.throttleReaadyFetch, d.config.TransientsDir, key.String(), opts.ExistingTransient)
	if err != nil {
		d.lk.Unlock()
		return err
	}

	w := &waiter{outCh: out, ctx: ctx}

	// add the shard to the shard catalogue, and drop the lock.
	s := &Shard{
		d:     d,
		key:   key,
		state: ShardStateNew,
		mount: upgraded,
		lazy:  opts.LazyInitialization,
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
// This operation may resolve near-instantaneously if the shard is available
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

type RecoverOpts struct {
}

// RecoverShard recovers a shard in ShardStateErrored state.
//
// If the shard referenced by the key doesn't exist, an error is returned
// immediately and no result is delivered on the supplied channel.
//
// If the shard is not in the ShardStateErrored state, the operation is accepted
// but an error will be returned quickly on the supplied channel.
//
// Otherwise, the recovery operation will be queued and the supplied channel
// will be notified when it completes.
//
// TODO add an operation identifier to ShardResult -- starts to look like
//
//	a Trace event?
func (d *DAGStore) RecoverShard(ctx context.Context, key shard.Key, out chan ShardResult, _ RecoverOpts) error {
	d.lk.Lock()
	s, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardUnknown)
	}
	d.lk.Unlock()

	tsk := &task{op: OpShardRecover, shard: s, waiter: &waiter{ctx: ctx, outCh: out}}
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

func (d *DAGStore) Stats() map[ShardState]int {
	d.lk.RLock()
	defer d.lk.RUnlock()

	stats := make(map[ShardState]int)
	for _, s := range d.shards {
		stats[s.state]++
	}
	return stats
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

// GC performs DAG store garbage collection by reclaiming transient files of
// shards that are currently available but inactive, or errored.
//
// GC runs with exclusivity from the event loop.
func (d *DAGStore) GC(ctx context.Context) (*GCResult, error) {
	ch := make(chan *GCResult)
	select {
	case d.gcCh <- ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *DAGStore) Close() error {
	d.cancelFn()
	d.wg.Wait()
	_ = d.store.Sync(context.TODO(), ds.Key{})
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
	results, err := d.store.Query(d.ctx, query.Query{})
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

		log.Debugw("restored shard state on dagstore startup", "shard", s.key, "shard state", s.state, "shard error", s.err,
			"shard lazy", s.lazy)
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
	err := fmt.Errorf(format, args...)
	return d.queueTask(&task{op: OpShardFail, shard: s, err: err}, ch)
}
