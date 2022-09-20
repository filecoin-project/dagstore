package dagstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/dagstore/gc"

	mh "github.com/multiformats/go-multihash"

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

// FetchOnStartPolicy specifies the policy that determines if a registered shard
// whose index is absent should be fetched on start.
type FetchOnStartPolicy int

const (
	FetchNow FetchOnStartPolicy = iota
	FetchOnAcquire
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

	// ErrShardIllegalReservationRequest is returned when we get a reservation request for a shard
	// that is not in a valid state for reservation requests.
	ErrShardIllegalReservationRequest = errors.New("illegal shard reservation request")

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
	// automatedgcTraceCh is where the Automated GC trace will be sent, if channel is non-nil.
	automatedgcTraceCh chan AutomatedGCResult

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

	// Automated GC state
	//---
	// counter tracking the size of the transient directory along with reservations; guarded by the event loop
	totalTransientDirSize int64
	//
	// The garbage collector strategy we are using; calls to this should only be made from the event loop
	gcs gc.GarbageCollectionStrategy
	//
	// immutable, can be read anywhere without a lock.
	defaultReservationSize    int64
	automatedGCEnabled        bool
	maxTransientDirSize       int64
	transientsGCWatermarkHigh float64
	transientsGCWatermarkLow  float64
}

var _ Interface = (*DAGStore)(nil)

type dispatch struct {
	w   *waiter
	res *ShardResult
}

type reservationReq struct {
	nPrevReservations int64
	want              int64
	response          chan *reservationResp
}

type reservationResp struct {
	reserved int64
	err      error
}

type releaseReq struct {
	release int64
}

// Task represents an operation to be performed on a shard or the DAG store.
type task struct {
	reservationReq *reservationReq
	releaseReq     *releaseReq
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

type AutomatedGCConfig struct {
	// DefaultReservationSize configures the default amount of space to reserve for a transient
	// when downloading it if the size of the transient is not known upfront.
	DefaultReservationSize int64

	// GarbeCollectionStrategy specifies the garbage collection strategy we will use
	// for the automated watermark based GC feature. See the documentation of `gc.GarbageCollectionStrategy` for more details.
	GarbeCollectionStrategy gc.GarbageCollectionStrategy

	// MaxTransientDirSize specifies the maximum allowable size of the transients directory.
	MaxTransientDirSize int64

	// TransientsGCWatermarkHigh is the proportion of the `MaxTransientDirSize` at which we we proactively starts GCing
	// the transients directory till the ratio of (transient directory size / `MaxTransientDirSize`) is equal to or less
	// than the `TransientsGCWatermarkLow` config param below.
	TransientsGCWatermarkHigh float64

	// TransientsGCWatermarkLow: See documentation of `TransientsGCWatermarkHigh` above.
	TransientsGCWatermarkLow float64

	// AutomatedGCTraceCh is a channel where the caller desires to be notified of every
	// Automated GC reclaim operation. Publishing to this channel blocks the event loop, so the
	// caller must ensure the channel is serviced appropriately.
	//
	// Note: Not actively consuming from this channel will make the event
	// loop block.
	AutomatedGCTraceCh chan AutomatedGCResult
}

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

	// AutomatedGCEnabled enables Automated GC according to the given GC policy.
	AutomatedGCEnabled bool

	// AutomatedGCConfig specifies the confguration parameters to use for the Automated GC.
	AutomatedGCConfig *AutomatedGCConfig

	FetchOnStart FetchOnStartPolicy
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

	if cfg.AutomatedGCEnabled {
		if err := validateAutomatedGCConfig(cfg.AutomatedGCConfig); err != nil {
			return nil, err
		}
	}

	if cfg.TopLevelIndex == nil {
		log.Info("using in-memory inverted index")
		cfg.TopLevelIndex = index.NewInverted(dssync.MutexWrap(ds.NewMapDatastore()))
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
		automatedGCEnabled:  cfg.AutomatedGCEnabled,
		gcs:                 &gc.NoOpStrategy{},
	}

	if max := cfg.MaxConcurrentIndex; max > 0 {
		dagst.throttleIndex = throttle.Fixed(max)
	}

	if max := cfg.MaxConcurrentReadyFetches; max > 0 {
		dagst.throttleReaadyFetch = throttle.Fixed(max)
	}

	dagst.defaultReservationSize = int64(defaultReservation)
	if dagst.automatedGCEnabled {
		if cfg.AutomatedGCConfig.DefaultReservationSize != 0 {
			dagst.defaultReservationSize = cfg.AutomatedGCConfig.DefaultReservationSize
		}

		dagst.gcs = cfg.AutomatedGCConfig.GarbeCollectionStrategy
		dagst.maxTransientDirSize = cfg.AutomatedGCConfig.MaxTransientDirSize
		dagst.transientsGCWatermarkHigh = cfg.AutomatedGCConfig.TransientsGCWatermarkHigh
		dagst.transientsGCWatermarkLow = cfg.AutomatedGCConfig.TransientsGCWatermarkLow
		dagst.automatedgcTraceCh = cfg.AutomatedGCConfig.AutomatedGCTraceCh

		// default reservation size should not exceed the maximum transient directory su
		if dagst.maxTransientDirSize <= dagst.defaultReservationSize {
			dagst.defaultReservationSize = dagst.maxTransientDirSize / 10
		}
	}

	var err error
	dagst.totalTransientDirSize, err = dagst.transientDirSize()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate transient dir size: %w", err)
	}

	return dagst, nil
}

func validateAutomatedGCConfig(cfg *AutomatedGCConfig) error {
	if cfg == nil {
		return errors.New("automated GC config cannot be empty since automated GC has been enabled")
	}

	if cfg.GarbeCollectionStrategy == nil {
		return errors.New("garbage collection strategy should not be nil when automated GC is enabled")
	}

	if cfg.TransientsGCWatermarkLow == 0 || cfg.TransientsGCWatermarkHigh == 0 {
		return errors.New("high or low watermark cannot be zero")
	}

	if cfg.TransientsGCWatermarkHigh <= cfg.TransientsGCWatermarkLow {
		return errors.New("high water mark should be greater than low watermark")
	}

	if cfg.MaxTransientDirSize == 0 {
		return errors.New("max transient directory size should not be zero")
	}

	return nil
}

// Start starts a DAG store.
func (d *DAGStore) Start(ctx context.Context) error {
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
				if d.config.FetchOnStart == FetchOnAcquire {
					s.fetchOnNextAcquire = true
				}

				// reset back to new, and queue the OpShardRegister.
				s.state = ShardStateNew
				toRegister = append(toRegister, s)
			}
		}
		// all shards are reclaimable in the beginning
		d.gcs.NotifyReclaimable(s.key)
	}

	// do an automated GC if needed
	d.automatedGCIfNeeded()

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

	queueTask := func(tsk *task, ch chan<- *task) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.ctx.Done():
			return d.ctx.Err()
		case ch <- tsk:
			return nil
		}
	}

	// release the queued registrations before we return.
	for _, s := range toRegister {
		if err := queueTask(&task{op: OpShardRegister, shard: s, waiter: &waiter{ctx: ctx}}, d.externalCh); err != nil {
			return fmt.Errorf("failed to queue task for shard %s: %w", s, err)
		}
	}

	// queue shard recovery for shards in the errored state before we return.
	for _, s := range toRecover {
		if err := queueTask(&task{op: OpShardRecover, shard: s, waiter: &waiter{ctx: ctx}}, d.externalCh); err != nil {
			return fmt.Errorf("failed to queue task for shard %s: %w", s, err)
		}
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

	// ReservationOpts are used to configure the resevation mechanism when downloading
	// transients whose size is not known upfront when automated GC is enabled.
	ReservationOpts []mount.ReservationGatedDownloaderOpt
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
	downloader := d.downloader(key, 0, opts.ReservationOpts...)
	upgraded, err := mount.Upgrade(mnt, d.throttleReaadyFetch, d.config.TransientsDir, key.String(), opts.ExistingTransient, downloader)
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
	// NoDownload can be supplied when acquiring a shard to indicate that the shard should only be acquired if the transient
	// already exists and does not need to be downloaded from a mount that does not support random access.
	NoDownload bool
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
func (d *DAGStore) AcquireShard(ctx context.Context, key shard.Key, out chan ShardResult, opts AcquireOpts) error {
	d.lk.Lock()
	s, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardUnknown)
	}
	d.lk.Unlock()

	tsk := &task{op: OpShardAcquire, shard: s, waiter: &waiter{ctx: ctx, outCh: out, noDownload: opts.NoDownload}}
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
//  a Trace event?
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
	Key                     shard.Key
	Op                      OpType
	After                   ShardInfo
	TransientDirSizeCounter int64
}

type ShardInfo struct {
	ShardState
	Error         error
	refs          uint32
	TransientSize int64
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
	info := ShardInfo{ShardState: s.state, Error: s.err, refs: s.refs, TransientSize: s.transientSize}
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
		info := ShardInfo{ShardState: s.state, Error: s.err, refs: s.refs, TransientSize: s.transientSize}
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

func (d *DAGStore) downloader(key shard.Key, knownTransientSize int64, opts ...mount.ReservationGatedDownloaderOpt) mount.TransientDownloader {
	var downloader mount.TransientDownloader = &mount.SimpleDownloader{}
	if d.automatedGCEnabled {
		downloader = mount.NewReservationGatedDownloader(key, knownTransientSize, &transientAllocator{d}, opts...)
	}
	return downloader
}
