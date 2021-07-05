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
	carindex "github.com/ipld/go-car/v2/index"
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
	// scratch *ScratchSpace

	// externalCh receives external tasks.
	externalCh chan *Task
	// internalCh receives internal tasks to the event loop.
	internalCh chan *Task
	// completionCh receives tasks queued up as a result of async completions.
	completionCh chan *Task

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
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
		mounts:       mounts,
		config:       cfg,
		indices:      indices,
		shards:       make(map[shard.Key]*Shard),
		externalCh:   make(chan *Task, 128), // len=128, concurrent external tasks that can be queued up before putting backpressure.
		internalCh:   make(chan *Task, 1),   // len=1, because eventloop will only ever stage another internal event.
		completionCh: make(chan *Task, 64),  // len=64, hitting this limit will just make async tasks wait.
		ctx:          ctx,
		cancelFn:     cancel,
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
// This method returns an error synchronously if preliminary validation fails.
// Otherwise, it queues the shard for registration. The caller should monitor
// supplied channel for a result.
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
	return d.queueTask(tsk, d.externalCh)
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
func (d *DAGStore) AcquireShard(key shard.Key, out chan ShardResult, _ AcquireOpts) error {
	d.lk.Lock()
	shrd, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return fmt.Errorf("%s: %w", key.String(), ErrShardUnknown)
	}
	d.lk.Unlock()

	tsk := &Task{Op: OpShardAcquire, Shard: shrd, Resp: out}
	return d.queueTask(tsk, d.externalCh)
}

func (d *DAGStore) Close() error {
	d.cancelFn()
	d.wg.Wait()
	return nil
}

func (d *DAGStore) queueTask(tsk *Task, ch chan<- *Task) error {
	select {
	case <-d.ctx.Done():
		return fmt.Errorf("dag store closed")
	case ch <- tsk:
		return nil
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
