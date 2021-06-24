package dagstore

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

// TODO insert index into the index repo.
// TODO implement DAG accessor
// TODO integrate Cabs blockstore
// TODO finish implementing AcquireShard
// TODO implement ReleaseShard
// TODO ability to park AcquireShard while shard is initializing
// TODO do we need shard transactions (ability to perform many atomic actions inside a TX)?
// TODO do we need a control goroutine to manage the scrap area, and more?

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
	config  *Config
	indices index.FullIndexRepo
}

type Config struct {
	// ScrapRoot is the path to the scrap area, where local local copies of
	// remote mounts are saved.
	ScrapRootDir string

	// IndexRootDir is the path where indices are stored.
	IndexRootDir string

	// Datastore is the datastore where shard state will be persisted.
	Datastore ds.Datastore

	// Mounts are the recognized mount types, bound to their corresponding
	// URL schemes.
	Mounts map[string]mount.Type
}

func NewDAGStore(cfg *Config) (*DAGStore, error) {
	// validate and manage scrap root directory.
	if cfg.ScrapRootDir == "" {
		return nil, fmt.Errorf("missing scrap area root path")
	}
	if err := ensureDir(cfg.ScrapRootDir); err != nil {
		return nil, fmt.Errorf("failed to create scrap root dir: %w", err)
	}

	// validate and manage index root directory.
	if cfg.IndexRootDir == "" {
		return nil, fmt.Errorf("missing index repo root path")
	}
	if err := ensureDir(cfg.IndexRootDir); err != nil {
		return nil, fmt.Errorf("failed to create index root dir: %w", err)
	}

	// handle the datastore.
	if cfg.Datastore == nil {
		log.Warnf("no datastore provided; falling back to in-mem datastore; shard state will not survive restarts")
		cfg.Datastore = ds.NewMapDatastore()
	}

	// create the registry and register all mount types.
	mounts := mount.NewRegistry()
	for scheme, typ := range cfg.Mounts {
		if err := mounts.Register(scheme, typ); err != nil {
			return nil, fmt.Errorf("failed to register mount factory: %w", err)
		}
	}

	// instantiate the index repo.
	indices, err := index.NewFSRepo(cfg.IndexRootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate full index repo: %w", err)
	}

	// TODO: recover persisted shard state from the Datastore.

	dagst := &DAGStore{
		mounts:  mounts,
		config:  cfg,
		indices: indices,
	}

	return dagst, nil
}

type RegisterOpt func(opts *registerOpts) error

type registerOpts struct {
	existingTransient string
}

// ExistingTransient can be supplied when registering a shard to indicate that
// there's already an existing local transient copy that can be used for
// indexing.
func ExistingTransient(path string) RegisterOpt {
	return func(opts *registerOpts) error {
		opts.existingTransient = path
		return nil
	}
}

func (d *DAGStore) RegisterShard(key shard.Key, mount mount.Mount, options ...RegisterOpt) error {
	var opts registerOpts
	for _, o := range options {
		if err := o(&opts); err != nil {
			return err
		}
	}

	d.lk.Lock()
	if _, ok := d.shards[key]; ok {
		d.lk.Unlock()
		return ErrShardExists // TODO: encode shard key in error
	}
	// TODO populate the existing transient copy from options.
	shrd := &Shard{
		state: ShardStateNew,
		mount: mount,
	}
	d.shards[key] = shrd
	d.lk.Unlock() // drop the shard catalogue lock.

	idx, err := shrd.initializeShard()
	if err != nil {
		return fmt.Errorf("failed to initialize shard: %w", err)
	}

	if err := ; err != nil {
		return err
	}

	// perform indexing.
	shrd.state = ShardStateIndexing
	idx, err := shrd.initializeIndex()
	if err != nil {
		return err
	}

	return nil
}

func (d *DAGStore) DestroyShard(key shard.Key) error {
	d.lk.Lock()
	defer d.lk.Unlock()

	shrd, ok := d.shards[key]
	if !ok {
		return ErrShardUnknown // TODO: encode shard key
	}

	return shrd.CleanTransient()
}

// AcquireShard acquires access to the specified shard, and returns a
// ShardAccessor, an object that enables various patterns of access to the data
// contained within the shard.
//
// If the shard is not active, it will be reactivated. That could mean
func (d *DAGStore) AcquireShard(key shard.Key) (ShardAccessor, error) {
	d.lk.Lock()
	shrd, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return nil, ErrShardUnknown // TODO: encode shard key
	}
	d.lk.Unlock() // drop the dagstore lock.

	shrd.lk.Lock()
	defer shrd.lk.Unlock()

	// TODO implement refcounting.
	if err := shrd.ensureLocal(); err != nil {
		//
	}
	return nil, nil
}

func (d *DAGStore) ReleaseShard(key shard.Key) error {
	d.lk.Lock()
	shrd, ok := d.shards[key]
	if !ok {
		d.lk.Unlock()
		return ErrShardUnknown // TODO: encode shard key
	}

	shrd.lk.Lock()
	defer shrd.lk.Unlock()

	d.lk.Unlock()
	// shrd.incr()
	// TODO
	return nil

}

// control is a control goroutine.
func (d *DAGStore) control() {
	// TODO
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

// TODO shardTx may be a method to start a shard transaction.
func shardTx() {

}
