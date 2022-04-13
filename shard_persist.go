package dagstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
)

// PersistedShard is the persistent representation of the Shard.
type PersistedShard struct {
	Key           string     `json:"k"`
	URL           string     `json:"u"`
	TransientPath string     `json:"t"`
	State         ShardState `json:"s"`
	Lazy          bool       `json:"l"`
	Error         string     `json:"e"`
}

type PersistStore interface {
	Save(context.Context, PersistedShard) error
	Get(context.Context, shard.Key) (*PersistedShard, error)
	List(context.Context) ([]*PersistedShard, error)
	Close(context.Context) error
}

var _ PersistStore = (*DsPersistStore)(nil)

type DsPersistStore struct {
	store ds.Datastore
}

func NewDsPersistStore(ds ds.Batching) PersistStore {
	// namespace all store operations.
	return &DsPersistStore{namespace.Wrap(ds, StoreNamespace)}
}

func (dsPersistStore *DsPersistStore) Save(ctx context.Context, ps PersistedShard) error {
	psBytes, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("failed to serialize shard state: %w", err)
	}
	// assuming that the datastore is namespaced if need be.
	k := ds.NewKey(ps.Key)
	if err := dsPersistStore.store.Put(ctx, k, psBytes); err != nil {
		return fmt.Errorf("failed to put shard state: %w", err)
	}
	if err := dsPersistStore.store.Sync(ctx, ds.Key{}); err != nil {
		return fmt.Errorf("failed to sync shard state to store: %w", err)
	}
	return nil
}

func (dsPersistStore *DsPersistStore) Get(ctx context.Context, key shard.Key) (*PersistedShard, error) {
	k := ds.NewKey(key.String())
	shardBytes, err := dsPersistStore.store.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("unable to get shard of key %s %w", key, err)
	}
	s := &PersistedShard{}
	if err := json.Unmarshal(shardBytes, s); err != nil {
		return nil, fmt.Errorf("unable to unmarshal shard %w", err)
	}
	return s, nil
}

func (dsPersistStore *DsPersistStore) List(ctx context.Context) ([]*PersistedShard, error) {
	queryResults, err := dsPersistStore.store.Query(ctx, query.Query{})
	if err != nil {
		return nil, fmt.Errorf("failed to recover dagstore state from store: %w", err)
	}
	var result []*PersistedShard
	for {
		res, ok := queryResults.NextSync()
		if !ok {
			return result, nil
		}
		s := &PersistedShard{}
		if err := json.Unmarshal(res.Value, s); err != nil {
			log.Warnf("failed to load shard %s: %s; skipping", shard.KeyFromString(res.Key), err)
			continue
		}
		result = append(result, s)
	}
}

func (dsPersistStore *DsPersistStore) Close(ctx context.Context) error {
	return dsPersistStore.store.Sync(ctx, ds.Key{})
}

// persist persists the shard's state into the supplied Datastore. It calls
// MarshalJSON, which requires holding a shard lock to be safe.
func (s *Shard) persist(ctx context.Context, store PersistStore) error {
	u, err := s.d.mounts.Represent(s.mount)
	if err != nil {
		return fmt.Errorf("failed to encode mount: %w", err)
	}
	ps := PersistedShard{
		Key:           s.key.String(),
		URL:           u.String(),
		State:         s.state,
		Lazy:          s.lazy,
		TransientPath: s.mount.TransientPath(),
	}
	if s.err != nil {
		ps.Error = s.err.Error()
	}
	return store.Save(ctx, ps)
}

func fromPersistedShard(ctx context.Context, d *DAGStore, ps *PersistedShard) (*Shard, error) {
	s := &Shard{d: d}
	// restore basics.
	s.key = shard.KeyFromString(ps.Key)
	s.state = ps.State
	s.lazy = ps.Lazy
	if ps.Error != "" {
		s.err = errors.New(ps.Error)
	}

	// restore mount.
	u, err := url.Parse(ps.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse mount URL: %w", err)
	}
	mnt, err := s.d.mounts.Instantiate(u)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate mount from URL: %w", err)
	}
	s.mount, err = mount.Upgrade(mnt, s.d.throttleReaadyFetch, s.d.config.TransientsDir, s.key.String(), ps.TransientPath)
	if err != nil {
		return nil, fmt.Errorf("failed to apply mount upgrader: %w", err)
	}
	return s, nil
}
