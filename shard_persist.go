package dagstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
)

// PersistedShard is the persistent representation of the Shard.
type PersistedShard struct {
	Key           string     `json:"k"`
	URL           string     `json:"u"`
	State         ShardState `json:"s"`
	TransientPath string     `json:"t"`
	Error         string     `json:"e"`
}

// MarshalJSON returns a serialized representation of the state. It must be
// called from inside the event loop, as it accesses mutable state, or under a
// shard read lock.
func (s *Shard) MarshalJSON() ([]byte, error) {
	u, err := s.d.mounts.Represent(s.mount)
	if err != nil {
		return nil, fmt.Errorf("failed to encode mount: %w", err)
	}
	ps := PersistedShard{
		Key:           s.key.String(),
		URL:           u.String(),
		State:         s.state,
		TransientPath: s.mount.TransientPath(),
	}
	if s.err != nil {
		ps.Error = s.err.Error()
	}

	return json.Marshal(ps)
	// TODO maybe switch to CBOR, as it's probably faster.
	// var b bytes.Buffer
	// if err := ps.MarshalCBOR(&b); err != nil {
	// 	return nil, err
	// }
	// return b.Bytes(), nil
}

func (s *Shard) UnmarshalJSON(b []byte) error {
	var ps PersistedShard // TODO try to avoid this alloc by marshalling/unmarshalling directly.
	if err := json.Unmarshal(b, &ps); err != nil {
		return err
	}

	// restore basics.
	s.key = shard.KeyFromString(ps.Key)
	s.state = ps.State
	if ps.Error != "" {
		s.err = errors.New(ps.Error)
	}

	// restore mount.
	u, err := url.Parse(ps.URL)
	if err != nil {
		return fmt.Errorf("failed to parse mount URL: %w", err)
	}
	mnt, err := s.d.mounts.Instantiate(u)
	if err != nil {
		return fmt.Errorf("failed to instantiate mount from URL: %w", err)
	}
	s.mount, err = mount.Upgrade(mnt, s.d.config.TransientsDir, ps.TransientPath)
	if err != nil {
		return fmt.Errorf("failed to apply mount upgrader: %w", err)
	}

	return nil
}

func (s *Shard) persist(store ds.Datastore) error {
	ps, err := s.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize shard state: %w", err)
	}
	// assuming that the datastore is namespaced if need be.
	k := ds.NewKey(s.key.String())
	if err := store.Put(k, ps); err != nil {
		return fmt.Errorf("failed to put shard state: %w", err)
	}
	if err := store.Sync(ds.Key{}); err != nil {
		return fmt.Errorf("failed to sync shard state to store: %w", err)
	}
	return nil
}
