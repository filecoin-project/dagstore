package dagstore

import (
	"encoding/json"
	"fmt"

	ds "github.com/ipfs/go-datastore"
)

var (
	// StoreNamespace is the namespace under which shard state will be persisted.
	StoreNamespace = ds.NewKey("dagstore")
)

// PersistedShard is the persistent representation of the Shard.
type PersistedShard struct {
	Key           string     `json:"k"`
	URL           string     `json:"u"`
	State         ShardState `json:"s"`
	Indexed       bool       `json:"i"`
	TransientPath string     `json:"t"`
}

// Marshal returns a serialized representation of the state. It must be called
// from inside the event loop, as it accesses mutable state.
func (s *Shard) Marshal() ([]byte, error) {
	ps := PersistedShard{
		Key:           s.key.String(),
		URL:           s.mount.Info().URL.String(),
		State:         s.state,
		Indexed:       s.indexed,
		TransientPath: s.mount.TransientPath(),
	}
	return json.Marshal(ps)
	// TODO maybe switch to CBOR, as it's probably faster.
	// var b bytes.Buffer
	// if err := ps.MarshalCBOR(&b); err != nil {
	// 	return nil, err
	// }
	// return b.Bytes(), nil
}

func (s *Shard) persist(store ds.Datastore) error {
	ps, err := s.Marshal()
	if err != nil {
		return fmt.Errorf("failed to serialize shard state: %w", err)
	}
	k := s.key.String()
	key := StoreNamespace.ChildString(k)
	if err := store.Put(key, ps); err != nil {
		return fmt.Errorf("failed to put shard state: %w", err)
	}
	if err := store.Sync(StoreNamespace); err != nil {
		return fmt.Errorf("failed to sync shard state to store: %w", err)
	}
	return nil
}
