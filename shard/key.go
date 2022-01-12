package shard

import (
	"encoding/json"

	"github.com/ipfs/go-cid"
	"github.com/mr-tron/base58"
)

// Key represents a shard key. It can be instantiated from a string, a byte
// slice, or a CID.
type Key struct {
	// str stores a string or an arbitrary byte slice in base58 form. We cannot
	// store the raw byte slice because this struct needs to be comparable.
	str string
}

// KeyFromString returns a key representing an arbitrary string.
func KeyFromString(str string) Key {
	return Key{str: str}
}

// KeyFromBytes returns a key from a byte slice, encoding it in b58 first.
func KeyFromBytes(b []byte) Key {
	return Key{str: base58.Encode(b)}
}

// KeyFromCID returns a key representing a CID.
func KeyFromCID(cid cid.Cid) Key {
	return Key{str: cid.String()}
}

// String returns the string representation for this key.
func (k Key) String() string {
	return k.str
}

//
// We need a custom JSON marshaller and unmarshaller because str is a
// private field
//
func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.str)
}

func (k *Key) UnmarshalJSON(bz []byte) error {
	return json.Unmarshal(bz, &k.str)
}
