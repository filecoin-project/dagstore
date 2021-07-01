package index

import (
	"io"
	"strings"

	"github.com/ipld/go-car/v2/index"

	"github.com/ipfs/go-cid"
)

type CarFullIndex struct {
	idx index.Index
}

var _ FullIndex = (*CarFullIndex)(nil)

func (i *CarFullIndex) Offset(c cid.Cid) (uint64, error) {
	return i.idx.Get(c)
}

func (i *CarFullIndex) Contains(c cid.Cid) (bool, error) {
	// TODO: Ask for a Has method on index.Index?
	_, err := i.idx.Get(c)
	if err == nil {
		return true, nil
	}
	// TODO: Ask data-systems team to expose errNotFound, or provide an
	// IsNotFound method akin to os.IsNotExist
	// https://github.com/ipld/go-car/blob/6a6375a9a8c5906df4544aa1cf7b9610132c24c0/v2/index/errors.go#L7
	if strings.Contains(err.Error(), "not found") {
		return false, nil
	}
	return false, err
}

func (i *CarFullIndex) Len() (l int64, err error) {
	panic("not yet supported")
}

func (i *CarFullIndex) ForEach(f func(c cid.Cid, offset uint64) (ok bool, err error)) error {
	panic("not yet supported")
}

func (i *CarFullIndex) Marshal(w io.Writer) error {
	return index.WriteTo(i.idx, w)
}

func (i *CarFullIndex) Unmarshal(r io.Reader) error {
	idx, err := index.ReadFrom(r)
	if err != nil {
		return err
	}

	i.idx = idx
	return nil
}
