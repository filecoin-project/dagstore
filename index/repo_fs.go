package index

import (
	"os"
	"path/filepath"

	"golang.org/x/xerrors"

	"github.com/mr-tron/base58"

	"github.com/filecoin-project/dagstore/shard"
)

const repoVersion = "1"

// FullIndexFactory provides a mechanism to load a FullIndex from a file path
type FullIndexFactory interface {
	// Build returns a FullIndex loaded from the given path
	Build(path string) (FullIndex, error)
}

// FSIndexRepo implements FullIndexRepo using the local file system to store
// the indices
type FSIndexRepo struct {
	baseDir string
}

// NewFSRepo creates a new index repo that stores indices on the local
// filesystem with the given base directory as the root
func NewFSRepo(baseDir string) (*FSIndexRepo, error) {
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	l := &FSIndexRepo{baseDir: baseDir}

	// Get the repo version
	bs, err := os.ReadFile(l.versionPath())
	if err != nil {
		// If the repo has not been initialized, write out the repo version file
		if os.IsNotExist(err) {
			err = os.WriteFile(l.versionPath(), []byte(repoVersion), 0666)
			if err != nil {
				return nil, err
			}
			return l, nil
		}

		// There was some other error
		return nil, err
	}

	// Check that this library can read this repo
	if string(bs) != repoVersion {
		return nil, xerrors.Errorf("cannot read existing repo with version %s", bs)
	}

	return l, nil
}

func (l *FSIndexRepo) GetFullIndex(key shard.Key) (FullIndex, error) {
	idx := NewMockFullIndex() // TODO replace with an implementation backed by Carbs serialization/deserialization.
	path := l.indexPath(key)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	err = idx.Unmarshal(f)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (l *FSIndexRepo) AddFullIndex(key shard.Key, index FullIndex) (err error) {
	// Create a file at the key path
	f, err := os.Create(l.indexPath(key))
	if err != nil {
		return err
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// Write the index to the file
	return index.Marshal(f)
}

func (l *FSIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	// Remove the file at the key path
	return true, os.Remove(l.indexPath(key))
}

func (l *FSIndexRepo) StatFullIndex(key shard.Key) (Stat, error) {
	// Stat the file at the key path
	info, err := os.Stat(l.indexPath(key))
	if err != nil {
		// Check if the file exists
		if os.IsNotExist(err) {
			// Should we return ErrNotFound instead of Stat{Exists:false} ?
			return Stat{Exists: false}, nil
		}
		return Stat{}, err
	}

	return Stat{
		Exists: true,
		Size:   uint64(info.Size()),
	}, nil
}

func (l *FSIndexRepo) indexPath(key shard.Key) string {
	return filepath.Join(l.baseDir, base58.Encode(key)+".full.idx")
}

func (l *FSIndexRepo) versionPath() string {
	return filepath.Join(l.baseDir, ".version")
}

var _ FullIndexRepo = (*FSIndexRepo)(nil)
