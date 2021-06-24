package dagstore

import "os"

// Scrap encapsulates the scrap area for the DAG store.
//
// The scrap area is where local local copies for remote mounts are kept.
//
// TODO
type Scrap struct {
}

func (s *Scrap) newWriter() {

}

func (s *Scrap) notifyZeroRef() {

}

type TransientFile struct {
	*os.File
}
