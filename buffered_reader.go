package dagstore

import (
	"bufio"

	"github.com/filecoin-project/dagstore/mount"
)

type BufferedReader struct {
	r  mount.Reader
	br *bufio.Reader
}

func NewBufferedReader(r mount.Reader, size int) *BufferedReader {
	return &BufferedReader{
		r:  r,
		br: bufio.NewReaderSize(r, size),
	}
}

func (b *BufferedReader) Close() error {
	return b.r.Close()
}

func (b *BufferedReader) Read(p []byte) (n int, err error) {
	return b.br.Read(p)
}

func (b *BufferedReader) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = b.r.Seek(off, 0) // SeekStart
	if err != nil {
		return 0, err
	}
	return b.br.Read(p)
}

func (b *BufferedReader) Seek(offset int64, whence int) (int64, error) {
	return b.r.Seek(offset, whence)
}
