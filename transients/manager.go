package transients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/dagstore/mount"
)

var (
	readBufferSize     = int64(32 * 1024)
	defaultReservation = 134217728 // 128 Mib
	ErrNotEnoughSpace  = errors.New("not enough space")

	log = logging.Logger("dagstore/transients-manager")
)

type TransientsManager struct {
	rootDir string

	maxTransientSize int64

	mu        sync.Mutex
	totalUsed int64
}

func (t *TransientsManager) DeleteTransient(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat transient")
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove transient")
	}

	t.release(fi.Size())
	return nil
}

func (t *TransientsManager) Download(ctx context.Context, mount mount.Mount, dstPath string) error {
	// create the destination file we need to download the mount contents to.
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer dst.Close()

	from, err := mount.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch underlying mount: %w", err)
	}

	/*
		1.Reserve a certain number of bytes with the manager to download
	*/
	totalReserved, downloadErr := func() (int64, error) {
		totalReserved := int64(0)
		for {
			if ctx.Err() != nil {
				return totalReserved, ctx.Err()
			}
			reserved, err := t.reserve(ctx, mount)
			if err != nil {
				return totalReserved, fmt.Errorf("failed to make a reservation: %w", err)
			}
			totalReserved = totalReserved + reserved

			hasMore, err := t.downloadNBytes(ctx, from, reserved, dst)
			if err != nil {
				return totalReserved, fmt.Errorf("failed to download: %w", err)
			}
			if !hasMore {
				return totalReserved, nil
			}
		}
	}()

	// if there was an error downloading, remove the downloaded file and release all the bytes we'd reserved.
	if downloadErr != nil {
		// if we fail to remove the file, something has gone wrong and we shouldn't release the bytes to be on the safe side.
		if err := os.Remove(dstPath); err != nil {
			log.Errorw("failed to remove transient for failed download", "path", dstPath, "error", err)
			return downloadErr
		}
		t.release(totalReserved)
		return downloadErr
	}

	// if the download finished successfully, release (totalReserved - file size) bytes as we've not used those bytes.
	fi, statErr := dst.Stat()
	if statErr != nil {
		if err := os.Remove(dstPath); err != nil {
			log.Errorw("failed to remove transient for failed download", "path", dstPath, "error", err)
			return statErr
		}
		t.release(totalReserved)
		return statErr
	}

	t.release(totalReserved - fi.Size())
	return nil
}

func (t *TransientsManager) reserve(ctx context.Context, mount mount.Mount) (int64, error) {
	st, err := mount.Stat(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to stat underlying mount: %w", err)
	}

	var toReserve int64
	if st.Size == 0 {
		toReserve = int64(defaultReservation)
	} else {
		toReserve = st.Size
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// do we have enough space available ?
	if t.totalUsed+toReserve < t.maxTransientSize {
		t.totalUsed += toReserve
		return toReserve, nil
	}

	return 0, ErrNotEnoughSpace
}

func (t *TransientsManager) release(v int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalUsed = t.totalUsed - v
}

func (t *TransientsManager) downloadNBytes(ctx context.Context, reader mount.Reader, n int64, w io.Writer) (hasMore bool, err error) {
	toDownload := n

	buffer := make([]byte, readBufferSize)
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if toDownload <= 0 {
			return true, nil
		}

		if len(buffer) > int(toDownload) {
			buffer = buffer[0:toDownload]
		}

		read, rerr := reader.Read(buffer)
		toDownload = toDownload - int64(read)
		if read > 0 {
			written, werr := w.Write(buffer[0:read])
			if werr != nil {
				return false, fmt.Errorf("failed to write: %w", werr)
			}

			if written < 0 || read != written {
				return false, fmt.Errorf("read-write mismatch writing to the output file, read=%d, written=%d", read, written)
			}
		}

		if rerr == io.EOF {
			return false, nil
		}

		if rerr != nil {
			return false, rerr
		}
	}
}
