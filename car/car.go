package car

import (
	"io"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/multiformats/go-varint"
)

// TODO: This is copied from go-car. Replace with real index iterator when it's ready
func ReadCids(dsreader mount.Reader) ([]cid.Cid, error) {
	_, err := dsreader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	cv2r, err := car.NewReader(ToReaderAt(dsreader))
	if err != nil {
		return nil, err
	}

	reader := ToByteReadSeeker(cv2r.DataReader())
	l, err := varint.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}

	// The Seek call below is equivalent to getting the reader.offset directly.
	// We get it through Seek to only depend on APIs of a typical io.Seeker.
	// This would also reduce refactoring in case the utility reader is moved.
	if _, err = reader.Seek(0, io.SeekCurrent); err != nil {
		return nil, err
	}

	var cids []cid.Cid
	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Null padding; by default it's an error.
		if sectionLen == 0 {
			break
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(reader)
		if err != nil {
			return nil, err
		}

		cids = append(cids, c)

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if _, err = reader.Seek(remainingSectionLen, io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	return cids, nil
}
