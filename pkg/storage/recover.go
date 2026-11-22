package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// RecoverWAL scans the WAL file, excises any torn writes or corrupted trailing frames,
// and returns an opened, clean WAL instance ready for append operations.
func RecoverWAL(dir string, syncOnWrite bool) (*WAL, []*raftlitev1.LogEntry, error) {
	walPath := filepath.Join(dir, WALFileName)
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// If WAL doesn't exist, simply open a clean one
			wal, err := OpenWAL(dir, syncOnWrite)
			return wal, nil, err
		}
		return nil, nil, fmt.Errorf("recover: failed to open wal: %w", err)
	}

	var validEntries []*raftlitev1.LogEntry
	offsets := make(map[uint64]int64)
	var lastValidOffset int64

	for {
		entry, bytesRead, err := DecodeFrame(file)
		if err != nil {
			if err == io.EOF {
				// Clean EOF reached at frame boundary
				break
			}

			// Corruption, partial header, unexpected EOF, or CRC mismatch detected.
			// Truncate the file at lastValidOffset to discard torn write.
			if err := file.Truncate(lastValidOffset); err != nil {
				file.Close()
				return nil, nil, fmt.Errorf("recover: failed to truncate torn write: %w", err)
			}
			break
		}

		offsets[entry.Index] = lastValidOffset
		validEntries = append(validEntries, entry)
		lastValidOffset += bytesRead
	}

	// Seek to end of valid entries
	if _, err := file.Seek(lastValidOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, err
	}

	var lastIndex, lastTerm uint64
	if len(validEntries) > 0 {
		last := validEntries[len(validEntries)-1]
		lastIndex = last.Index
		lastTerm = last.Term
	}

	wal := &WAL{
		file:        file,
		path:        walPath,
		syncOnWrite: syncOnWrite,
		lastIndex:   lastIndex,
		lastTerm:    lastTerm,
		offsets:     offsets,
		entries:     validEntries,
	}

	return wal, validEntries, nil
}
