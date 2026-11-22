package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

const WALFileName = "raft.wal"

// WAL represents an append-only binary Write-Ahead Log.
type WAL struct {
	mu          sync.RWMutex
	file        *os.File
	path        string
	syncOnWrite bool
	lastIndex   uint64
	lastTerm    uint64

	// in-memory index -> file offset map for efficient truncation
	offsets map[uint64]int64
	entries []*raftlitev1.LogEntry
}

// OpenWAL opens or creates the WAL file inside the given directory.
func OpenWAL(dir string, syncOnWrite bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	walPath := filepath.Join(dir, WALFileName)
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open file: %w", err)
	}

	w := &WAL{
		file:        file,
		path:        walPath,
		syncOnWrite: syncOnWrite,
		offsets:     make(map[uint64]int64),
		entries:     make([]*raftlitev1.LogEntry, 0),
	}

	// Replay and index existing entries
	if err := w.replay(); err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: replay error: %w", err)
	}

	return w, nil
}

func (w *WAL) replay() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var currentOffset int64
	for {
		entry, bytesRead, err := DecodeFrame(w.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		w.offsets[entry.Index] = currentOffset
		w.entries = append(w.entries, entry)
		w.lastIndex = entry.Index
		w.lastTerm = entry.Term
		currentOffset += bytesRead
	}

	return nil
}

// Append writes a single LogEntry to disk and updates in-memory state.
func (w *WAL) Append(entry *raftlitev1.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	frame, err := EncodeFrame(entry)
	if err != nil {
		return err
	}

	// Seek to end
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(frame); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	w.offsets[entry.Index] = offset
	w.entries = append(w.entries, entry)
	w.lastIndex = entry.Index
	w.lastTerm = entry.Term

	return nil
}

// AppendBatch writes multiple LogEntries sequentially and performs a single fsync.
func (w *WAL) AppendBatch(entries []*raftlitev1.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		frame, err := EncodeFrame(entry)
		if err != nil {
			return err
		}

		frameLen := int64(len(frame))
		if _, err := w.file.Write(frame); err != nil {
			return err
		}

		w.offsets[entry.Index] = offset
		w.entries = append(w.entries, entry)
		w.lastIndex = entry.Index
		w.lastTerm = entry.Term
		offset += frameLen
	}

	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// ReadAll returns a copy of all loaded entries.
func (w *WAL) ReadAll() ([]*raftlitev1.LogEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make([]*raftlitev1.LogEntry, len(w.entries))
	copy(result, w.entries)
	return result, nil
}

// Truncate excises all entries with index > lastIndex from the log and disk.
func (w *WAL) Truncate(lastIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if lastIndex >= w.lastIndex {
		return nil
	}

	// Find the file offset where lastIndex ends (start of lastIndex + 1)
	cutIndex := lastIndex + 1
	cutOffset, exists := w.offsets[cutIndex]
	if !exists {
		if lastIndex == 0 {
			cutOffset = 0
		} else {
			// Find first entry with Index > lastIndex
			found := false
			for _, e := range w.entries {
				if e.Index > lastIndex {
					cutOffset = w.offsets[e.Index]
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
	}

	if err := w.file.Truncate(cutOffset); err != nil {
		return fmt.Errorf("wal: truncate failed: %w", err)
	}

	if _, err := w.file.Seek(cutOffset, io.SeekStart); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	// Update in-memory state
	newEntries := make([]*raftlitev1.LogEntry, 0, lastIndex)
	for _, e := range w.entries {
		if e.Index <= lastIndex {
			newEntries = append(newEntries, e)
		} else {
			delete(w.offsets, e.Index)
		}
	}
	w.entries = newEntries

	if len(w.entries) > 0 {
		w.lastIndex = w.entries[len(w.entries)-1].Index
		w.lastTerm = w.entries[len(w.entries)-1].Term
	} else {
		w.lastIndex = 0
		w.lastTerm = 0
	}

	return nil
}

// LastIndexAndTerm returns the index and term of the last entry in the WAL.
func (w *WAL) LastIndexAndTerm() (uint64, uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastIndex, w.lastTerm
}

// Close closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}
