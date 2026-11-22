package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const MetaFileName = "raft.meta"

type RaftMetadata struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// SaveMeta atomically saves currentTerm and votedFor using temp file write + fsync + atomic rename.
func SaveMeta(dir string, term uint64, votedFor string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("meta: mkdir failed: %w", err)
	}

	meta := RaftMetadata{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("meta: marshal failed: %w", err)
	}

	metaPath := filepath.Join(dir, MetaFileName)
	tmpPath := filepath.Join(dir, MetaFileName+".tmp")

	// Write to temporary file
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("meta: create tmp file failed: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("meta: write failed: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("meta: sync failed: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("meta: close tmp failed: %w", err)
	}

	// Atomic replace
	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("meta: atomic rename failed: %w", err)
	}

	return nil
}

// LoadMeta reads currentTerm and votedFor from the metadata file.
// If the file does not exist, returns 0, "", nil.
func LoadMeta(dir string) (uint64, string, error) {
	metaPath := filepath.Join(dir, MetaFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("meta: read failed: %w", err)
	}

	var meta RaftMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, "", fmt.Errorf("meta: parse failed: %w", err)
	}

	return meta.CurrentTerm, meta.VotedFor, nil
}
