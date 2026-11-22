package storage

import (
	"bytes"
	"path/filepath"
	"testing"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrame_EncodeDecode(t *testing.T) {
	entry := &raftlitev1.LogEntry{
		Term:  3,
		Index: 42,
		Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
		Data:  []byte("SET user:100 bob"),
	}

	frame, err := EncodeFrame(entry)
	require.NoError(t, err)
	assert.Equal(t, HeaderSize+len(entry.Data), len(frame))

	// Decode
	decoded, bytesRead, err := DecodeFrame(bytes.NewReader(frame))
	require.NoError(t, err)
	assert.Equal(t, int64(len(frame)), bytesRead)
	assert.Equal(t, entry.Term, decoded.Term)
	assert.Equal(t, entry.Index, decoded.Index)
	assert.Equal(t, entry.Type, decoded.Type)
	assert.Equal(t, entry.Data, decoded.Data)
}

func TestFrame_CRCMismatch(t *testing.T) {
	entry := &raftlitev1.LogEntry{
		Term:  1,
		Index: 1,
		Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
		Data:  []byte("data-to-corrupt"),
	}

	frame, err := EncodeFrame(entry)
	require.NoError(t, err)

	// Corrupt one payload byte
	frame[len(frame)-1] ^= 0xFF

	_, _, err = DecodeFrame(bytes.NewReader(frame))
	assert.ErrorIs(t, err, ErrCRCMismatch)
}

func TestWAL_AppendAndTruncate(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, true)
	require.NoError(t, err)
	defer wal.Close()

	// Append 5 entries
	for i := uint64(1); i <= 5; i++ {
		err := wal.Append(&raftlitev1.LogEntry{
			Term:  1,
			Index: i,
			Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
			Data:  []byte("val"),
		})
		require.NoError(t, err)
	}

	entries, err := wal.ReadAll()
	require.NoError(t, err)
	assert.Len(t, entries, 5)

	lastIdx, lastTerm := wal.LastIndexAndTerm()
	assert.Equal(t, uint64(5), lastIdx)
	assert.Equal(t, uint64(1), lastTerm)

	// Truncate to index 3 (removes 4 and 5)
	err = wal.Truncate(3)
	require.NoError(t, err)

	entriesAfter, err := wal.ReadAll()
	require.NoError(t, err)
	assert.Len(t, entriesAfter, 3)
	assert.Equal(t, uint64(3), entriesAfter[len(entriesAfter)-1].Index)

	lastIdx, _ = wal.LastIndexAndTerm()
	assert.Equal(t, uint64(3), lastIdx)
}

func TestMeta_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	// Initial load returns default 0, ""
	term, votedFor, err := LoadMeta(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), term)
	assert.Equal(t, "", votedFor)

	// Save
	err = SaveMeta(dir, 7, "node-3")
	require.NoError(t, err)

	// Load
	term, votedFor, err = LoadMeta(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), term)
	assert.Equal(t, "node-3", votedFor)

	// Verify temp file does not linger
	assert.NoFileExists(t, filepath.Join(dir, MetaFileName+".tmp"))
}
