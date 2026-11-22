package storage

import (
	"os"
	"path/filepath"
	"testing"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecover_TornWritePartialHeader(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, true)
	require.NoError(t, err)

	// Append 10 valid entries
	for i := uint64(1); i <= 10; i++ {
		err := wal.Append(&raftlitev1.LogEntry{
			Term:  1,
			Index: i,
			Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
			Data:  []byte("valid payload"),
		})
		require.NoError(t, err)
	}
	wal.Close()

	// Append 11 garbage bytes (partial header torn write)
	walPath := filepath.Join(dir, WALFileName)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("torn-header"))
	require.NoError(t, err)
	f.Close()

	// Run recovery
	recWAL, entries, err := RecoverWAL(dir, true)
	require.NoError(t, err)
	defer recWAL.Close()

	assert.Len(t, entries, 10)
	lastIdx, _ := recWAL.LastIndexAndTerm()
	assert.Equal(t, uint64(10), lastIdx)

	// Verify appending a new entry succeeds after recovery
	err = recWAL.Append(&raftlitev1.LogEntry{
		Term:  1,
		Index: 11,
		Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
		Data:  []byte("post recovery"),
	})
	require.NoError(t, err)

	allEntries, err := recWAL.ReadAll()
	require.NoError(t, err)
	assert.Len(t, allEntries, 11)
}

func TestRecover_CorruptedPayload(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, true)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		err := wal.Append(&raftlitev1.LogEntry{
			Term:  1,
			Index: i,
			Type:  raftlitev1.LogEntryType_ENTRY_TYPE_COMMAND,
			Data:  []byte("entry data"),
		})
		require.NoError(t, err)
	}
	wal.Close()

	// Corrupt last byte of entry 5
	walPath := filepath.Join(dir, WALFileName)
	stat, err := os.Stat(walPath)
	require.NoError(t, err)

	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	data[len(data)-1] ^= 0x55 // Corrupt CRC or payload
	err = os.WriteFile(walPath, data, 0644)
	require.NoError(t, err)

	// Recover should discard corrupted entry 5 and keep entries 1..4
	recWAL, entries, err := RecoverWAL(dir, true)
	require.NoError(t, err)
	defer recWAL.Close()

	assert.Len(t, entries, 4)
	assert.Equal(t, uint64(4), entries[len(entries)-1].Index)

	newStat, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Less(t, newStat.Size(), stat.Size())
}
