package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

const (
	// HeaderSize is the fixed 25-byte binary frame header:
	// CRC32(4B) + Term(8B) + Index(8B) + Type(1B) + CommandLen(4B)
	HeaderSize = 25
)

var (
	// ErrCRCMismatch is returned when frame CRC verification fails.
	ErrCRCMismatch = errors.New("wal: frame crc32 checksum mismatch")
	// ErrPayloadTooLarge is returned if CommandLen exceeds reasonable max limit (64MB).
	ErrPayloadTooLarge = errors.New("wal: frame payload exceeds 64MB limit")
)

// EncodeFrame serializes a LogEntry into the binary WAL frame format:
// [CRC32: 4B | Term: 8B | Index: 8B | Type: 1B | CommandLen: 4B | CommandBytes: NB]
func EncodeFrame(entry *raftlitev1.LogEntry) ([]byte, error) {
	cmdLen := len(entry.Data)
	if cmdLen > 64*1024*1024 {
		return nil, ErrPayloadTooLarge
	}

	frameLen := HeaderSize + cmdLen
	buf := make([]byte, frameLen)

	// Offset 4: Term (8B)
	binary.LittleEndian.PutUint64(buf[4:12], entry.Term)
	// Offset 12: Index (8B)
	binary.LittleEndian.PutUint64(buf[12:20], entry.Index)
	// Offset 20: Type (1B)
	buf[20] = byte(entry.Type)
	// Offset 21: CommandLen (4B)
	binary.LittleEndian.PutUint32(buf[21:25], uint32(cmdLen))
	// Offset 25: CommandBytes (NB)
	if cmdLen > 0 {
		copy(buf[25:], entry.Data)
	}

	// Calculate CRC32 over buf[4:] (Term + Index + Type + CommandLen + CommandBytes)
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf, nil
}

// DecodeFrame reads and validates a single binary frame from the reader.
// Returns the decoded LogEntry, the number of bytes read, and any error.
func DecodeFrame(r io.Reader) (*raftlitev1.LogEntry, int64, error) {
	header := make([]byte, HeaderSize)
	n, err := io.ReadFull(r, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, 0, io.EOF
		}
		return nil, int64(n), err
	}

	expectedCRC := binary.LittleEndian.Uint32(header[0:4])
	term := binary.LittleEndian.Uint64(header[4:12])
	index := binary.LittleEndian.Uint64(header[12:20])
	entryType := raftlitev1.LogEntryType(header[20])
	cmdLen := binary.LittleEndian.Uint32(header[21:25])

	if cmdLen > 64*1024*1024 {
		return nil, int64(n), ErrPayloadTooLarge
	}

	payload := make([]byte, cmdLen)
	pn, err := io.ReadFull(r, payload)
	totalBytes := int64(n + pn)
	if err != nil {
		return nil, totalBytes, err
	}

	// Verify CRC32
	crcTable := crc32.MakeTable(crc32.IEEE)
	computedCRC := crc32.Checksum(header[4:25], crcTable)
	if cmdLen > 0 {
		computedCRC = crc32.Update(computedCRC, crcTable, payload)
	}

	if computedCRC != expectedCRC {
		return nil, totalBytes, ErrCRCMismatch
	}

	entry := &raftlitev1.LogEntry{
		Term:  term,
		Index: index,
		Type:  entryType,
		Data:  payload,
	}

	return entry, totalBytes, nil
}
