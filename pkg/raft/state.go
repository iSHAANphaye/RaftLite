package raft

import (
	"sync"

	"github.com/raftlite/pkg/storage"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
)

// RaftState encapsulates the persistent and volatile state of a Raft node.
type RaftState struct {
	mu sync.RWMutex

	// Persistent state on all servers (must persist before replying to RPCs)
	currentTerm uint64
	votedFor    string
	log         []*raftlitev1.LogEntry // 1-based index (dummy at index 0)
	wal         *storage.WAL
	storageDir  string

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	role        NodeRole
	leaderID    string

	// Volatile state on leaders (re-initialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

// NewRaftState initializes state from persistent storage or clean defaults.
func NewRaftState(dir string, syncOnWrite bool) (*RaftState, error) {
	// Crash recovery: recover WAL and discard torn writes
	wal, recoveredEntries, err := storage.RecoverWAL(dir, syncOnWrite)
	if err != nil {
		return nil, err
	}

	// Load persistent metadata (term and votedFor)
	term, votedFor, err := storage.LoadMeta(dir)
	if err != nil {
		wal.Close()
		return nil, err
	}

	// Initialize log with dummy entry at index 0
	logEntries := make([]*raftlitev1.LogEntry, 0, len(recoveredEntries)+1)
	logEntries = append(logEntries, &raftlitev1.LogEntry{
		Term:  0,
		Index: 0,
		Type:  raftlitev1.LogEntryType_ENTRY_TYPE_UNSPECIFIED,
		Data:  nil,
	})
	logEntries = append(logEntries, recoveredEntries...)

	var lastCommit uint64
	if len(recoveredEntries) > 0 {
		lastCommit = 0 // commitIndex initialized to 0 upon startup
	}

	s := &RaftState{
		currentTerm: term,
		votedFor:    votedFor,
		log:         logEntries,
		wal:         wal,
		storageDir:  dir,
		commitIndex: lastCommit,
		lastApplied: 0,
		role:        RoleFollower,
		leaderID:    "",
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
	}

	return s, nil
}

// LastLogIndexAndTerm returns the index and term of the last entry in the log.
// Must be called with lock held or via thread-safe getter.
func (s *RaftState) lastLogIndexAndTermLocked() (uint64, uint64) {
	lastEntry := s.log[len(s.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// LastLogIndexAndTerm is the thread-safe version.
func (s *RaftState) LastLogIndexAndTerm() (uint64, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastLogIndexAndTermLocked()
}

// persistMeta writes currentTerm and votedFor to disk atomically.
func (s *RaftState) persistMetaLocked() error {
	return storage.SaveMeta(s.storageDir, s.currentTerm, s.votedFor)
}
