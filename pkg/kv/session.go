package kv

import (
	"encoding/json"
	"sync"
	"time"
)

// ClientSession records client sequence progression and cached response payload.
type ClientSession struct {
	LastSeqNum   uint64    `json:"last_seq_num"`
	LastResponse []byte    `json:"last_response"`
	LastUpdated  time.Time `json:"last_updated"`
}

// SessionTable maintains in-memory deduplication state for active clients.
type SessionTable struct {
	mu       sync.RWMutex
	sessions map[string]*ClientSession
}

// NewSessionTable initializes an empty SessionTable.
func NewSessionTable() *SessionTable {
	return &SessionTable{
		sessions: make(map[string]*ClientSession),
	}
}

// CheckOrUpdate performs linearizable deduplication:
// - If seqNum <= session.LastSeqNum: returns session.LastResponse, false (duplicate detected).
// - If seqNum > session.LastSeqNum: executes executeFn(), caches result in session.LastResponse,
//   updates session.LastSeqNum and session.LastUpdated, and returns result and true.
// Concurrency safe and thread-safe.
func (st *SessionTable) CheckOrUpdate(clientId string, seqNum uint64, executeFn func() []byte) ([]byte, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	sess, exists := st.sessions[clientId]
	if !exists {
		var resp []byte
		if executeFn != nil {
			resp = executeFn()
		}
		st.sessions[clientId] = &ClientSession{
			LastSeqNum:   seqNum,
			LastResponse: resp,
			LastUpdated:  time.Now(),
		}
		return resp, true
	}

	if seqNum <= sess.LastSeqNum {
		return sess.LastResponse, false
	}

	var resp []byte
	if executeFn != nil {
		resp = executeFn()
	}
	sess.LastSeqNum = seqNum
	sess.LastResponse = resp
	sess.LastUpdated = time.Now()
	return resp, true
}

// GetSession retrieves a snapshot of the client session if present.
func (st *SessionTable) GetSession(clientId string) (*ClientSession, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	sess, exists := st.sessions[clientId]
	if !exists {
		return nil, false
	}
	cp := *sess
	return &cp, true
}

// Len returns the count of active sessions.
func (st *SessionTable) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.sessions)
}

// Prune removes sessions older than the specified duration.
func (st *SessionTable) Prune(ttl time.Duration) int {
	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now()
	pruned := 0
	for id, sess := range st.sessions {
		if now.Sub(sess.LastUpdated) > ttl {
			delete(st.sessions, id)
			pruned++
		}
	}
	return pruned
}

// Snapshot serializes all client sessions into JSON.
func (st *SessionTable) Snapshot() ([]byte, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return json.Marshal(st.sessions)
}

// Restore deserializes client sessions from JSON.
func (st *SessionTable) Restore(data []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	var sessions map[string]*ClientSession
	if err := json.Unmarshal(data, &sessions); err != nil {
		return err
	}
	st.sessions = sessions
	return nil
}
