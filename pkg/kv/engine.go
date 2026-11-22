package kv

import (
	"encoding/json"
	"sync"
)

// KVEngine provides an in-memory key-value data store protected by a read-write mutex.
type KVEngine struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVEngine initializes an empty in-memory KVEngine.
func NewKVEngine() *KVEngine {
	return &KVEngine{
		data: make(map[string]string),
	}
}

// Put sets the value for the given key.
func (e *KVEngine) Put(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value
}

// Get retrieves the value and existence flag for the given key.
func (e *KVEngine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	val, exists := e.data[key]
	return val, exists
}

// CAS atomically compares and swaps the value for a key:
// - If expected == "" and key does not exist: sets key to new, returns ("", true).
// - If key exists and value == expected: sets key to new, returns (new, true).
// - Otherwise returns (actualCurrent, false).
func (e *KVEngine) CAS(key, expected, new string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	curr, exists := e.data[key]
	if !exists {
		if expected == "" {
			e.data[key] = new
			return "", true
		}
		return "", false
	}

	if curr == expected {
		e.data[key] = new
		return new, true
	}

	return curr, false
}

// Delete removes the key from the store. Returns true if key was present and deleted.
func (e *KVEngine) Delete(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.data[key]; exists {
		delete(e.data, key)
		return true
	}
	return false
}

// Len returns the number of keys in the engine.
func (e *KVEngine) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.data)
}

// Clone returns a shallow copy of the current key-value map.
func (e *KVEngine) Clone() map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	cp := make(map[string]string, len(e.data))
	for k, v := range e.data {
		cp[k] = v
	}
	return cp
}

// Snapshot serializes the in-memory key-value data to JSON.
func (e *KVEngine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return json.Marshal(e.data)
}

// Restore resets the in-memory key-value data from a JSON snapshot.
func (e *KVEngine) Restore(snapshot []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	newData := make(map[string]string)
	if err := json.Unmarshal(snapshot, &newData); err != nil {
		return err
	}
	e.data = newData
	return nil
}
