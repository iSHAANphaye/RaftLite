package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Applier listens to committed Raft ApplyMsg entries, applies state changes to
// KVEngine through SessionTable deduplication, and notifies registered client callers.
type Applier struct {
	mu          sync.Mutex
	engine      *KVEngine
	sessions    *SessionTable
	applyCh     <-chan ApplyMsg
	waiters     map[uint64][]chan []byte
	lastApplied uint64
	stopCh      chan struct{}
	wg          sync.WaitGroup
	closed      bool
}

// SnapshotState encapsulates combined KV data and client session deduplication state.
type SnapshotState struct {
	Data     map[string]string         `json:"data"`
	Sessions map[string]*ClientSession `json:"sessions"`
}

// NewApplier creates and starts a new Applier consumer loop.
// If engine or sessions are nil, default in-memory instances are allocated.
func NewApplier(engine *KVEngine, sessions *SessionTable, applyCh <-chan ApplyMsg) *Applier {
	if engine == nil {
		engine = NewKVEngine()
	}
	if sessions == nil {
		sessions = NewSessionTable()
	}
	a := &Applier{
		engine:   engine,
		sessions: sessions,
		applyCh:  applyCh,
		waiters:  make(map[uint64][]chan []byte),
		stopCh:   make(chan struct{}),
	}

	if applyCh != nil {
		a.wg.Add(1)
		go a.run()
	}
	return a
}

// Engine returns the underlying KVEngine.
func (a *Applier) Engine() *KVEngine {
	return a.engine
}

// Sessions returns the underlying SessionTable.
func (a *Applier) Sessions() *SessionTable {
	return a.sessions
}

// LastApplied returns the highest log or snapshot index applied to the state machine.
func (a *Applier) LastApplied() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastApplied
}

// Register registers a notification channel for the specified log index.
// When the command at index is committed and applied, the serialized response is sent.
// Channel is buffered with capacity 1 to ensure sends never block the apply loop.
func (a *Applier) Register(index uint64) chan []byte {
	a.mu.Lock()
	defer a.mu.Unlock()

	ch := make(chan []byte, 1)
	if a.closed {
		close(ch)
		return ch
	}
	a.waiters[index] = append(a.waiters[index], ch)
	return ch
}

// Deregister removes all pending response channels registered for an index.
func (a *Applier) Deregister(index uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.waiters, index)
}

// Close gracefully stops the consumer loop and closes all pending waiter channels.
func (a *Applier) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	close(a.stopCh)
	a.mu.Unlock()

	a.wg.Wait()
}

// Stop is an alias for Close.
func (a *Applier) Stop() {
	a.Close()
}

// CreateSnapshot serializes current state machine data and session deduplication table.
func (a *Applier) CreateSnapshot() ([]byte, error) {
	snap := SnapshotState{
		Data:     a.engine.Clone(),
		Sessions: make(map[string]*ClientSession),
	}
	a.sessions.mu.RLock()
	for k, v := range a.sessions.sessions {
		cp := *v
		snap.Sessions[k] = &cp
	}
	a.sessions.mu.RUnlock()
	return json.Marshal(snap)
}

// RestoreSnapshot deserializes snapshot payload into KVEngine and SessionTable.
func (a *Applier) RestoreSnapshot(data []byte) error {
	var snap SnapshotState
	if err := json.Unmarshal(data, &snap); err == nil && (snap.Data != nil || snap.Sessions != nil) {
		if snap.Data != nil {
			a.engine.mu.Lock()
			a.engine.data = snap.Data
			a.engine.mu.Unlock()
		}
		if snap.Sessions != nil {
			a.sessions.mu.Lock()
			a.sessions.sessions = snap.Sessions
			a.sessions.mu.Unlock()
		}
		return nil
	}
	return a.engine.Restore(data)
}

func (a *Applier) run() {
	defer a.wg.Done()
	defer a.drainWaitersOnExit()

	for {
		select {
		case <-a.stopCh:
			return
		case msg, ok := <-a.applyCh:
			if !ok {
				return
			}
			a.applyMessage(msg)
		}
	}
}

func (a *Applier) drainWaitersOnExit() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for idx, chs := range a.waiters {
		for _, ch := range chs {
			close(ch)
		}
		delete(a.waiters, idx)
	}
}

func (a *Applier) applyMessage(msg ApplyMsg) {
	if msg.SnapshotValid {
		a.mu.Lock()
		if msg.SnapshotIndex > a.lastApplied {
			a.lastApplied = msg.SnapshotIndex
		}
		a.mu.Unlock()

		if len(msg.Snapshot) > 0 {
			_ = a.RestoreSnapshot(msg.Snapshot)
		}
		return
	}

	if !msg.CommandValid {
		return
	}

	a.mu.Lock()
	if msg.CommandIndex > a.lastApplied {
		a.lastApplied = msg.CommandIndex
	}
	a.mu.Unlock()

	var cmd *Command
	switch c := msg.Command.(type) {
	case *Command:
		cmd = c
	case Command:
		cmd = &c
	case []byte:
		var err error
		cmd, err = DecodeCommand(c)
		if err != nil {
			a.notifyWaiters(msg.CommandIndex, encodeErrorResponse(err))
			return
		}
	case string:
		var err error
		cmd, err = DecodeCommand([]byte(c))
		if err != nil {
			a.notifyWaiters(msg.CommandIndex, encodeErrorResponse(err))
			return
		}
	default:
		if msg.Command == nil {
			a.notifyWaiters(msg.CommandIndex, encodeErrorResponse(errors.New("kv: nil command payload")))
			return
		}
		data, err := json.Marshal(msg.Command)
		if err != nil {
			a.notifyWaiters(msg.CommandIndex, encodeErrorResponse(err))
			return
		}
		cmd, err = DecodeCommand(data)
		if err != nil {
			a.notifyWaiters(msg.CommandIndex, encodeErrorResponse(err))
			return
		}
	}

	var respBytes []byte
	executeFn := func() []byte {
		return a.executeCommand(cmd)
	}

	if cmd.ClientID != "" {
		respBytes, _ = a.sessions.CheckOrUpdate(cmd.ClientID, cmd.SequenceNum, executeFn)
	} else {
		respBytes = executeFn()
	}

	a.notifyWaiters(msg.CommandIndex, respBytes)
}

func (a *Applier) executeCommand(cmd *Command) []byte {
	switch cmd.Op {
	case OpPut:
		a.engine.Put(cmd.Key, cmd.Value)
		resp := ClientPutResponse{
			Success: true,
		}
		data, _ := json.Marshal(resp)
		return data

	case OpGet:
		val, found := a.engine.Get(cmd.Key)
		resp := ClientGetResponse{
			Found: found,
			Value: val,
		}
		data, _ := json.Marshal(resp)
		return data

	case OpCAS:
		curr, swapped := a.engine.CAS(cmd.Key, cmd.ExpectedValue, cmd.Value)
		resp := ClientCASResponse{
			Success:      true,
			Swapped:      swapped,
			CurrentValue: curr,
		}
		data, _ := json.Marshal(resp)
		return data

	case OpDelete:
		found := a.engine.Delete(cmd.Key)
		resp := ClientDeleteResponse{
			Success: true,
			Found:   found,
		}
		data, _ := json.Marshal(resp)
		return data

	default:
		return encodeErrorResponse(fmt.Errorf("kv: unsupported operation: %s", cmd.Op))
	}
}

func (a *Applier) notifyWaiters(index uint64, resp []byte) {
	a.mu.Lock()
	chs, exists := a.waiters[index]
	if exists {
		delete(a.waiters, index)
	}
	a.mu.Unlock()

	if exists {
		for _, ch := range chs {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

func encodeErrorResponse(err error) []byte {
	resp := struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}{
		Success: false,
		Error:   err.Error(),
	}
	data, _ := json.Marshal(resp)
	return data
}
