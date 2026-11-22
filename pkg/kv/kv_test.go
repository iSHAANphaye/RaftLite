package kv

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 1. Basic KVEngine Operations
func TestKVEngine_BasicOperations(t *testing.T) {
	e := NewKVEngine()
	assert.Equal(t, 0, e.Len())

	// Non-existent key
	val, ok := e.Get("missing")
	assert.False(t, ok)
	assert.Empty(t, val)

	// Put and Get
	e.Put("key1", "val1")
	assert.Equal(t, 1, e.Len())
	val, ok = e.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "val1", val)

	// Overwrite
	e.Put("key1", "val2")
	val, ok = e.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "val2", val)

	// Clone check
	cp := e.Clone()
	assert.Equal(t, map[string]string{"key1": "val2"}, cp)

	// Delete
	deleted := e.Delete("key1")
	assert.True(t, deleted)
	assert.Equal(t, 0, e.Len())

	// Delete non-existent
	deleted = e.Delete("key1")
	assert.False(t, deleted)

	val, ok = e.Get("key1")
	assert.False(t, ok)
	assert.Empty(t, val)
}

// 2. CAS Semantics (Matching, Mismatch, Non-existent key)
func TestKVEngine_CAS(t *testing.T) {
	e := NewKVEngine()

	// Case 1: expected == "" and key does not exist -> sets key to new, returns ("", true)
	curr, swapped := e.CAS("key1", "", "alpha")
	assert.True(t, swapped)
	assert.Empty(t, curr)
	val, ok := e.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "alpha", val)

	// Case 2: key exists and value == expected -> sets key to new, returns (new, true)
	curr, swapped = e.CAS("key1", "alpha", "beta")
	assert.True(t, swapped)
	assert.Equal(t, "beta", curr)
	val, ok = e.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "beta", val)

	// Case 3: key exists and value != expected -> returns (actualCurrent, false)
	curr, swapped = e.CAS("key1", "wrong_val", "gamma")
	assert.False(t, swapped)
	assert.Equal(t, "beta", curr)
	val, ok = e.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "beta", val)

	// Case 4: expected != "" and key does not exist -> returns ("", false)
	curr, swapped = e.CAS("key2", "something", "delta")
	assert.False(t, swapped)
	assert.Empty(t, curr)
	_, ok = e.Get("key2")
	assert.False(t, ok)

	// Case 5: key exists with empty string value, CAS with expected == "" -> sets key to new, returns (new, true)
	e.Put("empty_val_key", "")
	curr, swapped = e.CAS("empty_val_key", "", "filled")
	assert.True(t, swapped)
	assert.Equal(t, "filled", curr)
	val, ok = e.Get("empty_val_key")
	assert.True(t, ok)
	assert.Equal(t, "filled", val)
}

// 3. KVEngine Snapshot and Restore
func TestKVEngine_SnapshotRestore(t *testing.T) {
	e1 := NewKVEngine()
	e1.Put("k1", "v1")
	e1.Put("k2", "v2")
	e1.Put("k3", "v3")

	snap, err := e1.Snapshot()
	require.NoError(t, err)

	e2 := NewKVEngine()
	err = e2.Restore(snap)
	require.NoError(t, err)

	assert.Equal(t, 3, e2.Len())
	v1, ok := e2.Get("k1")
	assert.True(t, ok)
	assert.Equal(t, "v1", v1)
	v2, ok := e2.Get("k2")
	assert.True(t, ok)
	assert.Equal(t, "v2", v2)
	v3, ok := e2.Get("k3")
	assert.True(t, ok)
	assert.Equal(t, "v3", v3)
}

// 4. Session Table Deduplication Semantics
func TestSessionTable_Deduplication(t *testing.T) {
	st := NewSessionTable()
	assert.Equal(t, 0, st.Len())

	executionCount := 0
	execFn1 := func() []byte {
		executionCount++
		return []byte("resp-seq-1")
	}

	// 1. Initial request seq 1
	resp, applied := st.CheckOrUpdate("clientA", 1, execFn1)
	assert.True(t, applied)
	assert.Equal(t, []byte("resp-seq-1"), resp)
	assert.Equal(t, 1, executionCount)
	assert.Equal(t, 1, st.Len())

	sess, ok := st.GetSession("clientA")
	require.True(t, ok)
	assert.Equal(t, uint64(1), sess.LastSeqNum)
	assert.Equal(t, []byte("resp-seq-1"), sess.LastResponse)

	// 2. Duplicate retry with same sequence number (seq 1)
	resp, applied = st.CheckOrUpdate("clientA", 1, func() []byte {
		t.Fatal("executeFn should not be executed on duplicate request")
		return nil
	})
	assert.False(t, applied)
	assert.Equal(t, []byte("resp-seq-1"), resp)
	assert.Equal(t, 1, executionCount)

	// 3. New higher sequence number (seq 2)
	execFn2 := func() []byte {
		executionCount++
		return []byte("resp-seq-2")
	}
	resp, applied = st.CheckOrUpdate("clientA", 2, execFn2)
	assert.True(t, applied)
	assert.Equal(t, []byte("resp-seq-2"), resp)
	assert.Equal(t, 2, executionCount)

	sess, ok = st.GetSession("clientA")
	require.True(t, ok)
	assert.Equal(t, uint64(2), sess.LastSeqNum)
	assert.Equal(t, []byte("resp-seq-2"), sess.LastResponse)

	// 4. Stale/out-of-order sequence number (seq 1 <= LastSeqNum 2)
	resp, applied = st.CheckOrUpdate("clientA", 1, func() []byte {
		t.Fatal("executeFn should not be executed on stale request")
		return nil
	})
	assert.False(t, applied)
	assert.Equal(t, []byte("resp-seq-2"), resp)
	assert.Equal(t, 2, executionCount)

	// 5. Independent client session (clientB, seq 1)
	resp, applied = st.CheckOrUpdate("clientB", 1, func() []byte {
		executionCount++
		return []byte("resp-clientB-1")
	})
	assert.True(t, applied)
	assert.Equal(t, []byte("resp-clientB-1"), resp)
	assert.Equal(t, 3, executionCount)
	assert.Equal(t, 2, st.Len())

	// 6. Prune session table
	pruned := st.Prune(1 * time.Hour)
	assert.Equal(t, 0, pruned)
	assert.Equal(t, 2, st.Len())

	// Snapshot and restore session table
	snap, err := st.Snapshot()
	require.NoError(t, err)

	st2 := NewSessionTable()
	err = st2.Restore(snap)
	require.NoError(t, err)
	assert.Equal(t, 2, st2.Len())
	sessB, ok := st2.GetSession("clientB")
	require.True(t, ok)
	assert.Equal(t, uint64(1), sessB.LastSeqNum)
	assert.Equal(t, []byte("resp-clientB-1"), sessB.LastResponse)
}

// 5. Command Encoding and Decoding
func TestCommand_EncodingDecoding(t *testing.T) {
	tests := []struct {
		name string
		cmd  *Command
	}{
		{
			name: "PUT command",
			cmd: &Command{
				Op:          OpPut,
				Key:         "user:1",
				Value:       "alice",
				ClientID:    "client-1",
				SequenceNum: 42,
			},
		},
		{
			name: "GET command",
			cmd: &Command{
				Op:  OpGet,
				Key: "user:1",
			},
		},
		{
			name: "CAS command",
			cmd: &Command{
				Op:            OpCAS,
				Key:           "counter",
				ExpectedValue: "10",
				Value:         "11",
				ClientID:      "client-2",
				SequenceNum:   99,
			},
		},
		{
			name: "DELETE command",
			cmd: &Command{
				Op:          OpDelete,
				Key:         "user:1",
				ClientID:    "client-1",
				SequenceNum: 43,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeCommand(tc.cmd)
			require.NoError(t, err)

			decoded, err := DecodeCommand(encoded)
			require.NoError(t, err)

			assert.Equal(t, tc.cmd.Op, decoded.Op)
			assert.Equal(t, tc.cmd.Key, decoded.Key)
			assert.Equal(t, tc.cmd.Value, decoded.Value)
			assert.Equal(t, tc.cmd.ExpectedValue, decoded.ExpectedValue)
			assert.Equal(t, tc.cmd.ClientID, decoded.ClientID)
			assert.Equal(t, tc.cmd.SequenceNum, decoded.SequenceNum)
		})
	}

	// Error handling
	_, err := EncodeCommand(nil)
	assert.Error(t, err)

	_, err = DecodeCommand(nil)
	assert.Error(t, err)

	_, err = DecodeCommand([]byte("invalid json"))
	assert.Error(t, err)
}

// 6. Applier Pipeline: Sequential Execution and Waiter Notifications
func TestApplier_Pipeline(t *testing.T) {
	applyCh := make(chan ApplyMsg, 16)
	applier := NewApplier(nil, nil, applyCh)
	defer applier.Close()

	// 1. Put
	putCmd := &Command{
		Op:          OpPut,
		Key:         "score",
		Value:       "100",
		ClientID:    "clientA",
		SequenceNum: 1,
	}
	putBytes, err := EncodeCommand(putCmd)
	require.NoError(t, err)

	waiter1 := applier.Register(1)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      putBytes,
		CommandIndex: 1,
		CommandTerm:  1,
	}

	select {
	case respBytes := <-waiter1:
		var putResp ClientPutResponse
		err := json.Unmarshal(respBytes, &putResp)
		require.NoError(t, err)
		assert.True(t, putResp.Success)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PUT response")
	}

	assert.Equal(t, uint64(1), applier.LastApplied())
	val, ok := applier.Engine().Get("score")
	assert.True(t, ok)
	assert.Equal(t, "100", val)

	// 2. Get
	getCmd := &Command{
		Op:  OpGet,
		Key: "score",
	}
	getBytes, err := EncodeCommand(getCmd)
	require.NoError(t, err)

	waiter2 := applier.Register(2)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      getBytes,
		CommandIndex: 2,
		CommandTerm:  1,
	}

	select {
	case respBytes := <-waiter2:
		var getResp ClientGetResponse
		err := json.Unmarshal(respBytes, &getResp)
		require.NoError(t, err)
		assert.True(t, getResp.Found)
		assert.Equal(t, "100", getResp.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GET response")
	}

	// 3. CAS Matching
	casCmd := &Command{
		Op:            OpCAS,
		Key:           "score",
		ExpectedValue: "100",
		Value:         "200",
		ClientID:      "clientA",
		SequenceNum:   2,
	}
	casBytes, err := EncodeCommand(casCmd)
	require.NoError(t, err)

	waiter3 := applier.Register(3)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      casBytes,
		CommandIndex: 3,
		CommandTerm:  1,
	}

	select {
	case respBytes := <-waiter3:
		var casResp ClientCASResponse
		err := json.Unmarshal(respBytes, &casResp)
		require.NoError(t, err)
		assert.True(t, casResp.Success)
		assert.True(t, casResp.Swapped)
		assert.Equal(t, "200", casResp.CurrentValue)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for CAS response")
	}

	val, _ = applier.Engine().Get("score")
	assert.Equal(t, "200", val)

	// 4. CAS Mismatch
	casMismatchCmd := &Command{
		Op:            OpCAS,
		Key:           "score",
		ExpectedValue: "wrong_guess",
		Value:         "300",
		ClientID:      "clientA",
		SequenceNum:   3,
	}
	casMismatchBytes, err := EncodeCommand(casMismatchCmd)
	require.NoError(t, err)

	waiter4 := applier.Register(4)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      casMismatchBytes,
		CommandIndex: 4,
		CommandTerm:  1,
	}

	select {
	case respBytes := <-waiter4:
		var casResp ClientCASResponse
		err := json.Unmarshal(respBytes, &casResp)
		require.NoError(t, err)
		assert.True(t, casResp.Success)
		assert.False(t, casResp.Swapped)
		assert.Equal(t, "200", casResp.CurrentValue)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for CAS mismatch response")
	}

	// 5. Delete
	delCmd := &Command{
		Op:          OpDelete,
		Key:         "score",
		ClientID:    "clientA",
		SequenceNum: 4,
	}
	delBytes, err := EncodeCommand(delCmd)
	require.NoError(t, err)

	waiter5 := applier.Register(5)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      delBytes,
		CommandIndex: 5,
		CommandTerm:  1,
	}

	select {
	case respBytes := <-waiter5:
		var delResp ClientDeleteResponse
		err := json.Unmarshal(respBytes, &delResp)
		require.NoError(t, err)
		assert.True(t, delResp.Success)
		assert.True(t, delResp.Found)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DELETE response")
	}

	_, ok = applier.Engine().Get("score")
	assert.False(t, ok)
	assert.Equal(t, uint64(5), applier.LastApplied())
}

// 7. Applier Deduplication with Retried Client Requests
func TestApplier_Deduplication(t *testing.T) {
	applyCh := make(chan ApplyMsg, 16)
	applier := NewApplier(nil, nil, applyCh)
	defer applier.Close()

	// Initial mutation: Put("k", "v1") with ClientID="client1", Seq=1
	cmd1 := &Command{
		Op:          OpPut,
		Key:         "k",
		Value:       "v1",
		ClientID:    "client1",
		SequenceNum: 1,
	}
	data1, err := EncodeCommand(cmd1)
	require.NoError(t, err)

	w1 := applier.Register(1)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      data1,
		CommandIndex: 1,
	}

	select {
	case resp := <-w1:
		var r ClientPutResponse
		require.NoError(t, json.Unmarshal(resp, &r))
		assert.True(t, r.Success)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cmd1")
	}

	val, _ := applier.Engine().Get("k")
	assert.Equal(t, "v1", val)

	// Duplicate retry: same ClientID="client1", same Seq=1, but trying to overwrite with "corrupted_v2"
	cmd1Retry := &Command{
		Op:          OpPut,
		Key:         "k",
		Value:       "corrupted_v2",
		ClientID:    "client1",
		SequenceNum: 1,
	}
	data1Retry, err := EncodeCommand(cmd1Retry)
	require.NoError(t, err)

	w2 := applier.Register(2)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      data1Retry,
		CommandIndex: 2,
	}

	select {
	case resp := <-w2:
		var r ClientPutResponse
		require.NoError(t, json.Unmarshal(resp, &r))
		// Cached response returned, but mutation MUST NOT have executed
		assert.True(t, r.Success)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cmd1Retry")
	}

	// State machine value MUST still be "v1"
	val, _ = applier.Engine().Get("k")
	assert.Equal(t, "v1", val, "duplicate mutation must not overwrite key-value store")

	// Next sequential command: ClientID="client1", Seq=2
	cmd2 := &Command{
		Op:          OpPut,
		Key:         "k",
		Value:       "v2",
		ClientID:    "client1",
		SequenceNum: 2,
	}
	data2, err := EncodeCommand(cmd2)
	require.NoError(t, err)

	w3 := applier.Register(3)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      data2,
		CommandIndex: 3,
	}

	select {
	case resp := <-w3:
		var r ClientPutResponse
		require.NoError(t, json.Unmarshal(resp, &r))
		assert.True(t, r.Success)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cmd2")
	}

	val, _ = applier.Engine().Get("k")
	assert.Equal(t, "v2", val)

	// Stale/out-of-order request: ClientID="client1", Seq=1 arrives later
	cmdStale := &Command{
		Op:          OpPut,
		Key:         "k",
		Value:       "stale_overwrite",
		ClientID:    "client1",
		SequenceNum: 1,
	}
	dataStale, err := EncodeCommand(cmdStale)
	require.NoError(t, err)

	w4 := applier.Register(4)
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      dataStale,
		CommandIndex: 4,
	}

	select {
	case resp := <-w4:
		var r ClientPutResponse
		require.NoError(t, json.Unmarshal(resp, &r))
		assert.True(t, r.Success)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cmdStale")
	}

	val, _ = applier.Engine().Get("k")
	assert.Equal(t, "v2", val, "stale request must not overwrite current state")
}

// 8. Snapshot Application via ApplyMsg
func TestApplier_Snapshot(t *testing.T) {
	applyCh := make(chan ApplyMsg, 16)
	applier := NewApplier(nil, nil, applyCh)
	defer applier.Close()

	// Initial data
	applier.Engine().Put("old_key", "old_val")

	// Snapshot payload containing new state
	snapState := SnapshotState{
		Data: map[string]string{
			"snap_k1": "snap_v1",
			"snap_k2": "snap_v2",
		},
		Sessions: map[string]*ClientSession{
			"clientSnap": {
				LastSeqNum:   50,
				LastResponse: []byte(`{"success":true}`),
				LastUpdated:  time.Now(),
			},
		},
	}
	snapBytes, err := json.Marshal(snapState)
	require.NoError(t, err)

	applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapBytes,
		SnapshotIndex: 100,
		SnapshotTerm:  5,
	}

	// Allow brief time for snapshot message to be processed
	require.Eventually(t, func() bool {
		return applier.LastApplied() == 100
	}, 2*time.Second, 10*time.Millisecond)

	v1, ok := applier.Engine().Get("snap_k1")
	assert.True(t, ok)
	assert.Equal(t, "snap_v1", v1)

	v2, ok := applier.Engine().Get("snap_k2")
	assert.True(t, ok)
	assert.Equal(t, "snap_v2", v2)

	// Verify session restored
	sess, ok := applier.Sessions().GetSession("clientSnap")
	assert.True(t, ok)
	assert.Equal(t, uint64(50), sess.LastSeqNum)
}

// 9. Clean Shutdown and Deregistration
func TestApplier_CloseAndCleanup(t *testing.T) {
	applyCh := make(chan ApplyMsg, 8)
	applier := NewApplier(nil, nil, applyCh)

	w1 := applier.Register(10)
	w2 := applier.Register(20)
	assert.NotNil(t, w2)

	// Deregister w2
	applier.Deregister(20)

	// Close applier
	applier.Close()

	// w1 should be closed by drainWaitersOnExit
	select {
	case _, ok := <-w1:
		assert.False(t, ok, "waiter channel should be closed upon applier shutdown")
	case <-time.After(1 * time.Second):
		t.Fatal("expected waiter channel to be closed on shutdown")
	}

	// Registering after close should return a closed channel
	w3 := applier.Register(30)
	select {
	case _, ok := <-w3:
		assert.False(t, ok)
	default:
		t.Fatal("expected channel registered after close to be closed immediately")
	}
}

// 10. Concurrency Tests under -race
func TestConcurrent_KVEngine(t *testing.T) {
	e := NewKVEngine()
	const numGoroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(gid int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", gid%10)

			for j := 0; j < opsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					e.Put(key, fmt.Sprintf("val-%d-%d", gid, j))
				case 1:
					_, _ = e.Get(key)
				case 2:
					_, _ = e.CAS(key, fmt.Sprintf("val-%d-%d", gid, j-1), fmt.Sprintf("cas-%d-%d", gid, j))
				case 3:
					_ = e.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrent_SessionTable(t *testing.T) {
	st := NewSessionTable()
	const numGoroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(gid int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client-%d", gid%5)

			for j := 0; j < opsPerGoroutine; j++ {
				seq := uint64(j)
				_, _ = st.CheckOrUpdate(clientID, seq, func() []byte {
					return []byte(fmt.Sprintf("resp-%d-%d", gid, j))
				})
				_, _ = st.GetSession(clientID)
				_ = st.Len()
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrent_Applier(t *testing.T) {
	applyCh := make(chan ApplyMsg, 200)
	applier := NewApplier(nil, nil, applyCh)
	defer applier.Close()

	const numWriters = 20
	const opsPerWriter = 25
	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := 0; i < numWriters; i++ {
		go func(wid int) {
			defer wg.Done()
			for j := 0; j < opsPerWriter; j++ {
				idx := uint64(wid*opsPerWriter + j + 1)
				w := applier.Register(idx)

				cmd := &Command{
					Op:          OpPut,
					Key:         fmt.Sprintf("k-%d", wid),
					Value:       fmt.Sprintf("v-%d-%d", wid, j),
					ClientID:    fmt.Sprintf("client-%d", wid),
					SequenceNum: uint64(j + 1),
				}
				cmdBytes, err := EncodeCommand(cmd)
				if err != nil {
					t.Errorf("failed to encode: %v", err)
					return
				}

				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      cmdBytes,
					CommandIndex: idx,
					CommandTerm:  1,
				}

				select {
				case resp := <-w:
					var r ClientPutResponse
					_ = json.Unmarshal(resp, &r)
					if !r.Success {
						t.Errorf("expected success for writer %d op %d", wid, j)
					}
				case <-time.After(5 * time.Second):
					t.Errorf("timed out waiting for idx %d", idx)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, uint64(numWriters*opsPerWriter), applier.LastApplied())
}
