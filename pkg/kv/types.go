package kv

import (
	"encoding/json"
	"errors"
)

// OpType defines the operation type for replicated key-value state machine commands.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpCAS    OpType = "CAS"
	OpDelete OpType = "DELETE"
)

// Command represents a replicated state machine operation payload.
type Command struct {
	Op            OpType `json:"op"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	ExpectedValue string `json:"expected_value"`
	ClientID      string `json:"client_id"`
	SequenceNum   uint64 `json:"sequence_num"`
}

// EncodeCommand serializes a Command into JSON bytes.
func EncodeCommand(cmd *Command) ([]byte, error) {
	if cmd == nil {
		return nil, errors.New("kv: cannot encode nil command")
	}
	return json.Marshal(cmd)
}

// DecodeCommand deserializes JSON bytes into a Command.
func DecodeCommand(data []byte) (*Command, error) {
	if len(data) == 0 {
		return nil, errors.New("kv: cannot decode empty command bytes")
	}
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// ApplyMsg represents a committed entry or snapshot sent from the Raft consensus core.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

// ClientPutResponse represents the outcome of a ClientPut operation.
type ClientPutResponse struct {
	Success    bool   `json:"success"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

// ClientGetResponse represents the outcome of a ClientGet operation.
type ClientGetResponse struct {
	Found      bool   `json:"found"`
	Value      string `json:"value,omitempty"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

// ClientCASResponse represents the outcome of a ClientCAS operation.
type ClientCASResponse struct {
	Success      bool   `json:"success"`
	Swapped      bool   `json:"swapped"`
	CurrentValue string `json:"current_value,omitempty"`
	Error        string `json:"error,omitempty"`
	LeaderHint   string `json:"leader_hint,omitempty"`
}

// ClientDeleteResponse represents the outcome of a ClientDelete operation.
type ClientDeleteResponse struct {
	Success    bool   `json:"success"`
	Found      bool   `json:"found"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

// Type aliases for ease of use across layers.
type PutResponse = ClientPutResponse
type GetResponse = ClientGetResponse
type CASResponse = ClientCASResponse
type DeleteResponse = ClientDeleteResponse
