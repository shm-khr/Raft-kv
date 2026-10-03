package kv

import "errors"

var (
	ErrNotLeader = errors.New("not leader")
	ErrTimeout   = errors.New("timed out")
)

const (
	OpSet = "SET"
	OpDel = "DEL"
)

// Command is what gets replicated through the Raft log. It lives in the kv
// package (not raft) because Raft treats commands as opaque.
type Command struct {
	Op    string // OpSet or OpDel
	Key   string
	Value string // unused for OpDel
}