package kv

import (
	"fmt"
	"sync"
	"time"

	"raft-kv/raft"
)

const commitTimeout = 2 * time.Second

// waiter is a Put/Delete blocked until its log entry is applied.
type waiter struct {
	term int       // term the entry was proposed in
	done chan bool // receives true if the entry applied at that index is ours
}

// KVServer sits on top of one Raft node. Writes go through the Raft log and
// are acknowledged only once committed and applied; reads are served locally
// by the leader.
type KVServer struct {
	mu      sync.Mutex
	rf      *raft.Raft
	store   *KVStore
	waiters map[int]waiter // log index -> blocked writer
}

// NewKVServer creates the server and starts its apply loop, which consumes
// committed entries from applyCh (the same channel given to raft.NewRaft).
func NewKVServer(rf *raft.Raft, applyCh <-chan raft.ApplyMsg) *KVServer {
	s := &KVServer{
		rf:      rf,
		store:   NewKVStore(),
		waiters: make(map[int]waiter),
	}
	go s.applyLoop(applyCh)
	return s
}

func (s *KVServer) applyLoop(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}

		// The leader's no-op entry has a nil Command, so the assertion fails
		// and we simply skip the store update.
		cmd, isCmd := msg.Command.(Command)
		if isCmd {
			s.store.Apply(cmd)
			fmt.Printf(">>> [node %d] APPLIED index=%d cmd=%+v\n", s.rf.Me(), msg.CommandIndex, cmd)
		}

		s.mu.Lock()
		if w, ok := s.waiters[msg.CommandIndex]; ok {
			// If the term differs, a different leader's entry took this
			// index and our proposal was overwritten.
			w.done <- (w.term == msg.CommandTerm) // buffered, never blocks
			delete(s.waiters, msg.CommandIndex)
		}
		s.mu.Unlock()
	}
}

// propose submits a command to Raft and blocks until it is applied, lost, or
// times out.
func (s *KVServer) propose(cmd Command) error {
	// Hold s.mu across Start + registering the waiter so the apply loop can
	// never apply this index before the waiter exists.
	s.mu.Lock()
	index, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		s.mu.Unlock()
		return ErrNotLeader
	}
	w := waiter{term: term, done: make(chan bool, 1)}
	s.waiters[index] = w
	s.mu.Unlock()

	select {
	case ok := <-w.done:
		if ok {
			return nil
		}
		return ErrNotLeader // our entry was overwritten: leadership changed
	case <-time.After(commitTimeout):
		s.mu.Lock()
		if cur, ok := s.waiters[index]; ok && cur.done == w.done {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
		return ErrTimeout
	}
}

func (s *KVServer) Put(key, value string) error {
	return s.propose(Command{Op: OpSet, Key: key, Value: value})
}

func (s *KVServer) Delete(key string) error {
	return s.propose(Command{Op: OpDel, Key: key})
}

// Get serves a read only if this node currently believes it's leader.
// NOTE: this is a dirty read - no ReadIndex confirmation yet, so during
// a partition a stale leader could serve an old value. Known gap.
func (s *KVServer) Get(key string) (string, bool, error) {
	if !s.rf.IsLeader() {
		return "", false, ErrNotLeader
	}
	value, ok := s.store.Get(key)
	return value, ok, nil
}

// Dump exposes the store's contents for debugging. It bypasses the leader
// check since it's an inspection hook, not a client read.
func (s *KVServer) Dump() map[string]string {
	return s.store.Dump()
}