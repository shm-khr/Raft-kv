package kv

import (
	"sync"
	"time"
)

const clerkTimeout = 5 * time.Second

// Clerk is a client that doesn't know which node is leader. It caches a
// hint and retries against the other servers on failure, round-robin.
type Clerk struct {
	mu         sync.Mutex
	servers    []*KVServer
	leaderHint int
}

func NewClerk(servers []*KVServer) *Clerk {
	return &Clerk{servers: servers}
}

// do keeps calling op, rotating through servers, until it succeeds or the
// overall deadline passes.
func (ck *Clerk) do(op func(s *KVServer) error) error {
	deadline := time.Now().Add(clerkTimeout)

	for time.Now().Before(deadline) {
		ck.mu.Lock()
		hint := ck.leaderHint
		ck.mu.Unlock()

		if err := op(ck.servers[hint]); err == nil {
			return nil
		}

		// wrong node, dead node, or timed out: try the next one
		ck.mu.Lock()
		ck.leaderHint = (hint + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	return ErrTimeout
}

func (ck *Clerk) Get(key string) (string, bool, error) {
	var value string
	var found bool
	err := ck.do(func(s *KVServer) error {
		var err error
		value, found, err = s.Get(key)
		return err
	})
	return value, found, err
}

func (ck *Clerk) Put(key, value string) error {
	return ck.do(func(s *KVServer) error { return s.Put(key, value) })
}

func (ck *Clerk) Delete(key string) error {
	return ck.do(func(s *KVServer) error { return s.Delete(key) })
}