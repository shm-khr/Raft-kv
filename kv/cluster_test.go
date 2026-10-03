package kv_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raft-kv/kv"
	"raft-kv/raft"
)

// ---------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------

type cluster struct {
	t       *testing.T
	nodes   []*raft.Raft
	servers []*kv.KVServer
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		nodes:   make([]*raft.Raft, n),
		servers: make([]*kv.KVServer, n),
	}

	for i := 0; i < n; i++ {
		var peers []int
		for p := 0; p < n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 100)
		c.nodes[i] = raft.NewRaft(i, peers, applyCh)
		c.servers[i] = kv.NewKVServer(c.nodes[i], applyCh)
	}
	for i, a := range c.nodes {
		for j, b := range c.nodes {
			if i != j {
				a.AddPeer(j, b)
			}
		}
	}
	for _, nd := range c.nodes {
		go nd.RunElectionTimer()
	}

	// Stops every node's background goroutines when the test ends.
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Kill()
		}
	})
	return c
}

// eventually polls cond until it is true or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

func (c *cluster) except(exclude []int) []int {
	skip := make(map[int]bool)
	for _, e := range exclude {
		skip[e] = true
	}
	var ids []int
	for i := range c.nodes {
		if !skip[i] {
			ids = append(ids, i)
		}
	}
	return ids
}

// leaderAmong returns the leader with the highest term among ids, or -1.
// (A partitioned stale leader has an older term, so the highest term wins.)
func (c *cluster) leaderAmong(ids []int) int {
	best, bestTerm := -1, -1
	for _, i := range ids {
		term, isLeader := c.nodes[i].GetState()
		if isLeader && term > bestTerm {
			best, bestTerm = i, term
		}
	}
	return best
}

// waitLeader blocks until a leader exists among the nodes not in exclude.
func (c *cluster) waitLeader(exclude ...int) int {
	c.t.Helper()
	ids := c.except(exclude)
	leader := -1
	eventually(c.t, 5*time.Second, "a leader to be elected", func() bool {
		leader = c.leaderAmong(ids)
		return leader >= 0
	})
	return leader
}

func (c *cluster) has(i int, key, want string) bool {
	v, ok := c.servers[i].Dump()[key]
	return ok && v == want
}

func (c *cluster) absent(i int, key string) bool {
	_, ok := c.servers[i].Dump()[key]
	return !ok
}

// waitAll waits until every node in ids stores key=want.
func (c *cluster) waitAll(ids []int, key, want string) {
	c.t.Helper()
	eventually(c.t, 5*time.Second, fmt.Sprintf("nodes %v to store %s=%s", ids, key, want), func() bool {
		for _, i := range ids {
			if !c.has(i, key, want) {
				return false
			}
		}
		return true
	})
}

// waitSameLogs waits until every node in ids has an identical committed log prefix.
func (c *cluster) waitSameLogs(ids []int) {
	c.t.Helper()
	eventually(c.t, 5*time.Second, fmt.Sprintf("nodes %v to agree on the committed log", ids), func() bool {
		var first string
		for n, i := range ids {
			log, commit := c.nodes[i].GetLogState()
			repr := fmt.Sprintf("commit=%d %v", commit, log[:commit+1])
			if n == 0 {
				first = repr
			} else if repr != first {
				return false
			}
		}
		return true
	})
}

func mustPut(t *testing.T, ck *kv.Clerk, key, value string) {
	t.Helper()
	if err := ck.Put(key, value); err != nil {
		t.Fatalf("Put(%s, %s) failed: %v", key, value, err)
	}
}

func mustGet(t *testing.T, ck *kv.Clerk, key, want string) {
	t.Helper()
	got, ok, err := ck.Get(key)
	if err != nil || !ok || got != want {
		t.Fatalf("Get(%s) = %q, ok=%v, err=%v; want %q", key, got, ok, err, want)
	}
}

// ---------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------

func TestElection(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()

	// Watch for a while: no term may ever have two leaders.
	for i := 0; i < 30; i++ {
		leadersPerTerm := make(map[int]int)
		for _, nd := range c.nodes {
			if term, isLeader := nd.GetState(); isLeader {
				leadersPerTerm[term]++
			}
		}
		for term, count := range leadersPerTerm {
			if count > 1 {
				t.Fatalf("term %d has %d leaders", term, count)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.waitLeader() // and it is still stable
}

func TestPutReplicatesToAll(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	mustPut(t, ck, "a", "1")
	mustGet(t, ck, "a", "1")
	c.waitAll([]int{0, 1, 2}, "a", "1")
	c.waitSameLogs([]int{0, 1, 2})
}

func TestDelete(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	mustPut(t, ck, "a", "1")
	c.waitAll([]int{0, 1, 2}, "a", "1")

	if err := ck.Delete("a"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, ok, err := ck.Get("a"); err != nil || ok {
		t.Fatalf("Get(a) after delete: ok=%v err=%v; want not found", ok, err)
	}
	eventually(t, 5*time.Second, "every node to drop key a", func() bool {
		return c.absent(0, "a") && c.absent(1, "a") && c.absent(2, "a")
	})
}

func TestWritesApplyInOrder(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	for i := 1; i <= 20; i++ {
		mustPut(t, ck, "k", fmt.Sprint(i))
	}
	c.waitAll([]int{0, 1, 2}, "k", "20")
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	mustPut(t, ck, "x", "1")
	c.waitAll([]int{0, 1, 2}, "x", "1")

	old := c.waitLeader()
	c.nodes[old].Kill()
	survivors := c.except([]int{old})

	c.waitLeader(old) // a new leader emerges among the survivors
	mustPut(t, ck, "y", "2")
	mustGet(t, ck, "x", "1") // data written before the crash survives
	mustGet(t, ck, "y", "2")

	c.waitAll(survivors, "y", "2")
	c.waitSameLogs(survivors)
}

func TestIsolatedLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	mustPut(t, ck, "a", "1")
	c.waitAll([]int{0, 1, 2}, "a", "1")

	old := c.waitLeader()
	c.nodes[old].Disconnect()

	// The cut-off leader still believes it is leader and accepts the write,
	// but it can't reach a majority, so the write must never commit.
	if err := c.servers[old].Put("lost", "v"); err == nil {
		t.Fatal("a write on a partitioned leader must not succeed")
	}

	// Meanwhile the majority elects a new leader and makes progress.
	newLeader := c.waitLeader(old)
	if err := c.servers[newLeader].Put("b", "2"); err != nil {
		t.Fatalf("new leader could not commit: %v", err)
	}

	// Heal the partition: the old leader must step down, adopt the new log,
	// and its uncommitted "lost" entry must be discarded everywhere.
	c.nodes[old].Reconnect()
	eventually(t, 5*time.Second, "old leader to step down", func() bool {
		return !c.nodes[old].IsLeader()
	})
	c.waitAll([]int{0, 1, 2}, "b", "2")
	c.waitSameLogs([]int{0, 1, 2})
	for i := range c.nodes {
		if !c.absent(i, "lost") {
			t.Fatalf("node %d applied an entry that was never committed", i)
		}
	}
}

func TestNoQuorumThenRecovery(t *testing.T) {
	c := newCluster(t, 3)
	ck := kv.NewClerk(c.servers)

	mustPut(t, ck, "a", "1")
	leader := c.waitLeader()

	for i := range c.nodes {
		if i != leader {
			c.nodes[i].Disconnect()
		}
	}
	if err := c.servers[leader].Put("stuck", "v"); err == nil {
		t.Fatal("a write without a majority must not succeed")
	}

	for i := range c.nodes {
		if i != leader {
			c.nodes[i].Reconnect()
		}
	}
	// Followers bumped their terms while isolated, so the cluster may need a
	// fresh election, but it must recover and accept writes again.
	mustPut(t, ck, "after", "ok")
	c.waitAll([]int{0, 1, 2}, "after", "ok")
	c.waitAll([]int{0, 1, 2}, "a", "1")
}

func TestConcurrentClients(t *testing.T) {
	c := newCluster(t, 3)

	const clients, perClient = 5, 10
	var wg sync.WaitGroup
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			ck := kv.NewClerk(c.servers)
			for i := 0; i < perClient; i++ {
				key, val := fmt.Sprintf("c%d-k%d", cl, i), fmt.Sprintf("v%d", i)
				if err := ck.Put(key, val); err != nil {
					t.Errorf("client %d: Put(%s) failed: %v", cl, key, err)
					return
				}
			}
		}(cl)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	for cl := 0; cl < clients; cl++ {
		for i := 0; i < perClient; i++ {
			c.waitAll([]int{0, 1, 2}, fmt.Sprintf("c%d-k%d", cl, i), fmt.Sprintf("v%d", i))
		}
	}
	c.waitSameLogs([]int{0, 1, 2})
}