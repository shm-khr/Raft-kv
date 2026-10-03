package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// ApplyMsg is what Raft hands to the layer above (the KV server) once an
// entry is committed. CommandTerm lets the layer above detect when the
// entry that landed at an index is not the one it proposed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	applyCond *sync.Cond // signalled whenever commitIndex advances (or on Kill)

	me      int
	peerIds []int
	peers   map[int]*Raft

	dead     int32
	isolated int32 // simulated network partition: no RPCs in or out

	// persistent state (not actually persisted yet)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	state       State
	commitIndex int
	lastApplied int

	// leader-only
	nextIndex  map[int]int // next log index to send to each peer
	matchIndex map[int]int // highest log index known replicated on each peer

	electionResetEvent time.Time
	applyCh            chan ApplyMsg
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func NewRaft(me int, peerIds []int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:                 me,
		peerIds:            peerIds,
		peers:              make(map[int]*Raft),
		currentTerm:        0,
		votedFor:           -1,
		state:              Follower,
		applyCh:            applyCh,
		log:                []LogEntry{{Term: 0, Command: nil}}, // sentinel at index 0
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
		electionResetEvent: time.Now(), // otherwise every node times out instantly at startup
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	go rf.applier()

	return rf
}

func (rf *Raft) AddPeer(id int, peer *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers[id] = peer
}

func (rf *Raft) getPeer(id int) *Raft {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers[id]
}

// becomeFollower must be called with rf.mu held.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

// ---------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// RunElectionTimer runs for the lifetime of the node. It used to return as
// soon as the node became leader, which meant a leader that later stepped
// down would never time out again. Now it just skips leaders.
func (rf *Raft) RunElectionTimer() {
	timeout := randomElectionTimeout()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if rf.isDead() {
			return
		}

		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionResetEvent) >= timeout {
			rf.startElection()
			timeout = randomElectionTimeout()
		}
		rf.mu.Unlock()
	}
}

// startElection must be called with rf.mu held.
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	savedTerm := rf.currentTerm
	rf.electionResetEvent = time.Now()
	rf.votedFor = rf.me

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	fmt.Printf("[node %d] became Candidate, term %d\n", rf.me, savedTerm)

	votesReceived := 1 // self-vote

	for _, peerId := range rf.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:         savedTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			if !rf.sendRequestVote(peerId, args, &reply) {
				return // peer unreachable
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if rf.state != Candidate || rf.currentTerm != savedTerm {
				return // stale response, situation has moved on
			}

			if reply.VoteGranted {
				votesReceived++
				if votesReceived*2 > len(rf.peerIds)+1 { // majority
					rf.becomeLeader()
				}
			}
		}(peerId)
	}
}

// becomeLeader must be called with rf.mu held.
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	fmt.Printf("[node %d] became LEADER, term %d\n", rf.me, rf.currentTerm)

	for _, peerId := range rf.peerIds {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = 0
	}

	// Append a no-op in the new term. A leader can only commit entries from
	// its own term directly, so without this, entries left over from earlier
	// terms stay uncommitted until a client happens to send a new command.
	rf.log = append(rf.log, LogEntry{Command: nil, Term: rf.currentTerm})

	rf.startLeader()
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.isDead() || rf.isIsolated() {
		return false // simulates the node being unreachable
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return true
	}

	// Election restriction (Raft paper 5.4.1): only vote for a candidate whose
	// log is at least as up-to-date as ours.
	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionResetEvent = time.Now()
	}
	return true
}

// sendRequestVote returns false if the peer could not be reached.
func (rf *Raft) sendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	peer := rf.getPeer(peerId)
	if peer == nil || rf.isIsolated() {
		return false
	}
	return peer.RequestVote(args, reply)
}

// ---------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------

// Start proposes a command. Only the leader accepts it.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isDead() || rf.state != Leader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index = len(rf.log) - 1
	term = rf.currentTerm

	fmt.Printf("[node %d] Start(%v) -> log index %d, term %d\n", rf.me, command, index, term)

	return index, term, true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.isDead() || rf.isIsolated() {
		return false // simulates the node being unreachable
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return true
	}

	// A valid AppendEntries for our term: the sender is the leader.
	rf.state = Follower
	rf.electionResetEvent = time.Now()

	if args.PrevLogIndex >= len(rf.log) {
		return true // my log is shorter than the leader expects
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return true // entry at that index is from a different (stale) term
	}

	// Consistency check passed: reconcile with the leader's entries.
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIdx := insertIndex + i
		if logIdx < len(rf.log) {
			if rf.log[logIdx].Term != entry.Term {
				// conflict: truncate from here, then append the rest
				rf.log = rf.log[:logIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			// same term at same index: already correct
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > rf.commitIndex { // never move commitIndex backwards
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
	return true
}

// sendAppendEntries returns false if the peer could not be reached.
func (rf *Raft) sendAppendEntries(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	peer := rf.getPeer(peerId)
	if peer == nil || rf.isIsolated() {
		return false
	}
	return peer.AppendEntries(args, reply)
}

// startLeader must be called with rf.mu held. It launches the heartbeat /
// replication loop for the current leadership term.
func (rf *Raft) startLeader() {
	leaderTerm := rf.currentTerm

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond) // must be << election timeout (150-300ms)
		defer ticker.Stop()

		for {
			if rf.isDead() {
				return
			}

			rf.mu.Lock()
			// the term check stops an old loop from living on if this node
			// steps down and is re-elected quickly
			if rf.state != Leader || rf.currentTerm != leaderTerm {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			for _, peerId := range rf.peerIds {
				go rf.replicateTo(peerId, leaderTerm)
			}

			<-ticker.C
		}
	}()
}

func (rf *Raft) replicateTo(peerId int, term int) {
	if rf.isDead() {
		return
	}

	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	next := rf.nextIndex[peerId]
	prevLogIndex := next - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	var entries []LogEntry
	if len(rf.log) > next {
		entries = append([]LogEntry{}, rf.log[next:]...)
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peerId, args, &reply) {
		return // peer unreachable: don't touch nextIndex
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.electionResetEvent = time.Now() // our timer is stale after a long stint as leader
		return
	}

	if rf.state != Leader || rf.currentTerm != term {
		return // stale reply, situation moved on
	}

	if reply.Success {
		match := prevLogIndex + len(entries)
		if match > rf.matchIndex[peerId] {
			rf.matchIndex[peerId] = match
		}
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		rf.advanceCommitIndex()
	} else if rf.nextIndex[peerId] > 1 {
		// consistency check failed on the follower: back off and retry
		rf.nextIndex[peerId]--
	}
}

// advanceCommitIndex must be called with rf.mu held. It finds the highest N
// replicated on a majority whose entry is from the current term (Raft only
// commits entries from its own term by counting replicas).
func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1 // leader itself has it
		for _, peerId := range rf.peerIds {
			if rf.matchIndex[peerId] >= n {
				count++
			}
		}
		if count*2 > len(rf.peerIds)+1 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// ---------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------

// applier is the only goroutine that sends on applyCh. It delivers entries
// in order using lastApplied, and sends without holding rf.mu so a slow
// consumer can never block the Raft state machine.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.isDead() {
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}

		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  entry.Term,
			})
		}

		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
		rf.mu.Lock()
	}
}

// ---------------------------------------------------------------------
// Lifecycle and inspection helpers
// ---------------------------------------------------------------------

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.state = Follower // dead nodes shouldn't claim to be leader
	fmt.Printf("[node %d] KILLED\n", rf.me)
	rf.applyCond.Broadcast() // wake the applier so it can exit
	rf.mu.Unlock()
}

func (rf *Raft) isDead() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Disconnect simulates a network partition around this node: it can neither
// send nor receive RPCs, but it keeps running (unlike Kill). Reconnect heals it.
func (rf *Raft) Disconnect() {
	atomic.StoreInt32(&rf.isolated, 1)
	fmt.Printf("[node %d] DISCONNECTED\n", rf.me)
}

func (rf *Raft) Reconnect() {
	atomic.StoreInt32(&rf.isolated, 0)
	fmt.Printf("[node %d] RECONNECTED\n", rf.me)
}

func (rf *Raft) isIsolated() bool {
	return atomic.LoadInt32(&rf.isolated) == 1
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) IsLeader() bool {
	_, isLeader := rf.GetState()
	return isLeader
}

func (rf *Raft) GetLogState() (log []LogEntry, commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logCopy := append([]LogEntry{}, rf.log...)
	return logCopy, rf.commitIndex
}