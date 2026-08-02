package raft // all files with this are part of the package raft

import (
	// provides random numbers
	"sync" // learn about thread !!!!
	"time"
)

type State int // typedef essentially where we want to call integer as State now

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Raft struct {
	mu sync.Mutex

	currentTerm int
	votedFor    int

	state State

	peers []int // peer ids
	me    int   // self id

	electionResetEvent time.Time //election timing
}

func NewRaft(me int, peers []int) *Raft {
	rf := &Raft{
		me:          me,
		peers:       peers,
		currentTerm: 0,
		votedFor:    -1,
		state:       Follower,
	}

	return rf
}

// STATUS: pasted struct + NewRaft, haven't added runElectionTimer yet
