package raft

import (
	"context"
	"net"
	"sync"
)

type State int8

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index      uint64
	Term       uint64
	Data       []byte
	AppendedAt uint64 // unix nano
}

// Persistent TODO: implement
type Persistent interface {
	GetCurrentTerm(ctx context.Context) (uint64, error)
	SetCurrentTerm(ctx context.Context, term uint64) error

	GetVotedFor(ctx context.Context) (string, error)
	SetVotedFor(ctx context.Context, candidateId string) error

	GetLogEntries(ctx context.Context) ([]*LogEntry, error)
	AppendLogEntries(ctx context.Context, l []*LogEntry) error
}

// FSM TODO: implement
type FSM interface {
	Apply(ctx context.Context, l *LogEntry) error
	LastApplied(ctx context.Context) (*LogEntry, error)
}

type Raft struct {
	mu *sync.Mutex

	currentState State
	commitIndex  uint64
	lastApplied  uint64
	log          []*LogEntry

	nodesIPs   []net.Addr
	persistent Persistent
	fsm        FSM

	// Only for leader
	nextIndex  []uint64
	matchIndex []uint64
}

func NewRaft(ctx context.Context, fsm FSM, p Persistent, nodesIPs []net.Addr) (*Raft, error) {
	lastApplied := uint64(0)
	lastAppliedLogEntry, err := fsm.LastApplied(ctx)
	if err != nil {
		return nil, err
	}
	if lastAppliedLogEntry != nil {
		lastApplied = lastAppliedLogEntry.Index
	}

	commitIndex := uint64(0)
	logEntries, err := p.GetLogEntries(ctx)
	if err != nil {
		return nil, err
	}
	if len(logEntries) > 0 {
		commitIndex = logEntries[len(logEntries)-1].Index
	}

	return &Raft{
		mu: &sync.Mutex{},

		currentState: Follower,
		commitIndex:  commitIndex,
		lastApplied:  lastApplied,
		log:          logEntries,

		nodesIPs:   nodesIPs,
		persistent: p,
		fsm:        fsm,

		nextIndex:  nil,
		matchIndex: nil,
	}, nil
}

func (r *Raft) CurrentTerm(ctx context.Context) (uint64, error) {
	return r.persistent.GetCurrentTerm(ctx)
}

func (r *Raft) LogEntry(_ context.Context, index uint64) *LogEntry {
	if uint64(len(r.log)) < index {
		return nil
	}
	return r.log[index-1]
}

func (r *Raft) LastLogEntry(_ context.Context) *LogEntry {
	if len(r.log) < 1 {
		return nil
	}
	return r.log[r.commitIndex]
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []*LogEntry, leaderCommit uint64) error {
	if len(l) < 1 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: implement overwrite
	err := r.persistent.AppendLogEntries(ctx, l)
	if err != nil {
		return err
	}

	err = r.appendLogEntries(ctx, l)
	if err != nil {
		return err
	}

	if leaderCommit > r.commitIndex {
		r.commitIndex = min(leaderCommit, r.log[len(r.log)-1].Index)
	}

	return nil
}

func (r *Raft) appendLogEntries(ctx context.Context, l []*LogEntry) error {
	for i, newLogEntry := range l {
		currentLogEntry := r.LogEntry(ctx, newLogEntry.Index)
		if currentLogEntry == nil {
			r.log = append(r.log, l[i:]...)
			return nil
		}

		if currentLogEntry.Term != newLogEntry.Term {
			r.log = append(r.log[:newLogEntry.Index-1], l[i:]...)
			return nil
		}
	}

	return nil
}
