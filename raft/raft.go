package raft

import (
	"context"
	"net"
	"sync"

	"golang.org/x/sync/errgroup"
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
	GetCurrentTerm(ctx context.Context) uint64
	SetCurrentTerm(ctx context.Context, term uint64)

	GetVotedFor(ctx context.Context) string
	SetVotedFor(ctx context.Context, candidateId string)

	GetLogEntries(ctx context.Context) []*LogEntry
	AppendLogEntries(ctx context.Context, l []*LogEntry)
}

// FSM TODO: implement
type FSM interface {
	Apply(ctx context.Context, l *LogEntry)
	LastApplied(ctx context.Context) *LogEntry
}

type Raft struct {
	mu *sync.RWMutex

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
	lastAppliedLogEntry := fsm.LastApplied(ctx)
	if lastAppliedLogEntry != nil {
		lastApplied = lastAppliedLogEntry.Index
	}

	commitIndex := uint64(0)
	logEntries := p.GetLogEntries(ctx)
	if len(logEntries) > 0 {
		commitIndex = logEntries[len(logEntries)-1].Index
	}

	return &Raft{
		mu: &sync.RWMutex{},

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

func (r *Raft) GetCurrentTerm(ctx context.Context) uint64 {
	return r.persistent.GetCurrentTerm(ctx)
}

func (r *Raft) SetCurrentTerm(ctx context.Context, term uint64) {
	r.persistent.SetCurrentTerm(ctx, term)
}

func (r *Raft) GetCurrentState(_ context.Context) State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentState
}

func (r *Raft) SetState(_ context.Context, state State) {
	r.mu.Lock()
	r.currentState = state
	r.mu.Unlock()
}

func (r *Raft) GetLogEntry(ctx context.Context, index uint64) *LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, index)
}

func (r *Raft) GetLastLogEntry(ctx context.Context) *LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, r.getCommitIndex(ctx))
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []*LogEntry, leaderCommit uint64) {
	if len(l) < 1 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: implement overwrite
	r.persistent.AppendLogEntries(ctx, l)
	r.appendLogEntries(ctx, l)

	if leaderCommit > r.getCommitIndex(ctx) {
		r.setCommitIndex(ctx, min(leaderCommit, r.log[len(r.log)-1].Index))
	}
}

func (r *Raft) Start(ctx context.Context) error {
	gr, ctx := errgroup.WithContext(ctx)

	gr.Go(func() error { return r.stateHandlerLoop(ctx) })
	gr.Go(func() error { return r.logApplierLoop(ctx) })

	return gr.Wait()
}

func (r *Raft) stateHandlerLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			switch r.GetCurrentState(ctx) {
			case Follower:
				r.followerLoop(ctx)
			case Candidate:
				r.candidateLoop(ctx)
			case Leader:
				r.leaderLoop(ctx)
			}
		}
	}
}

func (r *Raft) followerLoop(ctx context.Context) {
	for r.GetCurrentState(ctx) == Follower {
		// TODO: implement
	}
}

func (r *Raft) candidateLoop(ctx context.Context) {
	for r.GetCurrentState(ctx) == Candidate {
		// TODO: implement
	}
}

func (r *Raft) leaderLoop(ctx context.Context) {
	for r.GetCurrentState(ctx) == Leader {
		// TODO: implement
	}
}

func (r *Raft) logApplierLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			r.mu.Lock()
			if r.getCommitIndex(ctx) > r.getLastApplied(ctx) {
				r.applyLogEntries(ctx)
			}
			r.mu.Unlock()
		}
	}
}

func (r *Raft) getLogEntry(_ context.Context, index uint64) *LogEntry {
	if uint64(len(r.log)) < index {
		return nil
	}
	return r.log[index-1]
}

func (r *Raft) getCommitIndex(_ context.Context) uint64 {
	return r.commitIndex
}

func (r *Raft) setCommitIndex(_ context.Context, commitIndex uint64) {
	r.commitIndex = commitIndex
}

func (r *Raft) getLastApplied(_ context.Context) uint64 {
	return r.lastApplied
}

func (r *Raft) setLastApplied(_ context.Context, lastApplied uint64) {
	r.lastApplied = lastApplied
}

func (r *Raft) appendLogEntries(ctx context.Context, l []*LogEntry) {
	for i, newLogEntry := range l {
		currentLogEntry := r.getLogEntry(ctx, newLogEntry.Index)
		if currentLogEntry == nil {
			r.log = append(r.log, l[i:]...)
			return
		}

		if currentLogEntry.Term != newLogEntry.Term {
			r.log = append(r.log[:newLogEntry.Index-1], l[i:]...)
			return
		}
	}
}

func (r *Raft) applyLogEntries(ctx context.Context) {
	// TODO: implement
}
