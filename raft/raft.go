package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"raft/config"
	"raft/protos"
)

type State int8

const (
	Follower State = iota
	Candidate
	Leader

	minTimeout = 150 * time.Millisecond
	maxTimeout = 300 * time.Millisecond
)

type LogEntry struct {
	Index      int
	Term       int
	Data       []byte
	AppendedAt uint64 // unix nano
}

// Persistent TODO: implement
type Persistent interface {
	GetCurrentTerm(ctx context.Context) int
	SetCurrentTerm(ctx context.Context, term int)

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
	commitIndex  int
	lastApplied  int
	log          []*LogEntry
	heartbeatCh  chan struct{}

	cfg                 *config.Raft
	clusterNodesClients map[string]protos.RaftClient
	persistent          Persistent
	fsm                 FSM

	// Only for leader
	nextIndex  []int
	matchIndex []int
}

func NewRaft(ctx context.Context, cfg *config.Raft, fsm FSM, p Persistent) (*Raft, error) {
	lastApplied := 0
	lastAppliedLogEntry := fsm.LastApplied(ctx)
	if lastAppliedLogEntry != nil {
		lastApplied = lastAppliedLogEntry.Index
	}

	commitIndex := 0
	logEntries := p.GetLogEntries(ctx)
	if len(logEntries) > 0 {
		commitIndex = logEntries[len(logEntries)-1].Index
	}

	clusterNodesClients := make(map[string]protos.RaftClient, len(cfg.ClusterNodesAddr))
	for _, clusterNodeAddr := range cfg.ClusterNodesAddr {
		client, err := grpc.NewClient(clusterNodeAddr)
		if err != nil {
			return nil, err
		}
		clusterNodesClients[clusterNodeAddr] = protos.NewRaftClient(client)
	}

	return &Raft{
		mu: &sync.RWMutex{},

		currentState: Follower,
		commitIndex:  commitIndex,
		lastApplied:  lastApplied,
		log:          logEntries,
		heartbeatCh:  make(chan struct{}, 1),

		cfg:                 cfg,
		clusterNodesClients: clusterNodesClients,
		persistent:          p,
		fsm:                 fsm,

		nextIndex:  nil,
		matchIndex: nil,
	}, nil
}

func (r *Raft) GetCurrentTerm(ctx context.Context) int {
	return r.persistent.GetCurrentTerm(ctx)
}

func (r *Raft) SetCurrentTerm(ctx context.Context, term int) {
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

func (r *Raft) GetLogEntry(ctx context.Context, index int) *LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, index)
}

func (r *Raft) GetLastLogEntry(ctx context.Context) *LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, r.getCommitIndex(ctx))
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []*LogEntry, leaderCommit int) {
	defer r.nonblockingHeartbeat(ctx)

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
	defer r.Shutdown()

	gr, ctx := errgroup.WithContext(ctx)

	gr.Go(func() error { return r.stateHandlerLoop(ctx) })
	gr.Go(func() error { return r.logApplierLoop(ctx) })

	return gr.Wait()
}

func (r *Raft) Shutdown() {
	// TODO: implement
}

func (r *Raft) stateHandlerLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			switch state := r.GetCurrentState(ctx); state {
			case Follower:
				r.followerLoop(ctx)
			case Candidate:
				r.candidateLoop(ctx)
			case Leader:
				r.leaderLoop(ctx)
			default:
				panic(fmt.Sprintf("undefined node state: %d", state))
			}
		}
	}
}

func (r *Raft) followerLoop(ctx context.Context) {
	heartbeatTimeout := randomTimeout()

	for r.GetCurrentState(ctx) == Follower {
		select {
		case <-r.heartbeatCh:
			heartbeatTimeout = randomTimeout()
		case <-heartbeatTimeout:
			r.SetState(ctx, Candidate)
		default:
		}
	}
}

func (r *Raft) candidateLoop(ctx context.Context) {
	currentTerm := r.GetCurrentTerm(ctx) + 1
	voted, votesForElect := &atomic.Int64{}, r.quorumSize(ctx)
	electionTimeout := randomTimeout()
	r.sendVoteForMe(ctx, voted)

	for r.GetCurrentState(ctx) == Candidate {
		select {
		case <-electionTimeout:
			currentTerm++
			electionTimeout = randomTimeout()
			voted = &atomic.Int64{}
			r.sendVoteForMe(ctx, voted)
		default:
			if voted.Load()+1 >= int64(votesForElect) {
				r.SetCurrentTerm(ctx, currentTerm)
				r.SetState(ctx, Leader)
			}
		}
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

func (r *Raft) getLogEntry(_ context.Context, index int) *LogEntry {
	if len(r.log) < index {
		return nil
	}
	return r.log[index-1]
}

func (r *Raft) getCommitIndex(_ context.Context) int {
	return r.commitIndex
}

func (r *Raft) setCommitIndex(_ context.Context, commitIndex int) {
	r.commitIndex = commitIndex
}

func (r *Raft) getLastApplied(_ context.Context) int {
	return r.lastApplied
}

func (r *Raft) setLastApplied(_ context.Context, lastApplied int) {
	r.lastApplied = lastApplied
}

func (r *Raft) nonblockingHeartbeat(_ context.Context) {
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}
}

func (r *Raft) quorumSize(_ context.Context) int {
	return len(r.clusterNodesClients)/2 + 1
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

func (r *Raft) applyLogEntries(_ context.Context) {
	// TODO: implement
}

func (r *Raft) sendVoteForMe(ctx context.Context, voted *atomic.Int64) {
	for _, nodeClient := range r.clusterNodesClients {
		go func() {
			resp, err := nodeClient.RequestVote(ctx, &protos.RequestVoteReq{
				Term:         0,
				CandidateId:  "",
				LastLogIndex: 0,
				LastLogTerm:  0,
			})
			if err == nil && resp != nil {
				if resp.GetVoteGranted() {
					voted.Add(1)
				}
				if resp.GetTerm() > int64(r.GetCurrentTerm(ctx)) {
					r.SetCurrentTerm(ctx, int(resp.GetTerm()))
					r.SetState(ctx, Follower)
				}
			}
		}()
	}
}

func randomTimeout() <-chan time.Time {
	return time.After(time.Duration(rand.Int64N(int64(maxTimeout-minTimeout))) + minTimeout)
}
