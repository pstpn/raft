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
	IncCurrentTerm(ctx context.Context) int
	SetCurrentTerm(ctx context.Context, term int)

	GetVotedFor(ctx context.Context) (string, bool)
	SetVotedFor(ctx context.Context, candidateId string)

	GetLogEntries(ctx context.Context) []LogEntry
	AppendLogEntries(ctx context.Context, l []LogEntry)
}

// FSM TODO: implement
type FSM interface {
	Apply(ctx context.Context, l LogEntry)
	LastApplied(ctx context.Context) (LogEntry, bool)
}

type Raft struct {
	mu *sync.RWMutex

	currentState State
	commitIndex  int
	lastApplied  int
	log          []LogEntry
	heartbeatCh  chan struct{}

	cfg                 *config.Raft
	clusterNodesClients map[string]protos.RaftClient
	persistent          Persistent
	fsm                 FSM

	// Only for leader
	nodesNextIndex  []int
	nodesMatchIndex []int
}

func NewRaft(ctx context.Context, cfg *config.Raft, fsm FSM, p Persistent) (*Raft, error) {
	lastApplied := 0
	lastAppliedLogEntry, exists := fsm.LastApplied(ctx)
	if exists {
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

		nodesNextIndex:  nil,
		nodesMatchIndex: nil,
	}, nil
}

func (r *Raft) GetCurrentTerm(ctx context.Context) int {
	return r.persistent.GetCurrentTerm(ctx)
}

func (r *Raft) SetCurrentTerm(ctx context.Context, term int) {
	r.persistent.SetCurrentTerm(ctx, term)
}

func (r *Raft) SetVotedFor(ctx context.Context, candidateId string) {
	r.persistent.SetVotedFor(ctx, candidateId)
}

func (r *Raft) GetVotedFor(ctx context.Context) (string, bool) {
	return r.persistent.GetVotedFor(ctx)
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

func (r *Raft) GetLogEntry(ctx context.Context, index int) (LogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, index)
}

func (r *Raft) GetLastLogEntry(ctx context.Context) (LogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLogEntry(ctx, len(r.log))
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []LogEntry, leaderCommit int) {
	defer r.nonblockingHeartbeat(ctx)

	if len(l) < 1 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: implement overwrite
	r.persistent.AppendLogEntries(ctx, l)
	r.appendLogEntries(ctx, l)

	if leaderCommit > r.commitIndex {
		lastLogIndex := 0
		if len(r.log) > 1 {
			lastLogIndex = r.log[len(r.log)-1].Index
		}
		r.commitIndex = min(leaderCommit, lastLogIndex)
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
	currentTerm := r.persistent.IncCurrentTerm(ctx)
	voted, votesForElect := &atomic.Int64{}, r.quorumSize(ctx)
	electionTimeout := randomTimeout()
	r.callVoteForMeRPC(ctx, voted, currentTerm)

	for r.GetCurrentState(ctx) == Candidate {
		select {
		case <-electionTimeout:
			currentTerm++
			electionTimeout = randomTimeout()
			voted = &atomic.Int64{}
			r.callVoteForMeRPC(ctx, voted, currentTerm)
		default:
			if voted.Load()+1 >= int64(votesForElect) {
				r.persistent.SetCurrentTerm(ctx, currentTerm)
				r.SetState(ctx, Leader)
			}
		}
	}
}

func (r *Raft) leaderLoop(ctx context.Context) {
	r.initLeadersIndexes(ctx)
	r.callHeartbeatRPC(ctx)

	replicateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.replicateLeadersLogLoop(replicateCtx)

	for r.GetCurrentState(ctx) == Leader {
		switch {
		default:
			r.callHeartbeatRPC(ctx)
		}
	}
}

func (r *Raft) replicateLeadersLogLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			r.mu.RLock()
			lastLog, exists := r.GetLastLogEntry(ctx)
			if !exists {
				continue
			}
			lastLogIndex := lastLog.Index
			nextCommitIndex, quorumSize := r.commitIndex+1, r.quorumSize(ctx)
			r.mu.RUnlock()

			for i := range r.nodesNextIndex {
				r.mu.RLock()
				nodeNextIndex, nodeMatchIndex := r.nodesNextIndex[i], r.nodesMatchIndex[i]
				r.mu.RUnlock()

				if lastLogIndex >= nodeNextIndex {
					r.mu.RLock()
					logEntries := r.log[nodeNextIndex-1 : lastLogIndex-1]
					prevLogEntry := r.log[lastLogIndex-1]
					nodeClient := r.clusterNodesClients[r.cfg.ClusterNodesAddr[i]]
					r.mu.RUnlock()

					go func() {
						ok, err := r.callAppendEntriesRPC(ctx, logEntries, prevLogEntry, nodeClient)
						if err == nil && !ok {
							r.mu.Lock()
							r.nodesNextIndex[i]--
							r.mu.Unlock()
						} else if ok {
							r.mu.Lock()
							r.nodesNextIndex[i] = lastLogIndex
							r.nodesMatchIndex[i] = lastLogIndex - 1
							r.mu.Unlock()
						}
					}()
				}

				if nodeMatchIndex >= nextCommitIndex {
					quorumSize--
				}
			}

			r.mu.RLock()
			logTerm := r.log[nextCommitIndex-1].Term
			r.mu.RUnlock()
			if quorumSize < 1 && logTerm == r.persistent.GetCurrentTerm(ctx) {
				r.mu.Lock()
				r.commitIndex = nextCommitIndex
				r.mu.Unlock()
			}
		}
	}
}

func (r *Raft) logApplierLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			r.mu.RLock()
			applyNeeded := r.commitIndex > r.lastApplied
			r.mu.RUnlock()

			if applyNeeded {
				r.mu.Lock()
				r.applyLogEntries(ctx)
				r.mu.Unlock()
			}
		}
	}
}

func (r *Raft) getLogEntry(_ context.Context, index int) (LogEntry, bool) {
	if len(r.log) < index {
		return LogEntry{}, false
	}
	return r.log[index-1], true
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

func (r *Raft) appendLogEntries(ctx context.Context, l []LogEntry) {
	for i, newLogEntry := range l {
		currentLogEntry, exists := r.getLogEntry(ctx, newLogEntry.Index)
		if !exists {
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

func (r *Raft) callVoteForMeRPC(ctx context.Context, voted *atomic.Int64, currentTerm int) {
	lastLogEntry, exists := r.GetLastLogEntry(ctx)
	commitIndex, logTerm := int64(0), int64(0)
	if exists {
		r.mu.RLock()
		commitIndex, logTerm = int64(lastLogEntry.Index), int64(lastLogEntry.Term)
		r.mu.RUnlock()
	}

	for _, nodeClient := range r.clusterNodesClients {
		go func() {
			resp, err := nodeClient.RequestVote(ctx, &protos.RequestVoteReq{
				Term:         int64(currentTerm),
				CandidateId:  r.cfg.NodeAddr,
				LastLogIndex: commitIndex,
				LastLogTerm:  logTerm,
			})
			if err == nil && resp != nil {
				if resp.GetVoteGranted() {
					voted.Add(1)
				}
				if resp.GetTerm() > int64(r.persistent.GetCurrentTerm(ctx)) {
					r.persistent.SetCurrentTerm(ctx, int(resp.GetTerm()))
					r.SetState(ctx, Follower)
				}
			}
		}()
	}
}

func (r *Raft) callHeartbeatRPC(ctx context.Context) {
	r.mu.RLock()
	prevLogEntry, exists := r.GetLogEntry(ctx, len(r.log)-1)
	if !exists {
		r.mu.RUnlock()
		return
	}
	r.mu.RUnlock()

	for _, nodeClient := range r.clusterNodesClients {
		go func() { _, _ = r.callAppendEntriesRPC(ctx, nil, prevLogEntry, nodeClient) }()
	}
}

func (r *Raft) callAppendEntriesRPC(
	ctx context.Context,
	entries []LogEntry,
	prevLogEntry LogEntry,
	nodeClient protos.RaftClient,
) (bool, error) {
	currentTerm := r.persistent.GetCurrentTerm(ctx)
	r.mu.RLock()
	commitIndex := int64(r.commitIndex)
	r.mu.RUnlock()

	var appendEntries []*protos.Entry
	if len(entries) > 0 {
		appendEntries = logEntriesToGRPC(entries)
	}

	resp, err := nodeClient.AppendEntries(ctx, &protos.AppendEntriesReq{
		Entries:      appendEntries,
		Term:         int64(currentTerm),
		LeaderId:     r.cfg.NodeAddr,
		PrevLogIndex: int64(prevLogEntry.Index),
		PrevLogTerm:  int64(prevLogEntry.Term),
		LeaderCommit: commitIndex,
	})
	if err != nil || resp == nil {
		return false, fmt.Errorf("call append entries rpc: %w", err)
	}
	if resp.GetTerm() > int64(r.persistent.GetCurrentTerm(ctx)) {
		r.persistent.SetCurrentTerm(ctx, int(resp.GetTerm()))
		r.SetState(ctx, Follower)
		return false, ErrCurrentTermIsLower
	}

	return resp.Success, nil
}

func (r *Raft) initLeadersIndexes(ctx context.Context) {
	lastLogIndex := 0
	r.mu.RLock()
	lastLogEntry, exists := r.getLogEntry(ctx, len(r.log))
	r.mu.RUnlock()
	if exists {
		lastLogIndex = lastLogEntry.Index
	}

	r.mu.Lock()
	r.nodesNextIndex, r.nodesMatchIndex = make([]int, len(r.clusterNodesClients)), make([]int, len(r.clusterNodesClients))
	for i := range r.nodesNextIndex {
		r.nodesNextIndex[i] = lastLogIndex + 1
	}
	r.mu.Unlock()
}

func randomTimeout() <-chan time.Time {
	return time.After(time.Duration(rand.Int64N(int64(maxTimeout-minTimeout))) + minTimeout)
}

func logEntriesToGRPC(entries []LogEntry) []*protos.Entry {
	grpcEntries := make([]*protos.Entry, len(entries))
	for i, entry := range entries {
		grpcEntries[i] = &protos.Entry{
			Data:       entry.Data,
			Term:       int64(entry.Term),
			Index:      int64(entry.Index),
			AppendedAt: entry.AppendedAt,
		}
	}
	return grpcEntries
}
