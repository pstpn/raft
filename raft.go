package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"raft/protos"
)

type State int8

const (
	Follower State = iota
	Candidate
	Leader

	minTimeout = 150 * time.Millisecond
	maxTimeout = 300 * time.Millisecond

	heartbeatInterval = 50 * time.Millisecond
	replicateInterval = 1 * time.Millisecond
	applyInterval     = 1 * time.Millisecond
)

type LogEntry struct {
	Index      int
	Term       int
	Data       []byte
	AppendedAt int64 // unix nano
}

type CommandType int

const (
	Set CommandType = iota
	Delete
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value"`
}

type Raft struct {
	mu *sync.RWMutex

	currentState State
	commitIndex  int
	lastApplied  int
	log          []LogEntry
	heartbeatCh  chan struct{}
	applyLogCh   chan struct{}

	cfg                 *Config
	clusterNodesClients map[string]protos.RaftClient
	persistent          Persistent
	fsm                 FSM

	// Only for leader
	nodesNextIndex  []int
	nodesMatchIndex []int
}

func NewRaft(ctx context.Context, cfg *Config, fsm FSM, p Persistent) (*Raft, error) {
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
		heartbeatCh:  make(chan struct{}),
		applyLogCh:   make(chan struct{}),

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

func (r *Raft) GetValue(ctx context.Context, key string) ([]byte, bool) {
	return r.fsm.Get(ctx, key)
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
		if len(r.log) > 0 {
			lastLogIndex = r.log[len(r.log)-1].Index
		}
		r.commitIndex = min(leaderCommit, lastLogIndex)
		r.applyLogCh <- struct{}{}
	}
}

func (r *Raft) AppendClientCommand(ctx context.Context, cmd Command) error {
	if r.GetCurrentState(ctx) != Leader {
		return ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	r.mu.Lock()
	lastLogEntry, _ := r.getLogEntry(ctx, len(r.log))
	newEntry := LogEntry{
		Index:      lastLogEntry.Index + 1,
		Term:       r.persistent.GetCurrentTerm(ctx),
		Data:       data,
		AppendedAt: time.Now().UTC().UnixNano(),
	}

	r.persistent.AppendLogEntries(ctx, []LogEntry{newEntry})
	r.log = append(r.log, newEntry)
	r.mu.Unlock()

	checkApplyTimer := time.NewTimer(applyInterval)
	defer checkApplyTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkApplyTimer.C:
			r.mu.RLock()
			lastApplied := r.lastApplied
			r.mu.RUnlock()

			if lastApplied >= newEntry.Index {
				return nil
			}
		}
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
		}

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

func (r *Raft) followerLoop(ctx context.Context) {
	heartbeatTimer := time.NewTimer(randomTimeout())
	defer heartbeatTimer.Stop()

	for r.GetCurrentState(ctx) == Follower {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatCh:
			heartbeatTimer.Reset(randomTimeout())
		case <-heartbeatTimer.C:
			r.SetState(ctx, Candidate)
		}
	}
}

func (r *Raft) candidateLoop(ctx context.Context) {
	currentTerm := r.persistent.IncCurrentTerm(ctx)
	voted, voteCh, votesForElect := 1, make(chan struct{}), r.quorumSize(ctx)
	r.persistent.SetVotedFor(ctx, r.cfg.NodeAddr)

	electionTimer := time.NewTimer(randomTimeout())
	defer electionTimer.Stop()

	r.callVoteForMeRPC(ctx, voteCh, currentTerm)
	for r.GetCurrentState(ctx) == Candidate {
		select {
		case <-ctx.Done():
			return
		case <-electionTimer.C:
			currentTerm = r.persistent.IncCurrentTerm(ctx)
			electionTimer.Reset(randomTimeout())
			voted = 1
			r.persistent.SetVotedFor(ctx, r.cfg.NodeAddr)
			r.callVoteForMeRPC(ctx, voteCh, currentTerm)
		case <-voteCh:
			voted++
			if voted >= votesForElect {
				r.persistent.SetCurrentTerm(ctx, currentTerm)
				r.persistent.SetVotedFor(ctx, "")
				r.SetState(ctx, Leader)
			}
		}
	}
}

func (r *Raft) leaderLoop(ctx context.Context) {
	r.initLeadersIndexes(ctx)
	r.callHeartbeatRPC(ctx)

	heartbeatTimer := time.NewTimer(heartbeatInterval)
	defer heartbeatTimer.Stop()

	replicateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.replicateLeadersLogLoop(replicateCtx)

	for r.GetCurrentState(ctx) == Leader {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTimer.C:
			r.callHeartbeatRPC(ctx)
		}
	}
}

func (r *Raft) replicateLeadersLogLoop(ctx context.Context) {
	replicateTimer := time.NewTimer(replicateInterval)
	defer replicateTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-replicateTimer.C:
			if r.GetCurrentState(ctx) != Leader {
				return
			}
		}

		r.mu.RLock()
		prevLogEntry, exists := r.GetLastLogEntry(ctx)
		if !exists {
			continue
		}
		nextCommitIndex, quorumSize := r.commitIndex+1, r.quorumSize(ctx)
		nextLogTerm := r.log[nextCommitIndex-1].Term
		r.mu.RUnlock()

		for i := range r.nodesNextIndex {
			r.mu.RLock()
			nodeNextIndex, nodeMatchIndex := r.nodesNextIndex[i], r.nodesMatchIndex[i]
			r.mu.RUnlock()

			if prevLogEntry.Index >= nodeNextIndex {
				r.mu.RLock()
				logEntries := append([]LogEntry(nil), r.log[nodeNextIndex-1:prevLogEntry.Index]...)
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
						r.nodesNextIndex[i] = prevLogEntry.Index + 1
						r.nodesMatchIndex[i] = prevLogEntry.Index
						r.mu.Unlock()
					}
				}()
			}

			if nodeMatchIndex >= nextCommitIndex {
				quorumSize--
			}
		}

		if quorumSize < 1 && nextLogTerm == r.persistent.GetCurrentTerm(ctx) {
			r.mu.Lock()
			r.commitIndex = nextCommitIndex
			r.mu.Unlock()
			r.applyLogCh <- struct{}{}
		}
	}
}

func (r *Raft) logApplierLoop(ctx context.Context) error {
	applyTimer := time.NewTimer(applyInterval)
	defer applyTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.applyLogCh:
		case <-applyTimer.C:
		}

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

func (r *Raft) applyLogEntries(ctx context.Context) {
	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		entry, _ := r.getLogEntry(ctx, i)
		r.fsm.Apply(ctx, entry)
		r.lastApplied = i
	}
	r.lastApplied = r.commitIndex
}

func (r *Raft) callVoteForMeRPC(ctx context.Context, votedCh chan<- struct{}, currentTerm int) {
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
					votedCh <- struct{}{}
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
	prevLogEntry, _ := r.getLogEntry(ctx, len(r.log))
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

func randomTimeout() time.Duration {
	return time.Duration(rand.Int64N(int64(maxTimeout-minTimeout))) + minTimeout
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
