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
	"google.golang.org/grpc/credentials/insecure"

	"raft/protos"
)

type (
	State       int8
	CommandType int8
)

const (
	Follower State = iota
	Candidate
	Leader

	_ = true == false == true == false // =)

	Set CommandType = iota
	Delete

	minTimeout = 1500 * time.Millisecond
	maxTimeout = 3000 * time.Millisecond

	heartbeatInterval = 500 * time.Millisecond
	replicateInterval = 10 * time.Millisecond
	applyInterval     = 10 * time.Millisecond

	rpcTimeout = 200 * time.Millisecond
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (c CommandType) String() string {
	switch c {
	case Set:
		return "SET"
	case Delete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

type LogEntry struct {
	Index      int    `json:"index"`
	Term       int    `json:"term"`
	Data       []byte `json:"data"`
	AppendedAt int64  `json:"appendedAt"` // unix nano
}

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
	logger              *SimpleLogger
	clusterNodesClients map[string]protos.RaftClient
	persistent          *SimplePersistent
	fsm                 *SimpleFSM

	// Only for leader
	nodesNextIndex  []int
	nodesMatchIndex []int
}

func NewRaft(ctx context.Context, cfg *Config, logger *SimpleLogger, fsm *SimpleFSM, p *SimplePersistent) (*Raft, error) {
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
		client, err := grpc.NewClient(clusterNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clusterNodesClients[clusterNodeAddr] = protos.NewRaftClient(client)
	}

	raft := &Raft{
		mu: &sync.RWMutex{},

		currentState: Follower,
		commitIndex:  commitIndex,
		lastApplied:  lastApplied,
		log:          logEntries,
		heartbeatCh:  make(chan struct{}, 1),
		applyLogCh:   make(chan struct{}, 1),

		cfg:                 cfg,
		logger:              logger,
		clusterNodesClients: clusterNodesClients,
		persistent:          p,
		fsm:                 fsm,

		nodesNextIndex:  nil,
		nodesMatchIndex: nil,
	}

	logger.SetStateFunc(func() string {
		raft.mu.RLock()
		defer raft.mu.RUnlock()
		return raft.currentState.String()
	})

	return raft, nil
}

func (r *Raft) GetCurrentTerm(ctx context.Context) int {
	return r.persistent.GetCurrentTerm(ctx)
}

func (r *Raft) SetCurrentTerm(ctx context.Context, term int) {
	oldTerm := r.persistent.GetCurrentTerm(ctx)
	r.persistent.SetCurrentTerm(ctx, term)

	r.logger.Debugf("term changed: oldTerm=%d, currentTerm=%d", oldTerm, term)
}

func (r *Raft) SetVotedFor(ctx context.Context, candidateId string) {
	r.persistent.SetVotedFor(ctx, candidateId)

	r.logger.Debugf("voted for %q", candidateId)
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
	oldState := r.currentState
	r.currentState = state
	r.mu.Unlock()

	r.logger.Debugf("state changed: oldState=%q, currentState=%q", oldState.String(), state.String())
}

func (r *Raft) GetLogEntry(ctx context.Context, index int) (LogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLog(ctx, index)
}

func (r *Raft) GetLastLogEntry(ctx context.Context) (LogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLog(ctx, len(r.log))
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []LogEntry, leaderCommit int) {
	if len(l) < 1 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.persistent.AppendLogEntries(ctx, l)
	r.appendLogs(ctx, l)

	r.logger.Debugf("appended %d log entries from leader", len(l))

	if leaderCommit > r.commitIndex {
		lastLogIndex := 0
		if len(r.log) > 0 {
			lastLogIndex = r.log[len(r.log)-1].Index
		}
		r.commitIndex = min(leaderCommit, lastLogIndex)
		r.sendNonblockingApply(ctx)

		r.logger.Debugf("updated commitIndex to %d", r.commitIndex)
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
	lastLogEntry, _ := r.getLog(ctx, len(r.log))
	newEntry := LogEntry{
		Index:      lastLogEntry.Index + 1,
		Term:       r.persistent.GetCurrentTerm(ctx),
		Data:       data,
		AppendedAt: time.Now().UTC().UnixNano(),
	}

	r.persistent.AppendLogEntries(ctx, []LogEntry{newEntry})
	r.log = append(r.log, newEntry)
	r.mu.Unlock()

	r.logger.Infof("appended client command: %q", cmd.Type.String()+" "+cmd.Key)

	checkApplyTicker := time.NewTicker(applyInterval)
	defer checkApplyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkApplyTicker.C:
			r.mu.RLock()
			lastApplied := r.lastApplied
			r.mu.RUnlock()

			if lastApplied >= newEntry.Index {
				return nil
			}
		}
	}
}

func (r *Raft) SendNonblockingHeartbeat(_ context.Context) {
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
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
	heartbeatTicker := time.NewTicker(randomTimeout())
	defer heartbeatTicker.Stop()

	for r.GetCurrentState(ctx) == Follower {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatCh:
			heartbeatTicker.Reset(randomTimeout())
		case <-heartbeatTicker.C:
			r.SetState(ctx, Candidate)
		}
	}
}

func (r *Raft) candidateLoop(ctx context.Context) {
	currentTerm := r.persistent.IncCurrentTerm(ctx)
	voted, voteCh, votesForElect := 1, make(chan struct{}), r.quorumSize()
	r.persistent.SetVotedFor(ctx, r.cfg.NodeAddr)

	electionTicker := time.NewTicker(randomTimeout())
	defer electionTicker.Stop()

	r.callVoteForMeRPC(ctx, voteCh, currentTerm)
	for r.GetCurrentState(ctx) == Candidate {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatCh:
			electionTicker.Reset(randomTimeout())
		case <-electionTicker.C:
			currentTerm, voted = r.persistent.IncCurrentTerm(ctx), 1
			electionTicker.Reset(randomTimeout())
			r.persistent.SetVotedFor(ctx, r.cfg.NodeAddr)

			r.logger.Debugf("election timeout: oldTerm=%d, currentTerm=%d", currentTerm-1, currentTerm)

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
	r.logger.Debugf("became leader, sending initial heartbeats")

	r.initLeadersIndexes(ctx)
	r.callHeartbeatRPC(ctx)

	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	replicateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.replicateLeadersLogLoop(replicateCtx)

	for r.GetCurrentState(ctx) == Leader {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			r.callHeartbeatRPC(ctx)
		}
	}
}

func (r *Raft) replicateLeadersLogLoop(ctx context.Context) {
	replicateTicker := time.NewTicker(replicateInterval)
	defer replicateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-replicateTicker.C:
			if r.GetCurrentState(ctx) != Leader {
				return
			}
		}

		r.mu.RLock()
		logLen := len(r.log)
		nodesNextIndex := append([]int(nil), r.nodesNextIndex...)
		nodesMatchIndex := append([]int(nil), r.nodesMatchIndex...)

		nextCommitIndex := r.commitIndex + 1
		quorumSize := r.quorumSize()
		r.mu.RUnlock()

		for nodeIdx, nodeAddr := range r.cfg.ClusterNodesAddr {
			nodeNextIndex := nodesNextIndex[nodeIdx]
			if nodeNextIndex < 1 {
				nodeNextIndex = 1
			}

			nodeMatchIndex := nodesMatchIndex[nodeIdx]
			if nodesMatchIndex[nodeIdx] < logLen && nodeNextIndex < logLen {
				r.mu.RLock()
				prevLogEntry, _ := r.getLog(ctx, nodeNextIndex-1)
				entriesToSend := append([]LogEntry(nil), r.log[prevLogEntry.Index-1:logLen]...)
				nodeClient := r.clusterNodesClients[nodeAddr]
				r.mu.RUnlock()

				go r.callAppendEntriesRPC(ctx, nodeIdx, entriesToSend, prevLogEntry, nodeClient)
			}

			if nodeMatchIndex >= nextCommitIndex {
				quorumSize--
			}
		}

		if nextCommitIndex <= logLen {
			r.mu.RLock()
			nextCommitLogTerm := r.log[nextCommitIndex-1].Term
			r.mu.RUnlock()

			if quorumSize < 1 && nextCommitLogTerm == r.persistent.GetCurrentTerm(ctx) {
				r.mu.Lock()
				if r.commitIndex < nextCommitIndex {
					r.commitIndex = nextCommitIndex
				}
				r.mu.Unlock()

				r.sendNonblockingApply(ctx)
			}
		}
	}
}

func (r *Raft) logApplierLoop(ctx context.Context) error {
	applyTicker := time.NewTicker(applyInterval)
	defer applyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.applyLogCh:
		case <-applyTicker.C:
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

func (r *Raft) getLog(_ context.Context, index int) (LogEntry, bool) {
	if index < 1 || len(r.log) < index {
		return LogEntry{}, false
	}

	return r.log[index-1], true
}

func (r *Raft) quorumSize() int {
	totalNodes := len(r.clusterNodesClients) + 1
	return totalNodes/2 + 1
}

func (r *Raft) appendLogs(ctx context.Context, l []LogEntry) {
	for i, newLogEntry := range l {
		currentLogEntry, exists := r.getLog(ctx, newLogEntry.Index)
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
	prevAppliedIndex := r.lastApplied

	for i := prevAppliedIndex + 1; i <= r.commitIndex; i++ {
		entry, _ := r.getLog(ctx, i)
		r.fsm.Apply(ctx, entry)
		r.lastApplied = i

		r.logger.Debugf("applied log entry: index=%d, term=%d", entry.Index, entry.Term)
	}
	r.lastApplied = r.commitIndex

	applied := r.lastApplied - prevAppliedIndex
	if applied > 0 {
		r.logger.Infof("applied %d log entries up to index=%d", applied, r.lastApplied)
	}
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
		go func(client protos.RaftClient) {
			rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()

			resp, err := client.RequestVote(rpcCtx, &protos.RequestVoteReq{
				Term:         int64(currentTerm),
				CandidateId:  r.cfg.NodeAddr,
				LastLogIndex: commitIndex,
				LastLogTerm:  logTerm,
			})
			if err == nil && resp != nil {
				if resp.GetTerm() > int64(currentTerm) {
					r.logger.Debugf("received higher term in requestVote response: oldTerm=%d, currentTerm=%d",
						currentTerm,
						resp.GetTerm(),
					)

					r.persistent.SetCurrentTerm(ctx, int(resp.GetTerm()))
					r.SetState(ctx, Follower)
					return
				}

				if resp.GetVoteGranted() {
					votedCh <- struct{}{}
				}
			}
		}(nodeClient)
	}
}

func (r *Raft) callHeartbeatRPC(ctx context.Context) {
	r.mu.RLock()
	prevLogEntry, _ := r.getLog(ctx, len(r.log))
	r.mu.RUnlock()

	for nodeIdx, nodeAddr := range r.cfg.ClusterNodesAddr {
		go func(nodeIdx int, address string, client protos.RaftClient, prev LogEntry) {
			r.logger.Debugf("sending heartbeat to %q", address)

			if !r.callAppendEntriesRPC(ctx, nodeIdx, nil, prev, client) {
				r.logger.Warnf("heartbeat to %q rejected", address)
			}
		}(nodeIdx, nodeAddr, r.clusterNodesClients[nodeAddr], prevLogEntry)
	}
}

func (r *Raft) callAppendEntriesRPC(
	ctx context.Context,
	nodeIdx int,
	entries []LogEntry,
	prevLogEntry LogEntry,
	nodeClient protos.RaftClient,
) bool {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	currentTerm := r.persistent.GetCurrentTerm(ctx)
	r.mu.RLock()
	commitIndex := int64(r.commitIndex)
	r.mu.RUnlock()

	var appendEntries []*protos.Entry
	if len(entries) > 0 {
		appendEntries = logEntriesToGRPC(entries)
	}

	resp, err := nodeClient.AppendEntries(rpcCtx, &protos.AppendEntriesReq{
		Entries:      appendEntries,
		Term:         int64(currentTerm),
		LeaderId:     r.cfg.NodeAddr,
		PrevLogIndex: int64(prevLogEntry.Index),
		PrevLogTerm:  int64(prevLogEntry.Term),
		LeaderCommit: commitIndex,
	})
	if err != nil || resp == nil {
		return false
	}
	if resp.GetTerm() > int64(currentTerm) {
		r.logger.Debugf("received higher term in appendEntries response: oldTerm=%d, currentTerm=%d",
			currentTerm,
			resp.GetTerm(),
		)

		r.persistent.SetCurrentTerm(ctx, int(resp.GetTerm()))
		r.SetState(ctx, Follower)

		return false
	}
	if !resp.GetSuccess() {
		r.mu.Lock()
		if r.nodesNextIndex[nodeIdx] > 1 {
			r.nodesNextIndex[nodeIdx]--
		}
		r.mu.Unlock()

		return resp.GetSuccess()
	}

	lastSentIndex := prevLogEntry.Index
	if len(entries) > 0 {
		lastSentIndex = entries[len(entries)-1].Index
	}

	r.mu.Lock()
	r.nodesNextIndex[nodeIdx] = lastSentIndex + 1
	r.nodesMatchIndex[nodeIdx] = lastSentIndex
	r.mu.Unlock()

	return resp.GetSuccess()
}

func (r *Raft) initLeadersIndexes(ctx context.Context) {
	lastLogIndex := 0
	r.mu.RLock()
	lastLogEntry, exists := r.getLog(ctx, len(r.log))
	r.mu.RUnlock()
	if exists {
		lastLogIndex = lastLogEntry.Index
	}

	r.mu.Lock()
	r.nodesNextIndex, r.nodesMatchIndex = make([]int, len(r.cfg.ClusterNodesAddr)), make([]int, len(r.cfg.ClusterNodesAddr))
	for i := range r.nodesNextIndex {
		r.nodesNextIndex[i] = lastLogIndex + 1
	}
	r.mu.Unlock()
}

func (r *Raft) sendNonblockingApply(_ context.Context) {
	select {
	case r.applyLogCh <- struct{}{}:
	default:
	}
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
