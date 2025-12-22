package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft/protos"
)

type (
	State       int32
	CommandType int8
)

const (
	Follower State = iota
	Candidate
	Leader

	_ = true == false == true == false // =)

	Set CommandType = iota
	Delete

	minTimeout = 350 * time.Millisecond
	maxTimeout = 700 * time.Millisecond
	rpcTimeout = 200 * time.Millisecond

	leaderHeartbeatInterval = 200 * time.Millisecond
	leaderReplicateInterval = 20 * time.Millisecond
	applyInterval           = 20 * time.Millisecond
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

	currentState *atomic.Int32
	commitIndex  *atomic.Int64
	lastApplied  *atomic.Int64
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

		currentState: &atomic.Int32{},
		commitIndex:  &atomic.Int64{},
		lastApplied:  &atomic.Int64{},
		log:          logEntries,
		heartbeatCh:  make(chan struct{}),
		applyLogCh:   make(chan struct{}),

		cfg:                 cfg,
		logger:              logger,
		clusterNodesClients: clusterNodesClients,
		persistent:          p,
		fsm:                 fsm,

		nodesNextIndex:  nil,
		nodesMatchIndex: nil,
	}
	raft.currentState.Store(int32(Follower))
	raft.commitIndex.Store(int64(commitIndex))
	raft.lastApplied.Store(int64(lastApplied))

	logger.SetStateFunc(func() string { return raft.GetCurrentState().String() })

	return raft, nil
}

func (r *Raft) GetCurrentTerm(ctx context.Context) int { return r.persistent.GetCurrentTerm(ctx) }

func (r *Raft) SetCurrentTerm(ctx context.Context, term int) {
	oldTerm := r.persistent.GetCurrentTerm(ctx)
	r.persistent.SetCurrentTerm(ctx, term)

	r.logger.Debugf("term changed: oldTerm=%d, currentTerm=%d", oldTerm, term)
}

func (r *Raft) SetVotedFor(ctx context.Context, candidateId string) {
	r.persistent.SetVotedFor(ctx, candidateId)

	r.logger.Debugf("voted for %q", candidateId)
}

func (r *Raft) GetVotedFor(ctx context.Context) (string, bool) { return r.persistent.GetVotedFor(ctx) }

func (r *Raft) GetValue(ctx context.Context, key string) ([]byte, bool) { return r.fsm.Get(ctx, key) }

func (r *Raft) GetCurrentState() State { return State(r.currentState.Load()) }

func (r *Raft) SetState(state State) {
	if oldState := State(r.currentState.Swap(int32(state))); oldState != state {
		r.logger.Infof("state changed: oldState=%q, currentState=%q", oldState.String(), state.String())
	}
}

func (r *Raft) GetLogEntry(index int) (LogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLog(index)
}

func (r *Raft) GetLastLogEntry() LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, _ := r.getLog(len(r.log))

	return l
}

func (r *Raft) AppendLogEntries(ctx context.Context, l []LogEntry, leaderCommit int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(l) > 0 {
		r.appendLogs(l)
		r.persistent.SetLogEntries(ctx, r.log)

		r.logger.Debugf("appended %d log entries from leader", len(l))
	}

	if int64(leaderCommit) > r.commitIndex.Load() {
		lastLogIndex := 0
		if len(r.log) > 0 {
			lastLogIndex = r.log[len(r.log)-1].Index
		}
		r.commitIndex.Store(int64(min(leaderCommit, lastLogIndex)))
		r.sendNonblockingApply(ctx)

		r.logger.Debugf("updated commitIndex to %d", r.commitIndex.Load())
	}
}

func (r *Raft) AppendClientCommand(ctx context.Context, cmd Command) error {
	if r.GetCurrentState() != Leader {
		return ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	r.mu.Lock()
	lastLogEntry, _ := r.getLog(len(r.log))
	newEntry := LogEntry{
		Index:      lastLogEntry.Index + 1,
		Term:       r.persistent.GetCurrentTerm(ctx),
		Data:       data,
		AppendedAt: time.Now().UTC().UnixNano(),
	}

	r.log = append(r.log, newEntry)
	r.persistent.SetLogEntries(ctx, r.log)
	r.mu.Unlock()

	r.logger.Infof("appended client command=%q for key=%q", cmd.Type.String(), cmd.Key)

	checkApplyTicker := time.NewTicker(applyInterval)
	defer checkApplyTicker.Stop()

	for {
		if r.GetCurrentState() != Leader {
			return ErrNotLeader
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkApplyTicker.C:
			if r.lastApplied.Load() >= int64(newEntry.Index) {
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

		switch state := r.GetCurrentState(); state {
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

	for r.GetCurrentState() == Follower {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatCh:
			heartbeatTicker.Reset(randomTimeout())
		case <-heartbeatTicker.C:
			r.SetState(Candidate)
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
	for r.GetCurrentState() == Candidate {
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
				r.SetState(Leader)
			}
		}
	}
}

func (r *Raft) leaderLoop(ctx context.Context) {
	r.logger.Debug("became leader, sending initial heartbeats")

	stillFollowedNodes := &atomic.Int64{}
	r.initLeadersIndexes()
	r.callHeartbeatRPC(ctx, stillFollowedNodes)

	replicateTicker, heartbeatTicker := time.NewTicker(leaderReplicateInterval), time.NewTicker(leaderHeartbeatInterval)
	defer func() {
		replicateTicker.Stop()
		heartbeatTicker.Stop()
	}()

	for r.GetCurrentState() == Leader {
		select {
		case <-ctx.Done():
			return
		case <-replicateTicker.C:
			r.replicateLeadersLog(ctx)
		case <-heartbeatTicker.C:
			if stillFollowedNodes.Swap(0) < int64(r.quorumSize()-1) {
				r.SetState(Follower)
				return
			}
			r.callHeartbeatRPC(ctx, stillFollowedNodes)
		}
	}
}

func (r *Raft) replicateLeadersLog(ctx context.Context) {
	r.mu.RLock()
	logLen := len(r.log)
	nodesNextIndex := append([]int(nil), r.nodesNextIndex...)
	nodesMatchIndex := append([]int(nil), r.nodesMatchIndex...)

	nextCommitIndex := r.commitIndex.Load() + 1
	neededForQuorum := r.quorumSize() - 1
	r.mu.RUnlock()

	for nodeIdx, nodeAddr := range r.cfg.ClusterNodesAddr {
		nodeNextIndex, nodeMatchIndex := nodesNextIndex[nodeIdx], nodesMatchIndex[nodeIdx]
		if nodeMatchIndex < logLen && nodeNextIndex <= logLen {
			r.mu.RLock()
			prevLogEntry, _ := r.getLog(nodeNextIndex - 1)
			startIdx := nodeNextIndex - 1
			if startIdx < 0 {
				startIdx = 0
			}
			entriesToSend := append([]LogEntry(nil), r.log[startIdx:logLen]...)
			r.mu.RUnlock()

			r.logger.Debugf("sending appendEntries to %q, entriesCount=%d", nodeAddr, len(entriesToSend))

			go r.callAppendEntriesRPC(ctx, nodeIdx, entriesToSend, prevLogEntry, nodeAddr)
		}

		if int64(nodeMatchIndex) >= nextCommitIndex {
			neededForQuorum--
		}
	}

	if nextCommitIndex <= int64(logLen) {
		r.mu.RLock()
		nextCommitLogTerm := r.log[nextCommitIndex-1].Term
		r.mu.RUnlock()

		if neededForQuorum < 1 && nextCommitLogTerm == r.persistent.GetCurrentTerm(ctx) {
			if r.commitIndex.Load() < nextCommitIndex {
				r.commitIndex.Store(nextCommitIndex)
			}

			r.sendNonblockingApply(ctx)
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

		if r.commitIndex.Load() > r.lastApplied.Load() {
			r.mu.Lock()
			r.applyLogEntries(ctx)
			r.mu.Unlock()
		}
	}
}

func (r *Raft) getLog(index int) (LogEntry, bool) {
	if index < 1 || len(r.log) < index {
		return LogEntry{}, false
	}

	return r.log[index-1], true
}

func (r *Raft) quorumSize() int {
	totalNodes := len(r.clusterNodesClients) + 1
	return totalNodes/2 + 1
}

func (r *Raft) appendLogs(l []LogEntry) {
	for i, newLogEntry := range l {
		currentLogEntry, exists := r.getLog(newLogEntry.Index)
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
	prevAppliedIndex := r.lastApplied.Load()

	for i := prevAppliedIndex + 1; i <= r.commitIndex.Load(); i++ {
		entry, _ := r.getLog(int(i))
		r.fsm.Apply(ctx, entry)
		r.lastApplied.Store(i)

		r.logger.Debugf("applied log entry: index=%d, term=%d", entry.Index, entry.Term)
	}
	r.lastApplied.Store(r.commitIndex.Load())

	applied := r.lastApplied.Load() - prevAppliedIndex
	if applied > 0 {
		r.logger.Infof("applied %d log entries up to index=%d", applied, r.lastApplied.Load())
	}
}

func (r *Raft) callVoteForMeRPC(ctx context.Context, votedCh chan<- struct{}, currentTerm int) {
	lastLogEntry := r.GetLastLogEntry()

	for _, nodeClient := range r.clusterNodesClients {
		go func(client protos.RaftClient) {
			rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()

			resp, err := client.RequestVote(rpcCtx, &protos.RequestVoteReq{
				Term:         int64(currentTerm),
				CandidateId:  r.cfg.NodeAddr,
				LastLogIndex: int64(lastLogEntry.Index),
				LastLogTerm:  int64(lastLogEntry.Term),
			})
			if err == nil && resp != nil {
				if resp.GetTerm() > int64(currentTerm) {
					r.logger.Debugf("received higher term in requestVote response: oldTerm=%d, currentTerm=%d",
						currentTerm,
						resp.GetTerm(),
					)

					r.persistent.SetCurrentTerm(ctx, int(resp.GetTerm()))
					r.SetState(Follower)
					return
				}

				if resp.GetVoteGranted() {
					votedCh <- struct{}{}
				}
			}
		}(nodeClient)
	}
}

func (r *Raft) callHeartbeatRPC(ctx context.Context, stillFollowed *atomic.Int64) {
	r.mu.RLock()
	prevLogEntry, _ := r.getLog(len(r.log))
	r.mu.RUnlock()

	for nodeIdx, nodeAddr := range r.cfg.ClusterNodesAddr {
		go func(nodeIdx int, address string, prev LogEntry) {
			r.logger.Debugf("sending heartbeat to %q", address)

			if !r.callAppendEntriesRPC(ctx, nodeIdx, nil, prev, address) {
				r.logger.Warnf("heartbeat to %q rejected", address)
				return
			}
			stillFollowed.Add(1)
		}(nodeIdx, nodeAddr, prevLogEntry)
	}
}

func (r *Raft) callAppendEntriesRPC(
	ctx context.Context,
	nodeIdx int,
	entries []LogEntry,
	prevLogEntry LogEntry,
	nodeAddr string,
) bool {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	currentTerm, commitIndex := r.persistent.GetCurrentTerm(ctx), r.commitIndex.Load()
	var appendEntries []*protos.Entry
	if len(entries) > 0 {
		appendEntries = logEntriesToGRPC(entries)
	}

	resp, err := r.clusterNodesClients[nodeAddr].AppendEntries(rpcCtx, &protos.AppendEntriesReq{
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
		r.SetState(Follower)

		return false
	}
	if !resp.GetSuccess() {
		r.logger.Debugf("can't appendEntries to %q, decrementing next index and trying again", nodeAddr)

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

func (r *Raft) initLeadersIndexes() {
	r.mu.RLock()
	lastLogEntry, exists := r.getLog(len(r.log))
	r.mu.RUnlock()

	lastLogIndex := 0
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
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxTimeout-minTimeout)))
	return time.Duration(n.Int64()) + minTimeout
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
