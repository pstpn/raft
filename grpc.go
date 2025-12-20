package main

import (
	"context"

	"raft/protos"
)

type GRPCServer struct {
	protos.UnimplementedRaftServer
	raft *Raft
}

func NewGRPCServer(r *Raft) *GRPCServer {
	return &GRPCServer{raft: r}
}

func (g *GRPCServer) AppendEntries(ctx context.Context, req *protos.AppendEntriesReq) (*protos.AppendEntriesResp, error) {
	currentTerm := int64(g.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	if currentTerm < req.GetTerm() || g.raft.GetCurrentState(ctx) == Candidate {
		g.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		g.raft.SetState(ctx, Follower)
	}
	if g.raft.GetCurrentState(ctx) == Leader {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	prevLogEntry, exists := g.raft.GetLogEntry(ctx, int(req.GetPrevLogIndex()))
	if !exists || int64(prevLogEntry.Term) != req.GetPrevLogTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	g.raft.AppendLogEntries(ctx, logEntriesFromGRPC(req.GetEntries()), int(req.GetLeaderCommit()))

	return &protos.AppendEntriesResp{
		Term:    currentTerm,
		Success: true,
	}, nil
}

func (g *GRPCServer) RequestVote(ctx context.Context, req *protos.RequestVoteReq) (*protos.RequestVoteResp, error) {
	currentTerm := int64(g.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	if currentTerm < req.GetTerm() {
		g.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		g.raft.SetState(ctx, Follower)
	}
	if g.raft.GetCurrentState(ctx) == Leader {
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	lastLogEntry, _ := g.raft.GetLastLogEntry(ctx)

	// TODO: need to clear persistent [votedFor] var
	if candidateId, exists := g.raft.GetVotedFor(ctx); int64(lastLogEntry.Index) <= req.GetLastLogIndex() &&
		int64(lastLogEntry.Term) == req.GetLastLogTerm() &&
		(!exists || candidateId == req.GetCandidateId()) {
		g.raft.SetVotedFor(ctx, req.GetCandidateId())
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: true}, nil
	}

	return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
}

func (g *GRPCServer) SetValue(ctx context.Context, req *protos.SetValueReq) (*protos.SetValueResp, error) {
	err := g.raft.AppendClientCommand(ctx, Command{Type: Set, Key: req.GetKey(), Value: req.GetValue()})
	if err != nil {
		return nil, err
	}

	return &protos.SetValueResp{Success: true}, nil
}

func (g *GRPCServer) GetValue(ctx context.Context, req *protos.GetValueReq) (*protos.GetValueResp, error) {
	if g.raft.GetCurrentState(ctx) != Leader {
		return nil, ErrNotLeader
	}

	value, found := g.raft.GetValue(ctx, req.GetKey())
	return &protos.GetValueResp{Value: value, Found: found}, nil
}

func (g *GRPCServer) DeleteValue(ctx context.Context, req *protos.DeleteValueReq) (*protos.DeleteValueResp, error) {
	err := g.raft.AppendClientCommand(ctx, Command{Type: Delete, Key: req.GetKey()})
	if err != nil {
		return nil, err
	}

	return &protos.DeleteValueResp{Success: true}, nil
}

func logEntriesFromGRPC(entries []*protos.Entry) []LogEntry {
	logEntries := make([]LogEntry, len(entries))

	for i, entry := range entries {
		logEntries[i] = LogEntry{
			Index:      int(entry.GetIndex()),
			Term:       int(entry.GetTerm()),
			Data:       entry.GetData(),
			AppendedAt: entry.GetAppendedAt(),
		}
	}

	return logEntries
}
