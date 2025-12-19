package api

import (
	"context"

	"raft/protos"
	"raft/raft"
)

type GRPCHandler struct {
	protos.UnimplementedRaftServer
	raft *raft.Raft
}

func NewGRPCHandler() *GRPCHandler {
	return &GRPCHandler{}
}

func (g *GRPCHandler) AppendEntries(ctx context.Context, req *protos.AppendEntriesReq) (*protos.AppendEntriesResp, error) {
	currentTerm := int64(g.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	if currentTerm < req.GetTerm() {
		g.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		g.raft.SetState(ctx, raft.Follower)
	}
	if currentTerm == req.GetTerm() && g.raft.GetCurrentState(ctx) == raft.Candidate {
		g.raft.SetState(ctx, raft.Follower)
	}
	if g.raft.GetCurrentState(ctx) == raft.Leader {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	prevLogEntry := g.raft.GetLogEntry(ctx, int(req.GetPrevLogIndex()))
	if prevLogEntry == nil || int64(prevLogEntry.Term) != req.GetPrevLogTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	g.raft.AppendLogEntries(ctx, logEntriesFromGRPC(req.GetEntries()), int(req.GetLeaderCommit()))

	return &protos.AppendEntriesResp{
		Term:    currentTerm,
		Success: true,
	}, nil
}

func (g *GRPCHandler) RequestVote(ctx context.Context, req *protos.RequestVoteReq) (*protos.RequestVoteResp, error) {
	//TODO implement me
	panic("implement me")
}

func logEntriesFromGRPC(entries []*protos.Entry) []*raft.LogEntry {
	logEntries := make([]*raft.LogEntry, len(entries))
	for i, entry := range entries {
		logEntries[i] = &raft.LogEntry{
			Index:      int(entry.GetIndex()),
			Term:       int(entry.GetTerm()),
			Data:       entry.GetData(),
			AppendedAt: entry.GetAppendedAt(),
		}
	}
	return logEntries
}
