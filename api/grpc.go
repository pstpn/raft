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
	currentTerm := g.raft.GetCurrentTerm(ctx)
	if currentTerm > req.GetTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	if currentTerm < req.GetTerm() {
		g.raft.SetCurrentTerm(ctx, req.GetTerm())
		g.raft.SetState(ctx, raft.Follower)
	}
	if g.raft.GetCurrentState(ctx) == raft.Leader {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	prevLogEntry := g.raft.GetLogEntry(ctx, req.GetPrevLogIndex())
	if prevLogEntry == nil || prevLogEntry.Term != req.GetPrevLogTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	g.raft.AppendLogEntries(ctx, logEntriesFromGRPC(req.GetEntries()), req.GetLeaderCommit())

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
			Index:      entry.GetIndex(),
			Term:       entry.GetTerm(),
			Data:       entry.GetData(),
			AppendedAt: entry.GetAppendedAt(),
		}
	}
	return logEntries
}
