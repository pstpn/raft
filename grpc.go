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
		g.raft.logger.Debugf("ignoring appendEntries from %q: currentTerm=%d, reqTerm=%d",
			req.GetLeaderId(),
			currentTerm,
			req.GetTerm(),
		)

		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	if currentTerm < req.GetTerm() || g.raft.GetCurrentState() == Candidate {
		g.raft.logger.Debugf("received appendEntries from %q with higher/equal: reqTerm=%d, currentTerm=%d",
			req.GetLeaderId(),
			req.GetTerm(),
			currentTerm,
		)

		g.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		g.raft.SetState(Follower)
	}

	if g.raft.GetCurrentState() == Leader {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}
	defer g.raft.SendNonblockingHeartbeat(ctx)

	prevLogEntry, exists := g.raft.GetLogEntry(int(req.GetPrevLogIndex()))
	if (req.GetPrevLogIndex() > 0 && !exists) || int64(prevLogEntry.Term) != req.GetPrevLogTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	g.raft.AppendLogEntries(ctx, g.logEntriesFromGRPC(req.GetEntries()), int(req.GetLeaderCommit()))

	return &protos.AppendEntriesResp{
		Term:    currentTerm,
		Success: true,
	}, nil
}

func (g *GRPCServer) RequestVote(ctx context.Context, req *protos.RequestVoteReq) (*protos.RequestVoteResp, error) {
	currentTerm := int64(g.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		g.raft.logger.Debugf("ignoring requestVote from %q: currentTerm=%d, reqTerm=%d",
			req.GetCandidateId(),
			currentTerm,
			req.GetTerm(),
		)

		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	if currentTerm < req.GetTerm() {
		g.raft.logger.Debugf("received requestVote from %q with higher term: reqTerm=%d, currentTerm=%d",
			req.GetCandidateId(),
			req.GetTerm(),
			currentTerm,
		)

		g.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		g.raft.SetState(Follower)
	}

	if g.raft.GetCurrentState() == Leader {
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}
	defer g.raft.SendNonblockingHeartbeat(ctx)

	lastLogEntry := g.raft.GetLastLogEntry()
	upToDate := req.GetLastLogTerm() > int64(lastLogEntry.Term) ||
		(req.GetLastLogTerm() == int64(lastLogEntry.Term) && req.GetLastLogIndex() >= int64(lastLogEntry.Index))
	if candidateId, exists := g.raft.GetVotedFor(ctx); upToDate && (!exists || candidateId == req.GetCandidateId()) {
		g.raft.logger.Debugf("granting vote for %q in term=%d", req.GetCandidateId(), req.GetTerm())

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

func (g *GRPCServer) logEntriesFromGRPC(entries []*protos.Entry) []LogEntry {
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
