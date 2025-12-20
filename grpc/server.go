package grpc

import (
	"context"

	"raft/protos"
	"raft/raft"
)

type Server struct {
	protos.UnimplementedRaftServer
	raft *raft.Raft
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) AppendEntries(ctx context.Context, req *protos.AppendEntriesReq) (*protos.AppendEntriesResp, error) {
	currentTerm := int64(s.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	if currentTerm < req.GetTerm() || s.raft.GetCurrentState(ctx) == raft.Candidate {
		s.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		s.raft.SetState(ctx, raft.Follower)
	}
	if s.raft.GetCurrentState(ctx) == raft.Leader {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	prevLogEntry, exists := s.raft.GetLogEntry(ctx, int(req.GetPrevLogIndex()))
	if !exists || int64(prevLogEntry.Term) != req.GetPrevLogTerm() {
		return &protos.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	s.raft.AppendLogEntries(ctx, logEntriesFromGRPC(req.GetEntries()), int(req.GetLeaderCommit()))

	return &protos.AppendEntriesResp{
		Term:    currentTerm,
		Success: true,
	}, nil
}

func (s *Server) RequestVote(ctx context.Context, req *protos.RequestVoteReq) (*protos.RequestVoteResp, error) {
	currentTerm := int64(s.raft.GetCurrentTerm(ctx))
	if currentTerm > req.GetTerm() {
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	if currentTerm < req.GetTerm() {
		s.raft.SetCurrentTerm(ctx, int(req.GetTerm()))
		s.raft.SetState(ctx, raft.Follower)
	}
	if s.raft.GetCurrentState(ctx) == raft.Leader {
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	lastLogEntry, _ := s.raft.GetLastLogEntry(ctx)

	// TODO: need to clear persistent [votedFor] var
	if candidateId, exists := s.raft.GetVotedFor(ctx); int64(lastLogEntry.Index) <= req.GetLastLogIndex() &&
		int64(lastLogEntry.Term) == req.GetLastLogTerm() &&
		(!exists || candidateId == req.GetCandidateId()) {
		s.raft.SetVotedFor(ctx, req.GetCandidateId())
		return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: true}, nil
	}

	return &protos.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
}

func logEntriesFromGRPC(entries []*protos.Entry) []raft.LogEntry {
	logEntries := make([]raft.LogEntry, len(entries))
	for i, entry := range entries {
		logEntries[i] = raft.LogEntry{
			Index:      int(entry.GetIndex()),
			Term:       int(entry.GetTerm()),
			Data:       entry.GetData(),
			AppendedAt: entry.GetAppendedAt(),
		}
	}
	return logEntries
}
