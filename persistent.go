package main

import "context"

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
