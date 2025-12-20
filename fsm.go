package main

import "context"

// FSM TODO: implement
type FSM interface {
	Apply(ctx context.Context, l LogEntry)
	LastApplied(ctx context.Context) (LogEntry, bool)
	Get(ctx context.Context, key string) ([]byte, bool)
}
