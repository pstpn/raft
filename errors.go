package main

import "errors"

var (
	ErrNotLeader = errors.New("not a leader")
)
