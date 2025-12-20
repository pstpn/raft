package main

import "errors"

var (
	ErrCurrentTermIsLower = errors.New("current term is lower then received")
	ErrNotLeader          = errors.New("not a leader")
)
