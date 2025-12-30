package main

import "errors"

var (
	ErrNotLeader               = errors.New("not a leader")
	ErrLowerTerm               = errors.New("current term is not highest")
	ErrAppendEntriesToFollower = errors.New("failed to append entries to follower")
)
