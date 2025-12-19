package raft

import "errors"

var (
	ErrCurrentTermIsLower = errors.New("current term is lower then received")
)
