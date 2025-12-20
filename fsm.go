package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const fsmFilePerm = 0o777

type SimpleFSM struct {
	mu sync.RWMutex

	dataDir         string
	kvFile          string
	lastAppliedFile string

	kv          map[string][]byte
	lastApplied LogEntry
	hasApplied  bool
}

func NewSimpleFSM(dataDir string) (*SimpleFSM, error) {
	if err := os.MkdirAll(dataDir, fsmFilePerm); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	fsm := &SimpleFSM{
		dataDir:         dataDir,
		kvFile:          filepath.Join(dataDir, "kv.json"),
		lastAppliedFile: filepath.Join(dataDir, "last_applied.json"),
		kv:              make(map[string][]byte),
		hasApplied:      false,
	}

	if err := fsm.loadStore(); err != nil {
		return nil, fmt.Errorf("failed to load kv: %w", err)
	}
	if err := fsm.loadLastApplied(); err != nil {
		return nil, fmt.Errorf("failed to load last applied: %w", err)
	}

	return fsm, nil
}

func (s *SimpleFSM) loadStore() error {
	data, err := os.ReadFile(s.kvFile)
	if err != nil {
		if os.IsNotExist(err) {
			s.kv = make(map[string][]byte)
			return nil
		}

		return err
	}

	if len(data) == 0 {
		s.kv = make(map[string][]byte)
		return nil
	}

	var store map[string][]byte
	if err := json.Unmarshal(data, &store); err != nil {
		return fmt.Errorf("failed to unmarshal kv: %w", err)
	}
	s.kv = store

	return nil
}

func (s *SimpleFSM) saveKV() error {
	data, err := json.Marshal(s.kv)
	if err != nil {
		return fmt.Errorf("failed to marshal kv: %w", err)
	}

	return os.WriteFile(s.kvFile, data, fsmFilePerm)
}

func (s *SimpleFSM) loadLastApplied() error {
	data, err := os.ReadFile(s.lastAppliedFile)
	if err != nil {
		if os.IsNotExist(err) {
			s.lastApplied = LogEntry{}
			s.hasApplied = false
			return nil
		}

		return err
	}

	if len(data) == 0 {
		s.lastApplied = LogEntry{}
		s.hasApplied = false
		return nil
	}

	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return fmt.Errorf("failed to unmarshal last applied: %w", err)
	}
	s.lastApplied = entry
	s.hasApplied = true

	return nil
}

func (s *SimpleFSM) saveLastApplied() error {
	data, err := json.Marshal(s.lastApplied)
	if err != nil {
		return fmt.Errorf("failed to marshal last applied: %w", err)
	}

	return os.WriteFile(s.lastAppliedFile, data, fsmFilePerm)
}

func (s *SimpleFSM) Apply(_ context.Context, l LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %v", err))
	}

	switch cmd.Type {
	case Set:
		s.kv[cmd.Key] = cmd.Value
	case Delete:
		delete(s.kv, cmd.Key)
	default:
		panic(fmt.Sprintf("unknown command type: %v", cmd.Type))
	}

	s.lastApplied = l
	s.hasApplied = true

	if err := s.saveKV(); err != nil {
		panic(fmt.Sprintf("failed to save kv: %v", err))
	}
	if err := s.saveLastApplied(); err != nil {
		panic(fmt.Sprintf("failed to save last applied: %v", err))
	}
}

func (s *SimpleFSM) LastApplied(_ context.Context) (LogEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied, s.hasApplied
}

func (s *SimpleFSM) Get(_ context.Context, key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.kv[key]
	return value, exists
}
