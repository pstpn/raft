package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const filePerm = 0o777

type SimplePersistent struct {
	mu sync.RWMutex

	dataDir      string
	termFile     string
	votedForFile string
	logFile      string

	currentTerm int
	votedFor    string
	logEntries  []LogEntry
}

func NewSimplePersistent(dataDir string) (*SimplePersistent, error) {
	if err := os.MkdirAll(dataDir, filePerm); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	sp := &SimplePersistent{
		dataDir:      dataDir,
		termFile:     filepath.Join(dataDir, "current_term.txt"),
		votedForFile: filepath.Join(dataDir, "voted_for.txt"),
		logFile:      filepath.Join(dataDir, "log.json"),
		logEntries:   make([]LogEntry, 0),
	}

	if err := sp.loadTerm(); err != nil {
		return nil, fmt.Errorf("failed to load term: %w", err)
	}
	if err := sp.loadVotedFor(); err != nil {
		return nil, fmt.Errorf("failed to load voted for: %w", err)
	}
	if err := sp.loadLog(); err != nil {
		return nil, fmt.Errorf("failed to load log: %w", err)
	}

	return sp, nil
}

func (s *SimplePersistent) loadTerm() error {
	data, err := os.ReadFile(s.termFile)
	if err != nil {
		if os.IsNotExist(err) {
			s.currentTerm = 0
			return nil
		}

		return err
	}

	_, err = fmt.Sscanf(string(data), "%d", &s.currentTerm)
	return err
}

func (s *SimplePersistent) saveTerm() error {
	return os.WriteFile(s.termFile, []byte(strconv.Itoa(s.currentTerm)), filePerm)
}

func (s *SimplePersistent) loadVotedFor() error {
	data, err := os.ReadFile(s.votedForFile)
	if err != nil {
		if os.IsNotExist(err) {
			s.votedFor = ""
			return nil
		}

		return err
	}

	if len(data) == 0 {
		s.votedFor = ""
		return nil
	}
	s.votedFor = string(data)

	return nil
}

func (s *SimplePersistent) saveVotedFor() error {
	return os.WriteFile(s.votedForFile, []byte(s.votedFor), filePerm)
}

func (s *SimplePersistent) loadLog() error {
	data, err := os.ReadFile(s.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			s.logEntries = make([]LogEntry, 0)
			return nil
		}

		return err
	}

	if len(data) == 0 {
		s.logEntries = make([]LogEntry, 0)
		return nil
	}

	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("failed to unmarshal log: %w", err)
	}
	s.logEntries = entries

	return nil
}

func (s *SimplePersistent) saveLog() error {
	data, err := json.Marshal(s.logEntries)
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	return os.WriteFile(s.logFile, data, filePerm)
}

func (s *SimplePersistent) GetCurrentTerm(_ context.Context) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

func (s *SimplePersistent) IncCurrentTerm(_ context.Context) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm++
	s.votedFor = ""

	if err := s.saveTerm(); err != nil {
		panic(fmt.Sprintf("failed to save term: %v", err))
	}
	if err := s.saveVotedFor(); err != nil {
		panic(fmt.Sprintf("failed to save voted for: %v", err))
	}

	return s.currentTerm
}

func (s *SimplePersistent) SetCurrentTerm(_ context.Context, term int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = ""

	if err := s.saveTerm(); err != nil {
		panic(fmt.Sprintf("failed to save term: %v", err))
	}
	if err := s.saveVotedFor(); err != nil {
		panic(fmt.Sprintf("failed to save voted for: %v", err))
	}
}

func (s *SimplePersistent) GetVotedFor(_ context.Context) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor, s.votedFor != ""
}

func (s *SimplePersistent) SetVotedFor(_ context.Context, candidateId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateId

	if err := s.saveVotedFor(); err != nil {
		panic(fmt.Sprintf("failed to save voted for: %v", err))
	}
}

func (s *SimplePersistent) GetLogEntries(_ context.Context) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]LogEntry(nil), s.logEntries...)
}

func (s *SimplePersistent) SetLogEntries(_ context.Context, l []LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logEntries = append([]LogEntry(nil), l...)

	if err := s.saveLog(); err != nil {
		panic(fmt.Sprintf("failed to save log: %v", err))
	}
}
