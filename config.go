package main

import (
	"os"
	"strings"
)

type Config struct {
	LogLevel          string   `yaml:"logLevel"`
	FSMDataDir        string   `yaml:"fsmDataDir"`
	PersistentDataDir string   `yaml:"persistentDataDir"`
	NodeAddr          string   `yaml:"nodeAddr"`
	ClusterNodesAddr  []string `yaml:"clusterNodesAddr"`
}

func ReadConfig() *Config {
	cfg := &Config{
		LogLevel:          "info",
		FSMDataDir:        "./data/fsm",
		PersistentDataDir: "./data/persistent",
		NodeAddr:          "127.0.0.1:4001",
		ClusterNodesAddr:  []string{},
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		cfg.LogLevel = logLevel
	}
	if nodeAddr := os.Getenv("NODE_ADDR"); nodeAddr != "" {
		cfg.NodeAddr = nodeAddr
	}
	if clusterNodesAddr := os.Getenv("CLUSTER_NODES_ADDR"); clusterNodesAddr != "" {
		cfg.ClusterNodesAddr = strings.Split(clusterNodesAddr, ",")
	}
	if fsmDataDir := os.Getenv("FSM_DATA_DIR"); fsmDataDir != "" {
		cfg.FSMDataDir = fsmDataDir
	}
	if persistentDataDir := os.Getenv("PERSISTENT_DATA_DIR"); persistentDataDir != "" {
		cfg.PersistentDataDir = persistentDataDir
	}

	return cfg
}
