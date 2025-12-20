package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	configFileName = "config.yaml"
	configEnvVar   = "CONFIG_PATH"
)

type Config struct {
	LogLevel          string   `yaml:"logLevel"`
	FSMDataDir        string   `yaml:"fsmDataDir"`
	PersistentDataDir string   `yaml:"persistentDataDir"`
	NodeAddr          string   `yaml:"nodeAddr"`
	ClusterNodesAddr  []string `yaml:"clusterNodesAddr"`
}

func ReadConfig() *Config {
	path := os.Getenv(configEnvVar)
	if path == "" {
		path = configFileName
	}

	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		panic(fmt.Sprintf("failed to read config %s: %v", path, err))
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic(fmt.Sprintf("failed to parse config %s: %v", path, err))
	}

	if cfg.NodeAddr == "" {
		cfg.NodeAddr = "127.0.0.1:4001"
	}
	if cfg.ClusterNodesAddr == nil {
		cfg.ClusterNodesAddr = []string{}
	}
	if cfg.FSMDataDir == "" {
		cfg.FSMDataDir = "./data/fsm"
	}
	if cfg.PersistentDataDir == "" {
		cfg.PersistentDataDir = "./data/persistent"
	}

	return &cfg
}
