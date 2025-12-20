package main

type Config struct {
	NodeAddr         string   `yaml:"nodeAddr"`
	ClusterNodesAddr []string `yaml:"clusterNodesAddr"`
}
