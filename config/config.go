package config

type Raft struct {
	NodeAddr         string   `yaml:"nodeAddr"`
	ClusterNodesAddr []string `yaml:"clusterNodesAddr"`
}
