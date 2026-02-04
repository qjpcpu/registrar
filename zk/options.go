package zk

import (
	"time"

	"github.com/qjpcpu/zk"
)

type Options struct {
	// Cluster is the name used to distinguish different clusters, acting as a namespace in ZooKeeper.
	// This allows multiple Ergo clusters to share the same ZooKeeper instance. Defaults to "default".
	Cluster string
	// Endpoints is a list of ZooKeeper server endpoints (e.g., "127.0.0.1:2181").
	Endpoints []string

	// SessionTimeout is the session timeout for the ZooKeeper client.
	// If the session is inactive for this duration, ephemeral nodes created by this session
	// will be automatically deleted. Defaults to 10 seconds.
	SessionTimeout time.Duration
	// Auth provides authentication credentials for accessing ZooKeeper.
	Auth AuthConfig

	// ZkOptions allows passing native go-zk library connection options for advanced configuration.
	ZkOptions []zk.ConnOption

	// SupportRegisterApplication enables the registration of application routes for this node.
	SupportRegisterApplication bool
}

type AuthConfig struct {
	Scheme     string
	Credential string
}

func (za AuthConfig) isEmpty() bool {
	return za.Scheme == "" && za.Credential == ""
}

func (o Options) getZKOptions(nd *NodeDiscovery, opts ...zk.ConnOption) []zk.ConnOption {
	logger := LogFn(func(s string, a ...any) {
		if node := nd.Node; node != nil {
			node.Log().Debug(`(registrar/zk) `+s, a...)
		}
	})
	options := []zk.ConnOption{
		zk.WithLogger(logger),
	}
	return append(options, opts...)
}

type RoleType int

const (
	Follower RoleType = iota
	Leader
)

func (r RoleType) String() string {
	if r == Leader {
		return "LEADER"
	}
	return "FOLLOWER"
}
