package zk

import (
	"time"

	"ergo.services/ergo/gen"
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

	// RoleChanged is a listener that is triggered when the node's role (Leader/Follower) changes.
	// This is particularly useful for implementing actors that should only run on the leader node.
	RoleChanged RoleChangedListener

	// RoutesMapper is a function to modify the node's route information before it is published.
	// This is useful for handling NAT traversal or specifying a broadcast address in containerized
	// environments (e.g., Docker, Kubernetes).
	RoutesMapper RoutesMapper

	// LogFn is a custom logging function for internal registrar output. Defaults to a silent logger.
	LogFn LogFn

	// ZkOptions allows passing native go-zk library connection options for advanced configuration.
	ZkOptions []zk.ConnOption
}

// MapRoutesByAdvertiseAddress sets the advertised IP address and port for the node.
func MapRoutesByAdvertiseAddress(ip string, port int) RoutesMapper {
	return RoutesMapper(func(routes []gen.Route) []gen.Route {
		if len(routes) == 0 {
			return routes
		}
		return []gen.Route{
			{
				Host:             ip,
				Port:             uint16(port),
				TLS:              routes[0].TLS,
				HandshakeVersion: routes[0].HandshakeVersion,
				ProtoVersion:     routes[0].ProtoVersion,
			},
		}
	})
}

type AuthConfig struct {
	Scheme     string
	Credential string
}

func (za AuthConfig) isEmpty() bool {
	return za.Scheme == "" && za.Credential == ""
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

type RoleChangedListener interface {
	OnRoleChanged(RoleType)
}

type OnRoleChangedFunc func(RoleType)

func (fn OnRoleChangedFunc) OnRoleChanged(rt RoleType) {
	fn(rt)
}

func (o Options) getZKOptions(opts ...zk.ConnOption) []zk.ConnOption {
	options := []zk.ConnOption{
		zk.WithLogger(log_prefix(o.LogFn, "(registrar/zk) ")),
	}
	return append(options, opts...)
}

type RoutesMapper func([]gen.Route) []gen.Route
