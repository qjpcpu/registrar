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

	// RoutesMapper is a function to modify the node's route information before it is published.
	// This is useful for handling NAT traversal or specifying a broadcast address in containerized
	// environments (e.g., Docker, Kubernetes).
	RoutesMapper RoutesMapper

	// ZkOptions allows passing native go-zk library connection options for advanced configuration.
	ZkOptions []zk.ConnOption

	// SupportRegisterApplication enables the registration of application routes for this node.
	SupportRegisterApplication bool
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

type RoutesMapper func([]gen.Route) []gen.Route

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
