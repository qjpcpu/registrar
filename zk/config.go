package zk

import (
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

type Option func(*config)

// WithAuth sets the ZooKeeper authentication credentials.
func WithAuth(scheme string, credential string) Option {
	return func(o *config) {
		o.Auth = authConfig{Scheme: scheme, Credential: credential}
	}
}

// WithCluster sets the cluster name.
func WithCluster(key string) Option {
	return func(o *config) {
		if !isStrBlank(key) {
			o.Cluster = key
		}
	}
}

// ExcludeSelfWhenResolve whether exclude self for registrar.Nodes resolver.ResolveApplication, default is false.
func ExcludeSelfWhenResolve(exclude bool) Option {
	return func(o *config) {
		o.ExcludeSelfWhenResolve = exclude
	}
}

// WithCluster sets the logger.
func WithLogger(fn LogFn) Option {
	return func(o *config) {
		if fn != nil {
			o.LogFn = fn
		} else {
			o.LogFn = fn_mutelog
		}
	}
}

// WithSessionTimeout sets the ZooKeeper session timeout.
func WithSessionTimeout(tm time.Duration) Option {
	return func(o *config) {
		o.SessionTimeout = tm
	}
}

// WithAdvertiseAddress sets the advertised IP address and port for the node.
func WithAdvertiseAddress(ip string, port int) Option {
	return WithRoutesMapper(func(routes []gen.Route) []gen.Route {
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

// WithAdvertiseIP sets the advertised IP address for the node.
func WithAdvertiseIP(ip string) Option {
	return WithRoutesMapper(func(routes []gen.Route) []gen.Route {
		if len(routes) == 0 {
			return routes
		}
		return []gen.Route{
			{
				Host:             ip,
				Port:             routes[0].Port,
				TLS:              routes[0].TLS,
				HandshakeVersion: routes[0].HandshakeVersion,
				ProtoVersion:     routes[0].ProtoVersion,
			},
		}
	})
}

// WithAdvertisePort sets the advertised port for the node.
func WithAdvertisePort(port int) Option {
	return WithRoutesMapper(func(routes []gen.Route) []gen.Route {
		if len(routes) == 0 {
			return routes
		}
		return []gen.Route{
			{
				Host:             routes[0].Host,
				Port:             uint16(port),
				TLS:              routes[0].TLS,
				HandshakeVersion: routes[0].HandshakeVersion,
				ProtoVersion:     routes[0].ProtoVersion,
			},
		}
	})
}

// WithRoutesMapper provides a function to modify the routes before they are registered.
func WithRoutesMapper(m RoutesMapper) Option {
	return func(o *config) {
		o.RoutesMapper = m
	}
}

// WithRoleChangedListener sets a listener that is triggered when the node's role changes.
func WithRoleChangedListener(l RoleChangedListener) Option {
	return func(o *config) {
		o.RoleChanged = l
	}
}

// WithRoleChangedFunc sets a function to be called when the node's role changes.
func WithRoleChangedFunc(f OnRoleChangedFunc) Option {
	return func(o *config) {
		o.RoleChanged = f
	}
}

// WithZKHostProvider sets a dynamic host provider for ZooKeeper.
func WithZKOption(setting zk.ConnOption) Option {
	return func(o *config) {
		if setting != nil {
			o.zkOptions = append(o.zkOptions, setting)
		}
	}
}

type authConfig struct {
	Scheme     string
	Credential string
}

func (za authConfig) isEmpty() bool {
	return za.Scheme == "" && za.Credential == ""
}

type RoleChangedListener interface {
	OnRoleChanged(RoleType)
}

type OnRoleChangedFunc func(RoleType)

func (fn OnRoleChangedFunc) OnRoleChanged(rt RoleType) {
	fn(rt)
}

type config struct {
	Cluster                string
	Endpoints              []string
	SessionTimeout         time.Duration
	Auth                   authConfig
	RoleChanged            RoleChangedListener
	RoutesMapper           RoutesMapper
	LogFn                  LogFn
	ExcludeSelfWhenResolve bool
	zkOptions              []zk.ConnOption
}

func makeConfig(eds []string, options ...Option) *config {
	c := &config{
		Endpoints:      eds,
		Cluster:        "default",
		SessionTimeout: time.Second * 10,
		LogFn:          fn_mutelog,
	}
	for _, fn := range options {
		fn(c)
	}
	return c
}

func (c *config) GetZKOptions(opts ...zk.ConnOption) []zk.ConnOption {
	options := []zk.ConnOption{
		zk.WithLogger(log_prefix(c.LogFn, "(registrar/zk) ")),
	}
	return append(options, opts...)
}

type RoutesMapper func([]gen.Route) []gen.Route
