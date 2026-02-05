package zk

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/constants"
)

const (
	RegistrarVersion = "R1"
	RegistrarName    = "ZK Client"
)

// gen.Registrar interface implementation
func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	advRoutes := c.nodeDisc.RegisterRoutes(routes.Routes)
	if len(advRoutes) == 0 {
		return gen.StaticRoutes{}, gen.ErrNoRoute
	}

	c.nodeDisc.Node = node
	c.appDisc.Node = node
	eventName := gen.Atom(c.nodeDisc.RootZnode)
	eventRef, err := node.RegisterEvent(eventName, gen.EventOptions{Buffer: 64})
	if err != nil {
		return gen.StaticRoutes{}, err
	}
	c.nodeDisc.Event.Store(gen.Event{Name: eventName, Node: node.Name()})
	c.nodeDisc.EventRef.Store(eventRef)
	if err := c.nodeDisc.StartMember(); err != nil {
		return gen.StaticRoutes{}, err
	}
	if c.options.SupportRegisterApplication {
		if err := c.appDisc.StartMember(); err != nil {
			return gen.StaticRoutes{}, err
		}
		c.appDisc.RegisterApplicationRoutes(routes.ApplicationRoutes...)
	}
	return gen.StaticRoutes{}, nil
}

func (c *client) Resolver() gen.Resolver {
	return c
}

func (c *client) RegisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}

func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	if !c.options.SupportRegisterApplication {
		return gen.ErrUnsupported
	}
	return c.appDisc.RegisterApplicationRoutes(route)
}

func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	if !c.options.SupportRegisterApplication {
		return gen.ErrUnsupported
	}
	return c.appDisc.UnregisterApplicationRoute(name)
}

// Nodes returns a list of all discovered nodes exclude self in the cluster
func (c *client) Nodes() ([]gen.Atom, error) {
	return c.nodeDisc.ResolveNodes(), nil
}

func (c *client) ConfigItem(item string) (any, error) {
	if item == constants.LeaderNodeConfigItem {
		return c.nodeDisc.GetLeader(), nil
	}
	return nil, nil
}

func (c *client) Config(items ...string) (map[string]any, error) {
	return nil, gen.ErrUnsupported
}

func (c *client) Event() (gen.Event, error) {
	return c.nodeDisc.Event.Load(), nil
}

func (c *client) Info() gen.RegistrarInfo {
	return gen.RegistrarInfo{
		EmbeddedServer:             false,
		Version:                    c.Version(),
		SupportConfig:              false,
		SupportEvent:               true,
		SupportRegisterProxy:       false,
		SupportRegisterApplication: c.options.SupportRegisterApplication,
	}
}

func (c *client) Version() gen.Version {
	return gen.Version{
		Name:    RegistrarName,
		Release: RegistrarVersion,
		License: gen.LicenseMIT,
	}
}

func (c *client) Terminate() {
	c.Shutdown()
}
