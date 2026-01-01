package zk

import (
	"ergo.services/ergo/gen"
)

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	value, ok := c.nodeDisc.Members.Load(name)
	if !ok {
		return nil, gen.ErrNoRoute
	}
	return value.(*Node).Routes, nil
}

// ResolveApplication returns all known routes for a given application name
func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	if !c.options.SupportRegisterApplication {
		return nil, gen.ErrUnsupported
	}
	routes := c.appDisc.ResolveApp(name)
	if len(routes) > 0 {
		return routes, nil
	}
	return nil, gen.ErrNoRoute
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	// Proxy routing is not supported in zk registrar implementation
	return nil, gen.ErrNoRoute
}
