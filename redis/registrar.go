package redis

import (
	"sort"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/constants"
)

const (
	RegistrarVersion = "R1"
	RegistrarName    = "Redis Client"
)

var _ gen.Registrar = (*client)(nil)
var _ gen.Resolver = (*client)(nil)

func (c *client) Resolver() gen.Resolver                          { return c }
func (c *client) RegisterProxy(gen.Atom) error                    { return gen.ErrUnsupported }
func (c *client) UnregisterProxy(gen.Atom) error                  { return gen.ErrUnsupported }
func (c *client) ResolveProxy(gen.Atom) ([]gen.ProxyRoute, error) { return nil, gen.ErrNoRoute }
func (c *client) Config(...string) (map[string]any, error)        { return nil, gen.ErrUnsupported }

func (c *client) ConfigItem(item string) (any, error) {
	if item != constants.LeaderNodeConfigItem {
		return nil, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader, nil
}

func (c *client) Event() (gen.Event, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.event, nil
}
func (c *client) Info() gen.RegistrarInfo {
	return gen.RegistrarInfo{Version: c.Version(), SupportEvent: true, SupportRegisterApplication: c.options.SupportRegisterApplication}
}
func (c *client) Version() gen.Version {
	return gen.Version{Name: RegistrarName, Release: RegistrarVersion, License: gen.LicenseMIT}
}

func (c *client) Nodes() ([]gen.Atom, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var nodes []gen.Atom
	for name := range c.members {
		if name != c.local.Name {
			nodes = append(nodes, name)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes, nil
}
func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.members[name]
	if !ok {
		return nil, gen.ErrNoRoute
	}
	return append([]gen.Route(nil), m.Routes...), nil
}
func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	if !c.options.SupportRegisterApplication {
		return nil, gen.ErrUnsupported
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	routes := c.appRoutes[name]
	if len(routes) == 0 {
		return nil, gen.ErrNoRoute
	}
	result := make([]gen.ApplicationRoute, len(routes))
	for i, app := range routes {
		result[i] = cloneApp(app)
	}
	return result, nil
}
func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	if !c.options.SupportRegisterApplication {
		return gen.ErrUnsupported
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.stopped {
		return ErrShutdown
	}
	c.mu.Lock()
	c.apps[route.Name] = cloneApp(route)
	c.mu.Unlock()
	c.signal()
	return nil
}
func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	if !c.options.SupportRegisterApplication {
		return gen.ErrUnsupported
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.stopped {
		return ErrShutdown
	}
	c.mu.Lock()
	delete(c.apps, name)
	c.mu.Unlock()
	c.signal()
	return nil
}
func (c *client) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
