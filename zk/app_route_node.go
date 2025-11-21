package zk

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"ergo.services/ergo/gen"
)

type AppRouteNode struct {
	Name   gen.Atom `json:"app"`
	Node   gen.Atom `json:"node"`
	Weight int
	Mode   int
	State  int32
	Meta   sync.Map `json:"-"`
}

func NewAppRouteNode(route gen.ApplicationRoute) *AppRouteNode {
	return &AppRouteNode{
		Name:   route.Name,
		Node:   route.Node,
		Weight: route.Weight,
		Mode:   int(route.Mode),
		State:  int32(route.State),
	}
}

func (n *AppRouteNode) ToApplicationRoute() gen.ApplicationRoute {
	return gen.ApplicationRoute{
		Name:   n.Name,
		Node:   n.Node,
		Weight: n.Weight,
		Mode:   gen.ApplicationMode(n.Mode),
		State:  gen.ApplicationState(n.State),
	}
}

func (n *AppRouteNode) UniqueId() string {
	return fmt.Sprintf("%s-%s", string(n.Name), string(n.Node))
}

func (n *AppRouteNode) Equal(other *AppRouteNode) bool {
	if n == nil || other == nil {
		return false
	}
	if n == other {
		return true
	}
	return n.Name == other.Name &&
		n.Node == other.Node &&
		n.Weight == other.Weight &&
		n.Mode == other.Mode &&
		n.State == other.State
}

func (n *AppRouteNode) GetMeta(name string) (string, bool) {
	if value, ok := n.Meta.Load(name); ok {
		return value.(string), true
	}
	return "", false
}

func (n *AppRouteNode) GetSeq() int {
	if seqStr, ok := n.GetMeta(metaKeySeq); ok {
		return strToInt(seqStr)
	}
	return 0
}

func (n *AppRouteNode) SetPath(val string) {
	n.SetMeta("znode_path", val)
}

func (n *AppRouteNode) GetPath() string {
	v, _ := n.GetMeta("znode_path")
	return v
}

func (n *AppRouteNode) SetMeta(name string, val string) {
	n.Meta.Store(name, val)
}

func (n *AppRouteNode) Serialize() ([]byte, error) {
	data, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (n *AppRouteNode) Deserialize(data []byte) error {
	return json.Unmarshal(data, n)
}

type AppRoutesMapBuilder AppRoutesMap

func NewAppRoutesMapBuilder() *AppRoutesMapBuilder {
	m := AppRoutesMapBuilder(EmptyAppRoutesMap())
	return &m
}

func (m *AppRoutesMapBuilder) Set(n *AppRouteNode) {
	inner, ok := m.data[n.Name]
	if !ok {
		inner = make(map[gen.Atom]gen.ApplicationRoute)
		m.data[n.Name] = inner
	}
	inner[n.Node] = n.ToApplicationRoute()
}

func (m *AppRoutesMapBuilder) Build(self gen.Atom) AppRoutesMap {
	for app, inner := range m.data {
		routes := make([]gen.ApplicationRoute, 0, len(inner))
		for _, route := range inner {
			if route.Node != self {
				routes = append(routes, route)
			}
		}
		sort.SliceStable(routes, func(i, j int) bool {
			if routes[i].Name != routes[j].Name {
				return routes[i].Name < routes[j].Name
			}
			return routes[i].Node < routes[j].Node
		})
		m.excludeSelfRoutes[app] = routes
	}
	return AppRoutesMap(*m)
}

type AppRoutesMap struct {
	data              map[gen.Atom]map[gen.Atom]gen.ApplicationRoute
	excludeSelfRoutes map[gen.Atom][]gen.ApplicationRoute
}

func EmptyAppRoutesMap() AppRoutesMap {
	return AppRoutesMap{
		data:              make(map[gen.Atom]map[gen.Atom]gen.ApplicationRoute),
		excludeSelfRoutes: make(map[gen.Atom][]gen.ApplicationRoute),
	}
}

func (m AppRoutesMap) GetRoutesExcludeSelf(app gen.Atom) (routes []gen.ApplicationRoute) {
	return m.excludeSelfRoutes[app]
}

func (m AppRoutesMap) GetRoute(app, node gen.Atom) (gen.ApplicationRoute, bool) {
	inner, ok := m.data[app]
	if !ok {
		return gen.ApplicationRoute{}, false
	}
	val, ok := inner[node]
	return val, ok
}

func (m AppRoutesMap) Range(visit func(gen.Atom, gen.Atom, gen.ApplicationRoute) bool) {
	for app, inner := range m.data {
		for node, route := range inner {
			if !visit(app, node, route) {
				return
			}
		}
	}
}
