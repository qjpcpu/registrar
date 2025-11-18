package zk

import (
	"errors"
	"fmt"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

var (
	ErrShutdown = errors.New("registrar shutdown")
)

type RoleType int

const (
	Follower RoleType = iota
	Leader
)

const base_key = `/services/ergo`

func (r RoleType) String() string {
	if r == Leader {
		return "LEADER"
	}
	return "FOLLOWER"
}

type client struct {
	nodeDisc *NodeDiscovery
	appDisc  *AppRouteDiscovery
	shutdown *CloseChan
	conn     zkConn
}

func Create(endpoints []string, opts ...Option) (gen.Registrar, error) {
	zkCfg := makeConfig(endpoints, opts...)
	shutdownCh := NewCloseChan()
	aEvent := NewAtomicValue(gen.Event{})
	aEventRef := NewAtomicValue(gen.Ref{})
	nodeDisc := &NodeDiscovery{
		RootZnode:              buildZnode(base_key, zkCfg.Cluster, "nodes"),
		Log:                    log_prefix(zkCfg.LogFn, "(registrar/nodes) "),
		Routes:                 NewAtomicValue([]gen.Route{}),
		AdvRoutes:              NewAtomicValue([]gen.Route{}),
		RoutesMapper:           zkCfg.RoutesMapper,
		Shutdown:               shutdownCh,
		RoleChangedListener:    zkCfg.RoleChanged,
		Event:                  aEvent,
		EventRef:               aEventRef,
		ExcludeSelfWhenResolve: zkCfg.ExcludeSelfWhenResolve,

		self:            &Node{},
		role:            Follower,
		roleChangedChan: make(chan RoleType, 1),
		reWatch:         make(chan struct{}, 1),
		eventsCh:        make(chan fmt.Stringer, 8),
	}
	appDisc := &AppRouteDiscovery{
		RootZnode:              buildZnode(base_key, zkCfg.Cluster, "apps"),
		Log:                    log_prefix(zkCfg.LogFn, "(registrar/apps) "),
		Shutdown:               shutdownCh,
		Event:                  aEvent,
		EventRef:               aEventRef,
		ExcludeSelfWhenResolve: zkCfg.ExcludeSelfWhenResolve,

		myappsChanged:          make(chan struct{}, 8),
		allAppRoutes:           NewAtomicValue(EmptyAppRoutesMap()),
		reWatch:                make(chan struct{}, 1),
		eventsCh:               make(chan fmt.Stringer, 8),
	}
	p := &client{
		nodeDisc: nodeDisc,
		appDisc:  appDisc,
		shutdown: shutdownCh,
	}
	conn, err := connectZk(endpoints, zkCfg.SessionTimeout,
		zkCfg.GetZKOptions(zk.WithEventCallback(multiOnEvent(nodeDisc.OnEvent, appDisc.OnEvent)))...)
	if err != nil {
		return nil, err
	}
	if auth := zkCfg.Auth; !auth.isEmpty() {
		if err = conn.AddAuth(auth.Scheme, []byte(auth.Credential)); err != nil {
			return nil, err
		}
	}
	p.nodeDisc.Conn = conn
	p.appDisc.Conn = conn
	p.conn = conn

	return p, nil
}

func (p *client) Shutdown() (err error) {
	p.shutdown.Close(func() {
		err0 := p.nodeDisc.Stop()
		if err0 != nil {
			err = err0
		}
		err0 = p.appDisc.Stop()
		if err0 != nil {
			err = err0
		}
		p.conn.Close()
	})
	return
}
