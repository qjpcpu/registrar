package zk

import (
	"errors"
	"fmt"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

var (
	ErrShutdown = errors.New("registrar shutdown")
)

const (
	ZkRoot = `/ergo`
	// maxSelfHealRetries is the maximum number of consecutive re-registration
	// attempts when the node's own znode is not found in the cluster.
	maxSelfHealRetries = 1024
)

type client struct {
	options  Options
	nodeDisc *NodeDiscovery
	appDisc  *AppRouteDiscovery
	shutdown *CloseChan
	conn     zkConn
}

func Create(options Options) (gen.Registrar, error) {
	p := newClient(&options)
	conn, err := connectToZooKeeper(&options, p)
	if err != nil {
		return nil, err
	}
	p.nodeDisc.Conn = conn
	p.appDisc.Conn = conn
	p.conn = conn
	p.options = options

	return p, nil
}

func newClient(options *Options) *client {
	if options.Cluster == "" {
		options.Cluster = "default"
	}
	if options.SessionTimeout == 0 {
		options.SessionTimeout = time.Second * 10
	}
	shutdownCh := NewCloseChan()
	aEvent := NewAtomicValue(gen.Event{})
	aEventRef := NewAtomicValue(gen.Ref{})
	nodeDisc := &NodeDiscovery{
		RootZnode: buildZnode(ZkRoot, options.Cluster, "nodes"),
		Routes:    NewAtomicValue([]gen.Route{}),
		Shutdown:  shutdownCh,
		Event:     aEvent,
		EventRef:  aEventRef,

		self:            &Node{},
		fullpath:        NewAtomicValue[string](),
		role:            Follower,
		roleChangedChan: make(chan RoleType, 1),
		reWatch:         make(chan struct{}, 1),
		eventsCh:        make(chan fmt.Stringer, 8),
		leaderNode:      NewAtomicValue[gen.Atom](),
	}
	appDisc := &AppRouteDiscovery{
		RootZnode: buildZnode(ZkRoot, options.Cluster, "apps"),
		Shutdown:  shutdownCh,
		Event:     aEvent,
		EventRef:  aEventRef,

		myappsChanged: make(chan struct{}, 8),
		allAppRoutes:  NewAtomicValue(EmptyAppRoutesMap()),
		reWatch:       make(chan struct{}, 1),
		eventsCh:      make(chan fmt.Stringer, 8),
	}
	return &client{
		nodeDisc: nodeDisc,
		appDisc:  appDisc,
		shutdown: shutdownCh,
	}
}

func connectToZooKeeper(options *Options, c *client) (zkConn, error) {
	zkconn, _, err := zk.Connect(
		options.Endpoints,
		options.SessionTimeout,
		options.getZKOptions(c.nodeDisc, zk.WithEventCallback(multiOnEvent(c.nodeDisc.OnEvent, c.appDisc.OnEvent)))...)
	if err != nil {
		return nil, err
	}
	conn := &zkConnImpl{conn: zkconn}
	if auth := options.Auth; !auth.isEmpty() {
		if err = conn.AddAuth(auth.Scheme, []byte(auth.Credential)); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *client) Shutdown() (err error) {
	c.shutdown.Close(func() {
		// shutdown channel is already closed at this point,
		// watch goroutines will detect it and exit.
		var errs []error
		if err0 := c.nodeDisc.Stop(); err0 != nil {
			errs = append(errs, err0)
		}
		if c.options.SupportRegisterApplication {
			if err0 := c.appDisc.Stop(); err0 != nil {
				errs = append(errs, err0)
			}
		}
		c.conn.Close()
		err = errors.Join(errs...)
	})
	return
}
