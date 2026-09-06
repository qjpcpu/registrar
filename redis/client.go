package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"ergo.services/ergo/gen"
	goredis "github.com/redis/go-redis/v9"
)

var ErrShutdown = errors.New("registrar shutdown")
var ErrNotSynchronized = errors.New("registrar has not synchronized")

type client struct {
	options         Options
	store           *store
	id              string
	ctx             context.Context
	cancel          context.CancelFunc
	wake            chan struct{}
	done            chan struct{}
	lifecycle       sync.Mutex
	stopped         bool
	started         bool
	node            gen.NodeRegistrar
	event           gen.Event
	eventRef        gen.Ref
	mu              sync.RWMutex
	local           registration
	apps            map[gen.Atom]gen.ApplicationRoute
	members         map[gen.Atom]member
	appRoutes       map[gen.Atom][]gen.ApplicationRoute
	leader          gen.Atom
	isLeader        bool
	snapshotVersion string
	lastSnapshot    []member
	syncErr         error
	lastSync        time.Time
}

func Create(options Options) (gen.Registrar, error) {
	if options.Cluster == "" {
		options.Cluster = "default"
	}
	if strings.ContainsAny(options.Cluster, "{}") {
		return nil, fmt.Errorf("cluster name must not contain { or }")
	}
	if options.SessionTimeout == 0 {
		options.SessionTimeout = 10 * time.Second
	}
	if options.PollInterval == 0 {
		options.PollInterval = time.Second
	}
	if options.SessionTimeout < time.Millisecond || options.PollInterval < 0 {
		return nil, fmt.Errorf("session timeout must be at least 1ms and poll interval must be positive")
	}
	if options.MasterName != "" && options.RedisCluster {
		return nil, fmt.Errorf("MasterName and RedisCluster are mutually exclusive")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	// Context deadlines bound each operation, including retries during failover.
	common := goredis.UniversalOptions{
		Addrs: options.Endpoints, MasterName: options.MasterName,
		DB:       options.DB,
		Username: options.Username, Password: options.Password,
		ContextTimeoutEnabled: true,
	}
	var rdb goredis.UniversalClient
	switch {
	case options.MasterName != "":
		rdb = goredis.NewFailoverClient(common.Failover())
	case options.RedisCluster:
		rdb = goredis.NewClusterClient(common.Cluster())
	default:
		rdb = goredis.NewClient(common.Simple())
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &client{options: options, store: newStore(rdb, options.Cluster), id: hex.EncodeToString(raw[:]), ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), done: make(chan struct{}), apps: make(map[gen.Atom]gen.ApplicationRoute), members: make(map[gen.Atom]member), appRoutes: make(map[gen.Atom][]gen.ApplicationRoute)}, nil
}

func (c *client) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := c.options.SessionTimeout * 3 / 10
	if timeout > time.Second {
		timeout = time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.stopped {
		return gen.StaticRoutes{}, ErrShutdown
	}
	if c.started {
		return gen.StaticRoutes{}, nil
	}
	if len(routes.Routes) == 0 {
		return gen.StaticRoutes{}, gen.ErrNoRoute
	}
	event := gen.Event{Name: gen.Atom("/ergo/" + c.options.Cluster + "/nodes"), Node: node.Name()}
	ref, err := node.RegisterEvent(event.Name, gen.EventOptions{Buffer: 64})
	if err != nil {
		return gen.StaticRoutes{}, err
	}
	c.mu.Lock()
	c.node, c.event, c.eventRef = node, event, ref
	c.local = registration{Name: node.Name(), Routes: append([]gen.Route(nil), routes.Routes...)}
	if c.options.SupportRegisterApplication {
		for _, app := range routes.ApplicationRoutes {
			c.apps[app.Name] = cloneApp(app)
		}
	}
	c.mu.Unlock()
	if err = c.publish(); err == nil {
		err = c.refresh()
	}
	if err != nil {
		ctx, cancel := c.operationContext(context.Background())
		_ = c.store.remove(ctx, c.id)
		cancel()
		_ = node.UnregisterEvent(event.Name)
		c.cancel()
		_ = c.store.client.Close()
		c.stopped = true
		return gen.StaticRoutes{}, err
	}
	c.started = true
	go c.run()
	return gen.StaticRoutes{}, nil
}

func (c *client) publish() error {
	c.mu.RLock()
	record := c.local
	for _, app := range c.apps {
		record.Apps = append(record.Apps, app)
	}
	c.mu.RUnlock()
	ctx, cancel := c.operationContext(c.ctx)
	defer cancel()
	return c.store.write(ctx, c.id, record, c.options.SessionTimeout)
}

func (c *client) refresh() (refreshErr error) {
	defer func() {
		c.mu.Lock()
		c.syncErr = refreshErr
		if refreshErr == nil {
			c.lastSync = time.Now()
		}
		c.mu.Unlock()
	}()
	ctx, cancel := c.operationContext(c.ctx)
	members, version, changed, err := c.store.snapshotSince(ctx, c.snapshotVersion)
	cancel()
	if err != nil {
		return err
	}
	if !changed {
		c.mu.RLock()
		synchronized := c.leader != ""
		c.mu.RUnlock()
		if synchronized {
			return nil
		}
		members = c.lastSnapshot
	}
	found := false
	for _, m := range members {
		if m.ID == c.id {
			found = true
			break
		}
	}
	if !found {
		if err := c.publish(); err != nil {
			return err
		}
		ctx, cancel := c.operationContext(c.ctx)
		members, version, _, err = c.store.snapshotSince(ctx, "")
		cancel()
		if err != nil {
			return err
		}
	}
	c.snapshotVersion, c.lastSnapshot = version, members
	c.applySnapshot(members)
	return nil
}

func (c *client) run() {
	defer close(c.done)
	poll := time.NewTicker(c.options.PollInterval)
	renew := time.NewTicker(c.options.SessionTimeout * 3 / 10)
	defer poll.Stop()
	defer renew.Stop()
	dirty := false
	for {
		var err error
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
			// Publish the latest application state on the next poll/renew tick.
			dirty = true
		case <-renew.C:
			err = c.publish()
			if err == nil {
				dirty = false
				err = c.refresh()
			}
		case <-poll.C:
			if dirty {
				err = c.publish()
				if err == nil {
					dirty = false
				}
			}
			if err == nil {
				err = c.refresh()
			}
		}
		if err != nil && c.ctx.Err() == nil {
			c.mu.Lock()
			c.syncErr = err
			c.mu.Unlock()
			c.demote()
			c.node.Log().Error("(registrar/redis) synchronization failed: %v", err)
		}
	}
}

func (c *client) Terminate() {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	c.cancel()
	if c.started {
		<-c.done
		ctx, cancel := c.operationContext(context.Background())
		if err := c.store.remove(ctx, c.id); err != nil {
			c.node.Log().Error("(registrar/redis) unregister failed: %v", err)
		}
		cancel()
		c.demote()
		_ = c.node.UnregisterEvent(c.event.Name)
	}
	_ = c.store.client.Close()
}
