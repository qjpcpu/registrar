package zk

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

// AppRouteDiscovery is responsible for the registration and discovery of application routes.
// Each application route is published as an ephemeral, sequential znode in ZooKeeper.
// This component watches for changes in ZK to discover routes from other nodes and maintains
// a complete, up-to-date map of all application routes in the cluster. It also allows for
// dynamic registration and unregistration of routes for the local node.
type AppRouteDiscovery struct {
	Conn zkConn
	Node gen.NodeRegistrar

	/* options */
	RootZnode              string
	Log                    LogFn
	Shutdown               *CloseChan
	Event                  *AtomicValue[gen.Event]
	EventRef               *AtomicValue[gen.Ref]
	ExcludeSelfWhenResolve bool

	/* state fields */
	myapps        sync.Map                   // Stores the application routes registered by this specific node. Key: gen.Atom(AppName), Value: *AppRouteNode.
	myappsChanged chan struct{}              // A channel to signal that the set of local applications (`myapps`) has changed.
	allAppRoutes  *AtomicValue[AppRoutesMap] // A cache of all application routes from all nodes in the cluster.
	revision      uint64                     // The cversion of the root znode, used to detect changes.
	reWatch       chan struct{}              // A channel to trigger a re-watch, typically after a ZK session reconnect.
	eventsCh      chan fmt.Stringer          // A channel for sending application lifecycle events.
	started       bool
	startMu       sync.Mutex
}

// ResolveApp returns all known routes for a given application name from the local cache.
func (ard *AppRouteDiscovery) ResolveApp(name gen.Atom) []gen.ApplicationRoute {
	if ard.ExcludeSelfWhenResolve {
		return ard.allAppRoutes.Load().GetRoutesExcludeSelf(name)
	}
	return ard.allAppRoutes.Load().GetRoutes(name)
}

// RegisterApplicationRoutes adds or updates application routes for the current node.
// If any route is new or has changed, it signals the watch loop to re-evaluate and
// potentially re-register the routes in ZooKeeper.
func (ard *AppRouteDiscovery) RegisterApplicationRoutes(routes ...gen.ApplicationRoute) (err error) {
	var changed bool

	for _, item := range routes {
		route := NewAppRouteNode(item)
		if value, ok := ard.myapps.Load(item.Name); ok {
			if !route.Equal(value.(*AppRouteNode)) {
				ard.myapps.Store(item.Name, route)
				changed = true
			}
		} else {
			// This is a new application, so it's a change.
			ard.myapps.Store(item.Name, route)
			changed = true
		}
	}
	if changed {
		ard.myappsChanged <- struct{}{}
	}
	return nil
}

// UnregisterApplicationRoute removes an application route for the current node.
// It signals the watch loop to handle the change.
func (ard *AppRouteDiscovery) UnregisterApplicationRoute(name gen.Atom) error {
	if _, loaded := ard.myapps.LoadAndDelete(name); loaded {
		// The map was changed, notify the watcher.
		ard.myappsChanged <- struct{}{}
	}
	return nil
}

// StartMember initializes the discovery service.
// It ensures the root znode exists, registers the initial set of application routes,
// fetches all existing routes from the cluster, and starts the watch loop.
func (ard *AppRouteDiscovery) StartMember() error {
	ard.startMu.Lock()
	defer ard.startMu.Unlock()
	if ard.started {
		return nil
	}
	if err := ard.init(); err != nil {
		return err
	}

	ard.startEventNotifyLoop()

	// register self
	// This creates ephemeral sequential znodes for each application route of the current node.
	if err := ard.registerService(); err != nil {
		ard.Log("register service fail " + err.Error())
		return err
	}

	// Fetch the initial list of all application routes from all nodes.
	nodes, version, err := ard.fetchNodes()
	if err != nil {
		ard.Log("fetch app routes fail %v", err)
		return err
	}

	ard.updateNodes(nodes, version)
	ard.startWatching()

	ard.started = true
	return nil
}

func (ard *AppRouteDiscovery) init() error {
	if err := ensurePersistentNode(ard.Conn, ard.RootZnode); err != nil {
		ard.Log("create dir node fail path=%s %v", ard.RootZnode, err)
		return err
	}
	return nil
}

// registerService iterates through all local applications (`myapps`) and registers each
// one as a separate znode in ZooKeeper. It also cleans up any stale znodes from previous registrations.
func (ard *AppRouteDiscovery) registerService() (err error) {
	ard.myapps.Range(func(key any, value any) bool {
		if err0 := ard.registerSingleAppRoute(value.(*AppRouteNode)); err0 != nil {
			err = err0
		}
		return true
	})
	if err != nil {
		return err
	}
	ard.removeDeprecatedAppRoute()
	return nil
}

// registerSingleAppRoute creates a single ephemeral sequential znode for a given application route.
// The znode data is the serialized AppRouteNode.
func (ard *AppRouteDiscovery) registerSingleAppRoute(node *AppRouteNode) error {
	data, err := node.Serialize()
	if err != nil {
		ard.Log("register app route serialize fail. %v", err)
		return err
	}

	path, err := createEphemeralSequentialChildNode(ard.Conn, ard.znodeTag(node.Node), ard.RootZnode, data)
	if err != nil {
		ard.Log("create child node fail. node=%s %v", ard.RootZnode, err)
		return err
	}
	seq, err := parseSeq(path)
	if err != nil {
		ard.Log("create child node fail. node=%s %v", ard.RootZnode, err)
		return err
	}
	node.SetMeta(metaKeySeq, intToStr(seq))
	node.SetPath(path)

	return nil
}

// znodeTag creates a ZK-friendly node name from the Ergo node name,
// which may contain characters not allowed in ZK paths (like '@').
func (ard *AppRouteDiscovery) znodeTag(node gen.Atom) string {
	sb := strings.Builder{}
	sb.WriteByte('_')
	for _, r := range string(node) {
		if r == '_' || r == '.' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') {
			sb.WriteRune(r)
		} else if r == '@' || r == '-' {
			sb.WriteRune('_')
		}
	}
	sb.WriteByte('_')
	return sb.String()
}

// removeDeprecatedAppRoute finds and deletes any znodes in ZooKeeper that belong to the current node
// but do not correspond to a currently registered application in `myapps`. This is a cleanup
// mechanism to remove stale registrations.
func (ard *AppRouteDiscovery) removeDeprecatedAppRoute() error {
	children, _, err := ard.Conn.Children(ard.RootZnode)
	if err != nil {
		return err
	}
	paths := make(map[string]int)

	ard.myapps.Range(func(key any, value any) bool {
		node := value.(*AppRouteNode)
		if path := node.GetPath(); path != "" {
			paths[path] = node.GetSeq()
		}
		return true
	})
	var deprecated_paths []string
	for _, child := range children {
		child = buildZnode(ard.RootZnode, child)
		if strings.Contains(child, ard.znodeTag(ard.Node.Name())) {
			seq0, _ := parseSeq(child)
			if seq, ok := paths[child]; !ok || seq0 < seq {
				deprecated_paths = append(deprecated_paths, child)
			}
		}
	}
	for _, path := range deprecated_paths {
		if err = ard.Conn.Delete(path, -1); err != nil {
			ard.Log("fail to remove deprecated znode %s %v", path, err)
		} else {
			ard.Log("remove deprecated znode %s OK", path)
		}
	}
	return nil
}

// fetchNodes retrieves all application route znodes from ZooKeeper, deserializes them,
// and returns a list of AppRouteNode objects.
func (ard *AppRouteDiscovery) fetchNodes() ([]*AppRouteNode, int32, error) {
	key := ard.RootZnode
	children, stat, err := ard.Conn.Children(key)
	if err != nil {
		ard.Log("fetch nodes fail. path=%s %v", key, err)
		return nil, 0, err
	}

	var nodes []*AppRouteNode
	for _, short := range children {
		long := filepath.Join(key, short)
		value, _, err := ard.Conn.Get(long)
		if err != nil {
			ard.Log("fetch nodes fail. path=%s %v", long, err)
			return nil, stat.Cversion, err
		}
		n := &AppRouteNode{}
		if err := n.Deserialize(value); err != nil {
			ard.Log("fetch nodes deserialize fail. path=%s val=%s %v", long, string(value), err)
			return nil, stat.Cversion, err
		}
		seq, err := parseSeq(long)
		if err != nil {
			ard.Log("fetch nodes parse seq fail. path=%s val=%s %v", long, string(value), err)
			return nil, 0, err
		} else {
			n.SetMeta(metaKeySeq, intToStr(seq))
		}
		n.SetPath(long)
		nodes = append(nodes, n)
	}
	return ard.uniqNodes(nodes), stat.Cversion, nil
}

// uniqNodes filters the list of nodes to ensure that for each unique application route
// (defined by app name and node name), only the one with the highest sequence number
// (the most recent registration) is kept.
func (ard *AppRouteDiscovery) uniqNodes(nodes []*AppRouteNode) []*AppRouteNode {
	nodeMap := make(map[string]*AppRouteNode)
	for _, node := range nodes {
		if n, ok := nodeMap[node.UniqueId()]; ok {
			// keep node with higher version
			if node.GetSeq() > n.GetSeq() {
				nodeMap[node.UniqueId()] = node
			}
		} else {
			nodeMap[node.UniqueId()] = node
		}
	}

	var out []*AppRouteNode
	for _, node := range nodeMap {
		out = append(out, node)
	}
	return out
}

// updateNodes processes a new list of application routes.
// It compares the new list with the old one (`allAppRoutes`) and generates application lifecycle
// events (Loaded, Started, Stopping, Stopped) for any detected changes. Finally, it updates
// the `allAppRoutes` cache with the new set.
func (ard *AppRouteDiscovery) updateNodes(members []*AppRouteNode, reversion int32) {
	publish := func(route gen.ApplicationRoute) {
		switch route.State {
		case gen.ApplicationStateLoaded:
			ard.eventsCh <- EventApplicationLoaded{Name: route.Name, Node: route.Node, Weight: route.Weight}
		case gen.ApplicationStateRunning:
			ard.eventsCh <- EventApplicationStarted{Name: route.Name, Node: route.Node, Weight: route.Weight, Mode: route.Mode}
		case gen.ApplicationStateStopping:
			ard.eventsCh <- EventApplicationStopping{Name: route.Name, Node: route.Node}
		}
	}
	oldMembers := ard.allAppRoutes.Load()
	newMembersBuilder := NewAppRoutesMapBuilder()
	for _, n := range members {
		newMembersBuilder.Set(n)
		if old, ok := oldMembers.GetRoute(n.Name, n.Node); ok {
			if !NewAppRouteNode(old).Equal(n) {
				publish(n.ToApplicationRoute())
			}
		} else {
			publish(n.ToApplicationRoute())
		}
	}
	newMembers := newMembersBuilder.Build(ard.Node.Name())
	oldMembers.Range(func(app gen.Atom, node gen.Atom, route gen.ApplicationRoute) bool {
		if _, ok := newMembers.GetRoute(app, node); !ok {
			ard.eventsCh <- EventApplicationStopped{Name: app, Node: node}
		}
		return true
	})
	ard.allAppRoutes.Store(newMembers)
	ard.revision = uint64(reversion)
}

// startWatching starts a background goroutine that continuously watches for changes
// in the ZooKeeper `RootZnode`.
func (ard *AppRouteDiscovery) startWatching() {
	ctx := context.TODO()
	go func() {
		for !ard.Shutdown.Closed() {
			if err := ard.keepWatching(ctx); err != nil {
				if err == ErrShutdown {
					return
				}
				if err != zk.ErrConnectionClosed {
					ard.Log("failed to keepWatching. %v", err)
				}
			}
		}
	}()
}

// keepWatching is the core of the watch loop. It sets a watch on the root znode's children,
// and then blocks waiting for an event.
func (ard *AppRouteDiscovery) keepWatching(ctx context.Context) error {
	evtChan, err := ard.addWatcher(ctx, ard.RootZnode)
	if err != nil {
		return err
	}

	return ard._keepWatching(evtChan)
}

// addWatcher sets a `ChildrenW` watch on the given ZK path.
// Before returning, it checks if the children have changed since the last check.
// If they have, it fetches the new list, updates the node state, and re-establishes the watch.
// This handles the case where changes happen between a watch event and setting a new watch.
func (ard *AppRouteDiscovery) addWatcher(ctx context.Context, clusterKey string) (<-chan zk.Event, error) {
	_, stat, evtChan, err := ard.Conn.ChildrenW(clusterKey)
	if err != nil {
		return nil, err
	}

	ard.Log("watching path=%s children=%d cversion=%d", clusterKey, int(stat.NumChildren), stat.Cversion)
	if !ard.isChildrenChanged(stat) {
		return evtChan, nil
	}

	ard.Log("chilren changed, wait 1 sec and watch again old_cversion=%d new_cversion=%d", int(ard.revision), int(stat.Cversion))
	time.Sleep(1 * time.Second)
	nodes, version, err := ard.fetchNodes()
	if err != nil {
		return nil, err
	}
	if !ard.containSelf(nodes) {
		if err = ard.registerService(); err != nil {
			return nil, err
		}
	}
	// initialize members
	ard.updateNodes(nodes, version)
	return ard.addWatcher(ctx, clusterKey)
}

// _keepWatching blocks on various channels:
// - The ZK event stream for cluster changes.
// - The reWatch channel for connection recovery events.
// - The myappsChanged channel for local application route changes.
// - The shutdown channel to terminate the loop.
func (ard *AppRouteDiscovery) _keepWatching(stream <-chan zk.Event) error {
	select {
	case event := <-stream:
		if err := event.Err; err != nil {
			ard.Log("failure watching service. %v", err)
			return err
		}
	case <-ard.reWatch:
		ard.Log("zookeeper connection recoverd, check topo again.")
	case <-ard.myappsChanged:
		ard.Log("app routes changed, check topo again.")
	case <-ard.Shutdown.C():
		ard.Log("shutdown...")
		return ErrShutdown
	}

	// After any event, fetch the full list of routes to resynchronize the state.
	nodes, version, err := ard.fetchNodes()
	if err != nil {
		ard.Log("failure fetch nodes when watching service. %v", err)
		return err
	}
	for !ard.containSelf(nodes) {
		// i am lost, register self
		// This is a self-healing mechanism. If any of the node's own application routes are missing, re-register all of them.
		if err = ard.registerService(); err != nil {
			return err
		}
		// reload nodes
		nodes, version, err = ard.fetchNodes()
		if err != nil {
			ard.Log("failure fetch nodes when watching service. %v", err)
			return err
		}
		time.Sleep(time.Second)
	}
	ard.updateNodes(nodes, version)

	return nil
}

func (ard *AppRouteDiscovery) isChildrenChanged(stat *zk.Stat) bool {
	return stat.Cversion != int32(ard.revision)
}

// containSelf checks if all of the current node's registered applications (`myapps`)
// are present and correctly represented in the list of nodes fetched from ZooKeeper.
func (ard *AppRouteDiscovery) containSelf(ns []*AppRouteNode) bool {
	// Assume all local apps are present until proven otherwise.
	bContainSelf := true
	remoteNodes := make(map[string]*AppRouteNode)
	for i, item := range ns {
		remoteNodes[item.UniqueId()] = ns[i]
	}

	// Check each local app against the fetched remote nodes.
	ard.myapps.Range(func(key any, value any) bool {
		local := value.(*AppRouteNode)
		if rn, ok := remoteNodes[local.UniqueId()]; !ok || !rn.Equal(local) {
			bContainSelf = false
			return false
		}
		return true
	})
	return bContainSelf
}

// Stop gracefully shuts down the discovery service, deregistering all its application routes.
func (ard *AppRouteDiscovery) Stop() (err error) {
	err = ard.deregisterService()
	if err != nil {
		ard.Log("deregister app routes fail %v", err)
		return
	}
	return
}

// deregisterService removes all znodes associated with this node's application routes.
func (ard *AppRouteDiscovery) deregisterService() error {
	ard.myapps.Range(func(key any, value any) bool {
		local := value.(*AppRouteNode)
		if path := local.GetPath(); path != "" {
			ard.Conn.Delete(path, -1)
		}
		return true
	})
	return nil
}

// OnEvent handles ZooKeeper session events.
// It is responsible for triggering a re-watch when a session is re-established.
func (ard *AppRouteDiscovery) OnEvent(evt zk.Event) {
	if evt.Type != zk.EventSession {
		return
	}
	switch evt.State {
	case zk.StateHasSession:
		select {
		case ard.reWatch <- struct{}{}:
		default:
		}
	}
}

func (ard *AppRouteDiscovery) startEventNotifyLoop() {
	go func() {
		for {
			select {
			case evt := <-ard.eventsCh:
				if node := ard.Node; node != nil {
					if err := node.SendEvent(ard.Event.Load().Name, ard.EventRef.Load(), gen.MessageOptions{}, evt); err != nil {
						ard.Log("failed to send %s %v", evt.String(), err)
					} else {
						ard.Log("send event %s OK", evt.String())
					}
				}
			case <-ard.Shutdown.C():
				return
			}
		}
	}()
}
