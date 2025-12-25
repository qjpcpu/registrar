package zk

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

// NodeDiscovery is responsible for node registration and discovery within the Ergo cluster using ZooKeeper.
// Each node registers itself as an ephemeral, sequential znode under a common root path.
// It watches for changes in the list of children under this root to discover other nodes,
// and implements a leader election mechanism based on the lowest sequential znode ID.
type NodeDiscovery struct {
	Conn zkConn
	Node gen.NodeRegistrar

	/* options */
	RootZnode         string
	Routes, AdvRoutes *AtomicValue[[]gen.Route]
	RoutesMapper      RoutesMapper
	Shutdown          *CloseChan
	Event             *AtomicValue[gen.Event]
	EventRef          *AtomicValue[gen.Ref]

	/* state fields */
	self            *Node             // Represents the current node's information.
	Members         sync.Map          // A map of all discovered nodes in the cluster, including self. Key: gen.Atom(NodeName), Value: *Node.
	revision        uint64            // The cversion of the root znode, used to detect changes.
	fullpath        string            // The full ZK path of the ephemeral znode for this node.
	role            RoleType          // The current role of the node (Leader or Follower).
	roleChangedChan chan RoleType     // A channel to notify about role changes.
	reWatch         chan struct{}     // A channel to trigger a re-watch, typically after a ZK session reconnect.
	eventsCh        chan fmt.Stringer // A channel for sending node lifecycle events (joined/left).
	started         bool
	startMu         sync.Mutex
	closeErrorLog   atomic.Bool
	leaderNode      *AtomicValue[gen.Atom]
}

func (nd *NodeDiscovery) ResolveNodes() (nodes []gen.Atom) {
	nd.Members.Range(func(key any, value any) bool {
		nodeName := key.(gen.Atom)
		// skip self
		if nd.Node == nil || nd.Node.Name() != nodeName {
			nodes = append(nodes, nodeName)
		}
		return true
	})
	return
}

// StartMember begins the node discovery and registration process.
// It initializes the node, registers it in ZooKeeper, fetches the initial list of members,
// determines its leadership role, and starts watching for cluster changes.
func (nd *NodeDiscovery) StartMember() error {
	nd.startMu.Lock()
	defer nd.startMu.Unlock()
	if nd.started {
		return nil
	}

	// Initialize self node data.
	if err := nd.init(nd.AdvRoutes.Load()); err != nil {
		return err
	}

	nd.startEventNotifyLoop()

	// register self
	// This creates an ephemeral sequential znode in ZK.
	if err := nd.registerService(); err != nil {
		nd.Error("register service fail %v", err)
		return err
	}
	nd.Debug("starting node, register service: node=%s seq=%s", nd.self.Name, nd.self.Meta[metaKeySeq])

	// Fetch the initial list of all nodes in the cluster.
	nodes, version, err := nd.fetchNodes()
	if err != nil {
		nd.Error("fetch nodes fail %v", err)
		return err
	}

	// Update internal state with the fetched nodes and determine leadership.
	nd.updateNodesWithSelf(nodes, version)
	nd.updateLeadership(nodes)
	nd.startWatching()

	nd.started = true
	return nil
}

// RegisterRoutes updates the routes for the current node.
// It also applies any configured route mapping.
func (nd *NodeDiscovery) RegisterRoutes(routes []gen.Route) []gen.Route {
	nd.Routes.Store(routes)
	return nd.AdvRoutes.Store(nd.mapRoutes(routes))
}

func (nd *NodeDiscovery) mapRoutes(routes []gen.Route) []gen.Route {
	if mapfn := nd.RoutesMapper; mapfn != nil {
		return mapfn(routes)
	}
	return routes
}

func (nd *NodeDiscovery) init(routes []gen.Route) error {
	nd.self = NewNode(nd.Node.Name(), routes)
	nd.self.SetMeta(metaKeyID, string(nd.self.Name))

	if err := ensurePersistentNode(nd.Conn, nd.RootZnode); err != nil {
		nd.Error("create dir node fail path=%s %v", nd.RootZnode, err)
		return err
	}
	return nil
}

func (nd *NodeDiscovery) GetLeader() gen.Atom {
	return nd.leaderNode.Load()
}

func (nd *NodeDiscovery) startEventNotifyLoop() {
	sendEvent := func(evt fmt.Stringer) {
		if node := nd.Node; node != nil {
			if err := node.SendEvent(nd.Event.Load().Name, nd.EventRef.Load(), gen.MessageOptions{}, evt); err != nil {
				nd.Error("failed to send %s %v", evt.String(), err)
			} else {
				nd.Info("send event %s OK", evt.String())
			}
		}
	}
	go func() {
		for {
			select {
			case role := <-nd.roleChangedChan:
				if node := nd.Node; node != nil {
					sendEvent(buildRoleEvent(role, node.Name()))
				}
			case evt := <-nd.eventsCh:
				sendEvent(evt)
			case <-nd.Shutdown.C():
				return
			}
		}
	}()
}

// registerService creates an ephemeral sequential znode in ZooKeeper for the current node.
// This znode contains the node's name and routes. Its sequential nature is used for leader election.
func (nd *NodeDiscovery) registerService() error {
	data, err := nd.self.Serialize()
	if err != nil {
		nd.Error("register node Serialize fail. %v", err)
		return err
	}

	path, err := createEphemeralSequentialChildNode(nd.Conn, "node", nd.RootZnode, data)
	if err != nil {
		nd.Error("create child node fail. node=%s %v", nd.RootZnode, err)
		return err
	}
	nd.fullpath = path
	seq, err := parseSeq(path)
	if err != nil {
		nd.Error("create child node fail. node=%s %v", nd.RootZnode, err)
		return err
	}
	nd.self.SetMeta(metaKeySeq, intToStr(seq))
	nd.Debug("register node=%s seq=%d OK", nd.self.Name, seq)

	return nil
}

// fetchNodes retrieves the list of all registered nodes by listing the children of the root znode.
// It reads the data for each child znode to construct the list of *Node objects.
func (nd *NodeDiscovery) fetchNodes() ([]*Node, int32, error) {
	key := nd.RootZnode
	children, stat, err := nd.Conn.Children(key)
	if err != nil {
		nd.Error("fetch nodes fail. path=%s %v", key, err)
		return nil, 0, err
	}

	var nodes []*Node
	for _, short := range children {
		long := filepath.Join(key, short)
		value, _, err := nd.Conn.Get(long)
		if err != nil {
			nd.Error("fetch nodes fail. path=%s %v", long, err)
			return nil, stat.Cversion, err
		}
		n := Node{Meta: make(map[string]string)}
		if err := n.Deserialize(value); err != nil {
			nd.Error("fetch nodes deserialize fail. path=%s val=%s %v", long, string(value), err)
			return nil, stat.Cversion, err
		}
		seq, err := parseSeq(long)
		if err != nil {
			nd.Error("fetch nodes parse seq fail. path=%s val=%s %v", long, string(value), err)
			return nil, 0, err
		} else {
			n.SetMeta(metaKeySeq, intToStr(seq))
		}
		nd.Debug("fetched new node. node=%s path=%s seq=%d", n.Name, long, seq)
		nodes = append(nodes, &n)
	}
	return nd.uniqNodes(nodes), stat.Cversion, nil
}

// uniqNodes filters the list of nodes to ensure that for each node name, only the one
// with the highest sequence number (the most recent registration) is kept.
func (nd *NodeDiscovery) uniqNodes(nodes []*Node) []*Node {
	nodeMap := make(map[gen.Atom]*Node)
	for _, node := range nodes {
		if n, ok := nodeMap[node.Name]; ok {
			// keep node with higher version
			if node.GetSeq() > n.GetSeq() {
				nodeMap[node.Name] = node
			}
		} else {
			nodeMap[node.Name] = node
		}
	}

	var out []*Node
	for _, node := range nodeMap {
		out = append(out, node)
	}
	return out
}

func (nd *NodeDiscovery) updateNodesWithSelf(members []*Node, version int32) {
	nd.updateNodes(members, version)
	nd.Members.Store(nd.self.Name, nd.self)
}

// updateNodes compares the new list of members with the existing one,
// updates the internal `Members` map, and sends `EventNodeJoined` or `EventNodeLeft` events
// for any changes detected.
func (nd *NodeDiscovery) updateNodes(members []*Node, reversion int32) {
	newMembers := make(map[gen.Atom]*Node)
	for i, n := range members {
		newMembers[n.Name] = members[i]
		if _, ok := nd.Members.Load(n.Name); !ok && n.Name != nd.Node.Name() {
			nd.eventsCh <- EventNodeJoined{Name: n.Name}
		}
		nd.Members.Store(n.Name, n)
	}
	nd.Members.Range(func(key any, value any) bool {
		name := key.(gen.Atom)
		if _, ok := newMembers[name]; !ok {
			if name != nd.Node.Name() {
				nd.eventsCh <- EventNodeLeft{Name: name}
			}
			nd.Members.Delete(name)
		}
		return true
	})
	nd.revision = uint64(reversion)
}

// updateLeadership determines the node's role (Leader or Follower) based on the current list of nodes.
// If the role changes, it notifies the listener.
func (nd *NodeDiscovery) updateLeadership(ns []*Node) {
	role := Follower
	if nd.isLeaderOf(ns) {
		role = Leader
	}
	nd.leaderNode.Store(nd.chooseLeaderFrom(ns))
	if role != nd.role {
		nd.Info("role changed from=%s to=%s", nd.role.String(), role.String())
		nd.role = role
		nd.roleChangedChan <- role
	}
}

// isLeaderOf implements the leader election logic.
// The node with the smallest sequence number among all registered znodes is elected as the leader.
func (nd *NodeDiscovery) isLeaderOf(ns []*Node) bool {
	if len(ns) == 1 && nd.self != nil && ns[0].Name == nd.self.Name {
		return true
	}
	var seqList sequences
	for _, node := range ns {
		seqList = seqList.Add(node.GetSeq())
	}
	minSeq := seqList.Min()
	for _, node := range ns {
		if nd.self != nil && node.Name == nd.self.Name {
			nd.Debug("check self leadership, self_seq=%d min_seq=%d is_leader=%t",
				nd.self.GetSeq(),
				minSeq,
				minSeq == nd.self.GetSeq(),
			)
			return minSeq == nd.self.GetSeq()
		}
	}
	if nd.self != nil {
		nd.Debug("I'm follower self_seq=%d min_seq=%d seq_list=%s",
			nd.self.GetSeq(),
			minSeq,
			seqList.String(),
		)
	} else {
		nd.Debug("I'm follower, self node info is blank")
	}
	return false
}

func (nd *NodeDiscovery) chooseLeaderFrom(ns []*Node) gen.Atom {
	var minSeq = -1
	var leader gen.Atom
	for _, node := range ns {
		if seq := node.GetSeq(); seq < minSeq || minSeq == -1 {
			leader = node.Name
			minSeq = seq
		}
	}
	return leader
}

// startWatching starts a background goroutine that continuously watches for changes
// in the ZooKeeper cluster.
func (nd *NodeDiscovery) startWatching() {
	ctx := context.TODO()
	go func() {
		for !nd.Shutdown.Closed() {
			if err := nd.keepWatching(ctx); err != nil {
				if err == ErrShutdown {
					return
				}
				if err != zk.ErrConnectionClosed {
					nd.Error("failed to keepWatching. %v", err)
				}
			}
		}
	}()
}

// keepWatching is the core of the watch loop. It sets a watch on the root znode's children,
// and then blocks waiting for an event.
func (nd *NodeDiscovery) keepWatching(ctx context.Context) error {
	evtChan, err := nd.addWatcher(ctx, nd.RootZnode)
	if err != nil {
		return err
	}

	return nd._keepWatching(evtChan)
}

// addWatcher sets a `ChildrenW` watch on the given ZK path.
// Before returning, it checks if the children have changed since the last check.
// If they have, it fetches the new list, updates the node state, and re-establishes the watch.
// This handles the case where changes happen between a watch event and setting a new watch.
func (nd *NodeDiscovery) addWatcher(ctx context.Context, clusterKey string) (<-chan zk.Event, error) {
	_, stat, evtChan, err := nd.Conn.ChildrenW(clusterKey)
	if err != nil {
		return nil, err
	}

	nd.Debug("watching path=%s children=%d cversion=%d", clusterKey, int(stat.NumChildren), stat.Cversion)
	if !nd.isChildrenChanged(stat) {
		return evtChan, nil
	}

	nd.Debug("chilren changed, wait 1 sec and watch again old_cversion=%d new_cversion=%d", int(nd.revision), int(stat.Cversion))
	time.Sleep(1 * time.Second)
	nodes, version, err := nd.fetchNodes()
	if err != nil {
		return nil, err
	}
	if !nd.containSelf(nodes) {
		if err = nd.registerService(); err != nil {
			return nil, err
		}
	}
	// initialize members
	nd.updateNodes(nodes, version)
	nd.updateLeadership(nodes)
	return nd.addWatcher(ctx, clusterKey)
}

// _keepWatching blocks on various channels:
// - The ZK event stream for cluster changes.
// - The reWatch channel for connection recovery events.
// - The shutdown channel to terminate the loop.
func (nd *NodeDiscovery) _keepWatching(stream <-chan zk.Event) error {
	select {
	case event := <-stream:
		if err := event.Err; err != nil {
			nd.Error("failure watching service. %v", err)
			if childrenNotContains(nd.Conn, nd.RootZnode, filepath.Base(nd.fullpath)) {
				nd.Debug("node register info lost, register self again")
				nd.registerService()
			}
			return err
		}
	case <-nd.reWatch:
		nd.Debug("zookeeper connection recoverd, check topo again.")
	case <-nd.Shutdown.C():
		nd.Debug("shutdown...")
		return ErrShutdown
	}

	// After any event, fetch the full list of nodes to resynchronize the state.
	nodes, version, err := nd.fetchNodes()
	if err != nil {
		nd.Error("failure fetch nodes when watching service. %v", err)
		return err
	}
	for !nd.containSelf(nodes) {
		// i am lost, register self
		// This is a self-healing mechanism. If the node's znode is gone, re-register it.
		if err = nd.registerService(); err != nil {
			return err
		}
		// reload nodes
		nodes, version, err = nd.fetchNodes()
		if err != nil {
			nd.Error("failure fetch nodes when watching service. %v", err)
			return err
		}
		time.Sleep(time.Second)
	}
	nd.updateNodes(nodes, version)
	nd.updateLeadership(nodes)

	return nil
}

func (nd *NodeDiscovery) isChildrenChanged(stat *zk.Stat) bool {
	return stat.Cversion != int32(nd.revision)
}

// containSelf checks if the current node's registration is present and correct
// in the given list of nodes fetched from ZooKeeper.
func (nd *NodeDiscovery) containSelf(ns []*Node) bool {
	var bContainSelf bool
	for _, node := range ns {
		if nd.self != nil && node.Name == nd.self.Name {
			bContainSelf = nd.self.GetSeq() == node.GetSeq()
			break
		}
	}
	if !bContainSelf {
		if nd.self == nil {
			nd.Debug("I'm lost and self is blank")
		} else {
			nd.Debug("I'm lost node=%s seq=%d", nd.self.Name, nd.self.GetSeq())
		}
	}
	return bContainSelf
}

// Stop gracefully shuts down the NodeDiscovery, deregistering the node from ZooKeeper.
func (nd *NodeDiscovery) Stop() (err error) {
	nd.closeErrorLog.Store(true)
	nd.updateLeadership(nil)
	err = nd.deregisterService()
	if err != nil {
		nd.Error("deregister node member %v", err)
		return
	}
	return
}

func (nd *NodeDiscovery) deregisterService() error {
	if nd.fullpath != "" {
		nd.Conn.Delete(nd.fullpath, -1)
	}
	nd.fullpath = ""
	return nil
}

// OnEvent handles ZooKeeper session events.
// It is responsible for triggering re-watch on reconnection and managing leadership state on disconnection.
func (nd *NodeDiscovery) OnEvent(evt zk.Event) {
	nd.Debug("zookeeper event. type=%s state=%s path=%s", evt.Type.String(), evt.State.String(), evt.Path)
	if evt.Type != zk.EventSession {
		return
	}
	switch evt.State {
	case zk.StateConnecting, zk.StateDisconnected, zk.StateExpired:
		if nd.role == Leader {
			nd.Info("role changed: from=%s  to=%s", Leader.String(), Follower.String())
			nd.role = Follower
			nd.roleChangedChan <- Follower
		}
	case zk.StateConnected:
	case zk.StateHasSession:
		select {
		case nd.reWatch <- struct{}{}:
		default:
		}
	}
}

func (nd *NodeDiscovery) Info(format string, args ...any) {
	if node := nd.Node; node != nil {
		node.Log().Info(`(registrar/nodes)`+format, args...)
	}
}

func (nd *NodeDiscovery) Debug(format string, args ...any) {
	if node := nd.Node; node != nil {
		node.Log().Debug(`(registrar/nodes)`+format, args...)
	}
}

func (nd *NodeDiscovery) Error(format string, args ...any) {
	if node := nd.Node; node != nil && !nd.closeErrorLog.Load() {
		node.Log().Error(`(registrar/nodes)`+format, args...)
	}
}
