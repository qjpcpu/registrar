package redis

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/alicebob/miniredis/v2"
	"github.com/qjpcpu/registrar/constants"
	"github.com/qjpcpu/registrar/events"
)

type quietLog struct{ gen.Log }

func (quietLog) Error(string, ...any) {}

type testNode struct {
	gen.NodeRegistrar
	name       gen.Atom
	mu         sync.Mutex
	messages   []any
	registered bool
}

func (n *testNode) Name() gen.Atom { return n.name }
func (n *testNode) Log() gen.Log   { return quietLog{} }
func (n *testNode) RegisterEvent(gen.Atom, gen.EventOptions) (gen.Ref, error) {
	n.registered = true
	return gen.Ref{}, nil
}
func (n *testNode) UnregisterEvent(gen.Atom) error { n.registered = false; return nil }
func (n *testNode) SendEvent(_ gen.Atom, _ gen.Ref, _ gen.MessageOptions, message any) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = append(n.messages, message)
	return nil
}
func (n *testNode) take() []any {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := n.messages
	n.messages = nil
	return result
}

func makeClient(t *testing.T, addr string, apps bool) *client {
	t.Helper()
	reg, err := Create(Options{Endpoints: []string{addr}, SessionTimeout: time.Second, PollInterval: 20 * time.Millisecond, SupportRegisterApplication: apps})
	requireOK(t, err)
	c := reg.(*client)
	t.Cleanup(c.Terminate)
	return c
}
func startClient(t *testing.T, c *client, name gen.Atom) *testNode {
	t.Helper()
	node := &testNode{name: name}
	_, err := c.Register(node, gen.RegisterRoutes{Routes: []gen.Route{{Host: "localhost", Port: 1234}}})
	requireOK(t, err)
	return node
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}
func leaderOf(c *client) gen.Atom {
	v, _ := c.ConfigItem(constants.LeaderNodeConfigItem)
	return v.(gen.Atom)
}
func hasType[T any](messages []any) bool {
	for _, m := range messages {
		if _, ok := m.(T); ok {
			return true
		}
	}
	return false
}

func TestDiscoveryAndApplicationLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	a := makeClient(t, server.Addr(), true)
	na := startClient(t, a, "a@localhost")
	b := makeClient(t, server.Addr(), true)
	startClient(t, b, "b@localhost")
	eventually(t, func() bool {
		nodes, _ := a.Nodes()
		return len(nodes) == 1 && nodes[0] == "b@localhost" && leaderOf(b) == "a@localhost"
	})
	if !hasType[events.EventNodeJoined](na.take()) {
		t.Fatal("missing join event")
	}
	routes, err := a.Resolve("a@localhost")
	requireOK(t, err)
	if len(routes) != 1 {
		t.Fatal("self route missing")
	}
	_, err = a.Resolve("unknown")
	if !errors.Is(err, gen.ErrNoRoute) {
		t.Fatal(err)
	}
	app := gen.ApplicationRoute{Name: "app", Node: "b@localhost", Weight: 2, State: gen.ApplicationStateLoaded}
	for _, state := range []gen.ApplicationState{gen.ApplicationStateLoaded, gen.ApplicationStateRunning, gen.ApplicationStateStopping} {
		app.State = state
		requireOK(t, b.RegisterApplicationRoute(app))
		eventually(t, func() bool { routes, err := a.ResolveApplication("app"); return err == nil && routes[0].State == state })
		var observed bool
		eventually(t, func() bool {
			for _, m := range na.take() {
				switch state {
				case gen.ApplicationStateLoaded:
					_, observed = m.(events.EventApplicationLoaded)
				case gen.ApplicationStateRunning:
					_, observed = m.(events.EventApplicationStarted)
				case gen.ApplicationStateStopping:
					_, observed = m.(events.EventApplicationStopping)
				}
				if observed {
					return true
				}
			}
			return false
		})
	}
	requireOK(t, b.UnregisterApplicationRoute("app"))
	eventually(t, func() bool { _, err := a.ResolveApplication("app"); return errors.Is(err, gen.ErrNoRoute) })
	eventually(t, func() bool { return hasType[events.EventApplicationStopped](na.take()) })
	a.Terminate()
	eventually(t, func() bool { return leaderOf(b) == "b@localhost" })
	a.Terminate()
}

func TestSnapshotEventsAndLatestRegistration(t *testing.T) {
	server := miniredis.RunT(t)
	c := makeClient(t, server.Addr(), true)
	n := &testNode{name: "self"}
	c.node = n
	c.local.Name = n.name
	app := gen.ApplicationRoute{Name: "app", Node: "peer", State: gen.ApplicationStateRunning, Weight: 1}
	snapshot := []member{{ID: c.id, Seq: 2, registration: registration{Name: "self"}}, {ID: "old", Seq: 1, registration: registration{Name: "peer", Routes: []gen.Route{{Port: 1}}}}, {ID: "new", Seq: 3, registration: registration{Name: "peer", Routes: []gen.Route{{Port: 2}}, Apps: []gen.ApplicationRoute{app}}}}
	c.applySnapshot(snapshot)
	if leaderOf(c) != "self" {
		t.Fatal("leader must use deduplicated members")
	}
	routes, err := c.Resolve("peer")
	requireOK(t, err)
	if routes[0].Port != 2 {
		t.Fatal("old registration selected")
	}
	n.take()
	c.applySnapshot(snapshot)
	if len(n.take()) != 0 {
		t.Fatal("unchanged snapshot generated events")
	}
	snapshot[2].Apps[0].Weight = 9
	c.applySnapshot(snapshot)
	if !hasType[events.EventApplicationStarted](n.take()) {
		t.Fatal("weight change missing")
	}
	c.applySnapshot(snapshot[:1])
	messages := n.take()
	if !hasType[events.EventApplicationStopped](messages) || !hasType[events.EventNodeLeft](messages) {
		t.Fatal(messages)
	}
}

func TestRecoveryAndConcurrentApplications(t *testing.T) {
	server := miniredis.RunT(t)
	c := makeClient(t, server.Addr(), true)
	n := startClient(t, c, "a")
	requireOK(t, c.RegisterApplicationRoute(gen.ApplicationRoute{Name: "app", Node: "a", State: gen.ApplicationStateRunning}))
	eventually(t, func() bool { _, err := c.ResolveApplication("app"); return err == nil })
	// Commands fail while the server stays reachable; local routes must remain cached.
	server.SetError("ERR temporarily unavailable")
	eventually(t, func() bool { return leaderOf(c) == "" })
	if _, err := c.Resolve("a"); err != nil {
		t.Fatal("failure cleared snapshot")
	}
	if !hasType[events.EventNodeSwitchedToFollower](n.take()) {
		t.Fatal("missing demotion")
	}
	requireOK(t, c.UnregisterApplicationRoute("app"))
	server.FlushAll()
	server.SetError("")
	eventually(t, func() bool {
		_, err := c.ResolveApplication("app")
		return leaderOf(c) == "a" && errors.Is(err, gen.ErrNoRoute)
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = c.RegisterApplicationRoute(gen.ApplicationRoute{Name: "app", Node: "a", Weight: j})
				_, _ = c.Resolve("a")
				_, _ = c.Nodes()
				_ = c.UnregisterApplicationRoute("app")
			}
		}()
	}
	wg.Wait()
	final := gen.ApplicationRoute{Name: "app", Node: "a", Weight: 999, State: gen.ApplicationStateRunning}
	requireOK(t, c.RegisterApplicationRoute(final))
	eventually(t, func() bool {
		routes, err := c.ResolveApplication("app")
		return err == nil && reflect.DeepEqual(routes[0], final)
	})
}

func TestCapabilitiesAndFailedStart(t *testing.T) {
	server := miniredis.RunT(t)
	c := makeClient(t, server.Addr(), false)
	if c.Info().SupportRegisterApplication || !c.Info().SupportEvent || c.Info().SupportConfig {
		t.Fatal(c.Info())
	}
	if c.RegisterApplicationRoute(gen.ApplicationRoute{}) != gen.ErrUnsupported {
		t.Fatal("application capability")
	}
	if _, err := c.ResolveApplication("app"); err != gen.ErrUnsupported {
		t.Fatal(err)
	}
	if c.RegisterProxy("a") != gen.ErrUnsupported || c.UnregisterProxy("a") != gen.ErrUnsupported {
		t.Fatal("proxy capability")
	}
	if _, err := c.ResolveProxy("a"); err != gen.ErrNoRoute {
		t.Fatal(err)
	}
	if _, err := c.Config(); err != gen.ErrUnsupported {
		t.Fatal(err)
	}
	if value, err := c.ConfigItem("unknown"); value != nil || err != nil {
		t.Fatal(value, err)
	}
	n := &testNode{name: "a"}
	if _, err := c.Register(n, gen.RegisterRoutes{}); err != gen.ErrNoRoute {
		t.Fatal(err)
	}
	server.SetError("ERR unavailable")
	_, err := c.Register(n, gen.RegisterRoutes{Routes: []gen.Route{{Port: 1}}})
	if err == nil || n.registered {
		t.Fatal("failed registration did not clean event")
	}
	c.Terminate()
}

func TestExpiredMemberAndReregistration(t *testing.T) {
	server := miniredis.RunT(t)
	c := makeClient(t, server.Addr(), true)
	n := startClient(t, c, "a")
	// Seed a crashed member: nobody renews its lease.
	requireOK(t, c.store.write(context.Background(), "crashed", registration{Name: "crashed", Apps: []gen.ApplicationRoute{{Name: "app", Node: "crashed", State: gen.ApplicationStateRunning}}}, 100*time.Millisecond))
	eventually(t, func() bool { nodes, _ := c.Nodes(); return len(nodes) == 1 })
	eventually(t, func() bool { nodes, _ := c.Nodes(); return len(nodes) == 0 })
	if !hasType[events.EventNodeLeft](n.take()) {
		t.Fatal("expiry did not emit departure")
	}
	requireOK(t, c.store.remove(context.Background(), c.id))
	eventually(t, func() bool {
		members, err := c.store.snapshot(context.Background())
		return err == nil && len(members) == 1 && members[0].ID == c.id && members[0].Seq > 1
	})
}

func TestRedisCredentials(t *testing.T) {
	for _, username := range []string{"", "registrar"} {
		name := "password-only"
		if username != "" {
			name = "username-and-password"
		}
		t.Run(name, func(t *testing.T) {
			server := miniredis.RunT(t)
			server.RequireAuth("test-password")
			if username != "" {
				server.RequireUserAuth(username, "test-password")
			}
			reg, err := Create(Options{
				Endpoints: []string{server.Addr()},
				Username:  username,
				Password:  "test-password",
			})
			requireOK(t, err)
			c := reg.(*client)
			t.Cleanup(c.Terminate)
			startClient(t, c, "authenticated")
			members, err := c.store.snapshot(context.Background())
			requireOK(t, err)
			if len(members) != 1 || members[0].Name != "authenticated" {
				t.Fatalf("authenticated registration missing: %+v", members)
			}
		})
	}
}

func TestSnapshotNodeIncarnationChanges(t *testing.T) {
	for _, change := range []struct {
		name string
		id   string
		seq  int64
	}{
		{"lease-reregistered", "peer-instance", 4},
		{"process-restarted", "new-instance", 4},
		{"instance-changed-with-same-sequence", "new-instance", 2},
	} {
		t.Run(change.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			c := makeClient(t, server.Addr(), false)
			node := &testNode{name: "self"}
			c.node, c.local.Name = node, node.name
			snapshot := []member{
				{ID: c.id, Seq: 1, registration: registration{Name: "self"}},
				{ID: "peer-instance", Seq: 2, registration: registration{Name: "peer"}},
				{ID: "other-instance", Seq: 3, registration: registration{Name: "other"}},
			}
			c.applySnapshot(snapshot)
			node.take()
			// The peer is present in both polls, but its registration has changed.
			snapshot[1].ID, snapshot[1].Seq = change.id, change.seq
			c.applySnapshot(snapshot)
			want := []any{events.EventNodeLeft{Name: "peer"}, events.EventNodeJoined{Name: "peer"}}
			if got := node.take(); !reflect.DeepEqual(got, want) {
				t.Fatalf("incarnation events: got %#v, want %#v", got, want)
			}
			c.applySnapshot(snapshot)
			if got := node.take(); len(got) != 0 {
				t.Fatalf("stable registration emitted events: %#v", got)
			}
		})
	}
}
