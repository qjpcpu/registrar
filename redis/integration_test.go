package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/alicebob/miniredis/v2"
	"github.com/qjpcpu/registrar/events"
	goredis "github.com/redis/go-redis/v9"
)

type probeActor struct {
	act.Actor
	received chan any
}

func (a *probeActor) Init(args ...any) error {
	a.received = args[0].(chan any)
	return nil
}
func (a *probeActor) HandleMessage(_ gen.PID, message any) error {
	if message == "subscribe" {
		reg, err := a.Node().Network().Registrar()
		if err != nil {
			return err
		}
		event, err := reg.Event()
		if err != nil {
			return err
		}
		if _, err = a.MonitorEvent(event); err != nil {
			return err
		}
		a.received <- "subscribed"
		return nil
	}
	a.received <- message
	return nil
}
func (a *probeActor) HandleEvent(event gen.MessageEvent) error {
	a.received <- event.Message
	return nil
}

func TestErgoNodes(t *testing.T) {
	addr := os.Getenv("REDIS_ENDPOINTS")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	cluster := fmt.Sprintf("ergo-test-%d", time.Now().UnixNano())
	start := func(name gen.Atom) (gen.Node, gen.Registrar) {
		reg, err := Create(Options{Endpoints: strings.Split(addr, ","), Cluster: cluster, PollInterval: 20 * time.Millisecond, SupportRegisterApplication: true})
		requireOK(t, err)
		t.Cleanup(reg.Terminate)
		var options gen.NodeOptions
		options.Network.Registrar = reg
		options.Network.Acceptors = []gen.AcceptorOptions{{Host: "127.0.0.1", TCP: "tcp"}}
		options.Network.Cookie = "registrar-test"
		options.Log.DefaultLogger.Disable = true
		node, err := ergo.StartNode(name, options)
		requireOK(t, err)
		t.Cleanup(node.Stop)
		return node, reg
	}
	a, ra := start("redis-a@localhost")
	messages := make(chan any, 64)
	pid, err := a.SpawnRegister("probe", func() gen.ProcessBehavior { return &probeActor{} }, gen.ProcessOptions{}, messages)
	requireOK(t, err)
	requireOK(t, a.Send(pid, "subscribe"))
	select {
	case message := <-messages:
		if message != "subscribed" {
			t.Fatal(message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription timed out")
	}
	b, rb := start("redis-b@localhost")
	eventually(t, func() bool { nodes, _ := rb.Nodes(); return len(nodes) == 1 })
	requireOK(t, b.Send(gen.ProcessID{Name: "probe", Node: a.Name()}, "hello"))
	deadline := time.After(5 * time.Second)
	joined, hello := false, false
	for !joined || !hello {
		select {
		case m := <-messages:
			switch v := m.(type) {
			case string:
				hello = v == "hello"
			case events.EventNodeJoined:
				joined = v.Name == b.Name()
			}
		case <-deadline:
			t.Fatal("missing remote message or registrar event")
		}
	}
	requireOK(t, rb.RegisterApplicationRoute(gen.ApplicationRoute{Name: "test-app", Node: b.Name(), State: gen.ApplicationStateRunning, Mode: gen.ApplicationModePermanent, Weight: 7}))
	eventually(t, func() bool {
		routes, err := ra.Resolver().ResolveApplication("test-app")
		return err == nil && len(routes) == 1 && routes[0].Weight == 7 && routes[0].Mode == gen.ApplicationModePermanent
	})
}

// External deployment tests require dedicated Redis instances. Failover and slot
// migration run only with REDIS_TEST_FAILOVER=1 because they change topology.
func TestRedisDeployments(t *testing.T) {
	for _, mode := range []string{"single", "sentinel", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			env := "REDIS_ENDPOINTS"
			if mode == "sentinel" {
				env = "REDIS_SENTINEL_ENDPOINTS"
			}
			if mode == "cluster" {
				env = "REDIS_CLUSTER_ENDPOINTS"
			}
			addresses := os.Getenv(env)
			if addresses == "" {
				t.Skip("set " + env + " to test a real deployment")
			}
			options := Options{Endpoints: strings.Split(addresses, ","), Cluster: fmt.Sprintf("deployment-%d", time.Now().UnixNano()), PollInterval: 50 * time.Millisecond, SessionTimeout: 3 * time.Second, SupportRegisterApplication: true}
			if mode == "sentinel" {
				options.MasterName = os.Getenv("REDIS_MASTER_NAME")
			}
			options.RedisCluster = mode == "cluster"
			newClient := func(name gen.Atom) *client {
				reg, err := Create(options)
				requireOK(t, err)
				c := reg.(*client)
				t.Cleanup(c.Terminate)
				startClient(t, c, name)
				return c
			}
			a := newClient("a")
			b := newClient("b")
			eventually(t, func() bool { nodes, _ := a.Nodes(); return len(nodes) == 1 && leaderOf(b) == "a" })
			requireOK(t, a.RegisterApplicationRoute(gen.ApplicationRoute{Name: "app", Node: "a", State: gen.ApplicationStateRunning}))
			eventually(t, func() bool { _, err := b.ResolveApplication("app"); return err == nil })
			if os.Getenv("REDIS_TEST_FAILOVER") == "1" {
				switch mode {
				case "sentinel":
					admin := goredis.NewSentinelClient(&goredis.Options{Addr: options.Endpoints[0]})
					defer admin.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					before, err := admin.GetMasterAddrByName(ctx, options.MasterName).Result()
					requireOK(t, err)
					requireOK(t, admin.Failover(ctx, options.MasterName).Err())
					eventually(t, func() bool {
						after, err := admin.GetMasterAddrByName(ctx, options.MasterName).Result()
						return err == nil && strings.Join(after, ":") != strings.Join(before, ":")
					})
				case "cluster":
					migrateSlot(t, a)
					failoverCluster(t, a)
				}
				// A new application proves writes and peer reads recover on the new master.
				requireOK(t, a.RegisterApplicationRoute(gen.ApplicationRoute{Name: "after", Node: "a", State: gen.ApplicationStateRunning}))
				eventually(t, func() bool { _, err := b.ResolveApplication("after"); return err == nil })
			}
			a.Terminate()
			eventually(t, func() bool { nodes, _ := b.Nodes(); return len(nodes) == 0 && leaderOf(b) == "b" })
		})
	}
}

func migrateSlot(t *testing.T, c *client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cluster := c.store.client.(*goredis.ClusterClient)
	slot, err := cluster.ClusterKeySlot(ctx, c.store.keys[0]).Result()
	requireOK(t, err)
	slots, err := cluster.ClusterSlots(ctx).Result()
	requireOK(t, err)
	var source, target goredis.ClusterNode
	for _, s := range slots {
		if int(slot) >= s.Start && int(slot) <= s.End {
			source = s.Nodes[0]
		}
	}
	for _, s := range slots {
		if s.Nodes[0].ID != source.ID {
			target = s.Nodes[0]
			break
		}
	}
	if target.ID == "" {
		t.Fatal("slot migration needs multiple masters")
	}
	src := goredis.NewClient(&goredis.Options{Addr: source.Addr})
	defer src.Close()
	dst := goredis.NewClient(&goredis.Options{Addr: target.Addr})
	defer dst.Close()
	requireOK(t, dst.Do(ctx, "CLUSTER", "SETSLOT", slot, "IMPORTING", source.ID).Err())
	requireOK(t, src.Do(ctx, "CLUSTER", "SETSLOT", slot, "MIGRATING", target.ID).Err())
	// Move the registrar's keys together so Lua never sees a split dataset.
	parts := strings.Split(target.Addr, ":")
	args := []any{"MIGRATE", parts[0], parts[1], "", 0, 5000, "KEYS"}
	for _, key := range c.store.keys {
		args = append(args, key)
	}
	requireOK(t, src.Do(ctx, args...).Err())
	requireOK(t, cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		return node.Do(ctx, "CLUSTER", "SETSLOT", slot, "NODE", target.ID).Err()
	}))
}

func failoverCluster(t *testing.T, c *client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cluster := c.store.client.(*goredis.ClusterClient)
	slot, err := cluster.ClusterKeySlot(ctx, c.store.keys[0]).Result()
	requireOK(t, err)
	slots, err := cluster.ClusterSlots(ctx).Result()
	requireOK(t, err)
	var primary, replica goredis.ClusterNode
	for _, s := range slots {
		if int(slot) >= s.Start && int(slot) <= s.End {
			primary = s.Nodes[0]
			if len(s.Nodes) > 1 {
				replica = s.Nodes[1]
			}
		}
	}
	if replica.ID == "" {
		t.Fatal("cluster failover test needs a replica for each master")
	}
	admin := goredis.NewClient(&goredis.Options{Addr: replica.Addr})
	defer admin.Close()
	requireOK(t, admin.Do(ctx, "CLUSTER", "FAILOVER").Err())
	eventually(t, func() bool {
		slots, err := cluster.ClusterSlots(ctx).Result()
		if err != nil {
			return false
		}
		for _, s := range slots {
			if int(slot) >= s.Start && int(slot) <= s.End {
				return s.Nodes[0].ID != primary.ID
			}
		}
		return false
	})
}
