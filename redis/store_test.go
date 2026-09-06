package redis

import (
	"context"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreLeaseAndRetry(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	raw := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { raw.Close() })
	s := newStore(raw, "test{cluster}")
	ctx := context.Background()
	a := registration{Name: "a@localhost", Routes: []gen.Route{{Host: "localhost", Port: 1234}}}
	requireOK(t, s.write(ctx, "a", a, 10*time.Second))
	requireOK(t, s.write(ctx, "b", registration{Name: "b@localhost"}, 10*time.Second))
	// Repeating a write models a committed operation whose reply was lost.
	a.Apps = []gen.ApplicationRoute{{Name: "app", Node: a.Name, State: gen.ApplicationStateRunning}}
	requireOK(t, s.write(ctx, "a", a, 10*time.Second))
	snapshot, err := s.snapshot(ctx)
	requireOK(t, err)
	seq := map[string]int64{}
	for _, m := range snapshot {
		seq[m.ID] = m.Seq
		if m.ID == "a" && len(m.Apps) != 1 {
			t.Fatal("application not updated")
		}
	}
	if seq["a"] != 1 || seq["b"] != 2 {
		t.Fatalf("retry changed registration order: %v", seq)
	}
	server.SetTime(now.Add(9 * time.Second))
	requireOK(t, s.write(ctx, "b", registration{Name: "b@localhost"}, 10*time.Second))
	server.SetTime(now.Add(10 * time.Second))
	snapshot, err = s.snapshot(ctx)
	requireOK(t, err)
	if len(snapshot) != 1 || snapshot[0].ID != "b" {
		t.Fatalf("expired record visible: %+v", snapshot)
	}
	requireOK(t, s.write(ctx, "a", a, 10*time.Second))
	snapshot, err = s.snapshot(ctx)
	requireOK(t, err)
	for _, m := range snapshot {
		if m.ID == "a" && m.Seq != 3 {
			t.Fatalf("expected new sequence: %+v", m)
		}
	}
	isolated := newStore(raw, "other")
	snapshot, err = isolated.snapshot(ctx)
	requireOK(t, err)
	if len(snapshot) != 0 {
		t.Fatal("namespace leaked")
	}
	requireOK(t, s.remove(ctx, "a"))
	requireOK(t, s.remove(ctx, "a"))
	snapshot, err = s.snapshot(ctx)
	requireOK(t, err)
	if len(snapshot) != 1 || snapshot[0].ID != "b" {
		t.Fatal("remove affected another member")
	}
}

func TestSnapshotRevisionTracksDiscoveryChanges(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	raw := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer raw.Close()
	s := newStore(raw, "revisions")
	ctx := context.Background()
	record := registration{Name: "node", Apps: []gen.ApplicationRoute{{Name: "b"}, {Name: "a"}}}
	requireOK(t, s.write(ctx, "node", record, time.Minute))
	members, version, changed, err := s.snapshotSince(ctx, "")
	requireOK(t, err)
	if !changed || len(members) != 1 {
		t.Fatal(members, changed)
	}
	record.Apps[0], record.Apps[1] = record.Apps[1], record.Apps[0]
	requireOK(t, s.write(ctx, "node", record, time.Minute))
	members, next, changed, err := s.snapshotSince(ctx, version)
	requireOK(t, err)
	if changed || next != version || members != nil {
		t.Fatal("renewal returned a full snapshot")
	}
	record.Apps[0].State = gen.ApplicationStateRunning
	requireOK(t, s.write(ctx, "node", record, time.Minute))
	members, next, changed, err = s.snapshotSince(ctx, version)
	requireOK(t, err)
	if !changed || next == version || members[0].Apps[0].State != gen.ApplicationStateRunning {
		t.Fatal("application update missing")
	}
	version = next
	server.SetTime(now.Add(time.Minute))
	members, next, changed, err = s.snapshotSince(ctx, version)
	requireOK(t, err)
	if !changed || next == version || len(members) != 0 {
		t.Fatal("expiration did not change snapshot")
	}
	requireOK(t, s.write(ctx, "node", record, time.Minute))
	_, version, _, err = s.snapshotSince(ctx, next)
	requireOK(t, err)
	requireOK(t, s.remove(ctx, "node"))
	members, next, changed, err = s.snapshotSince(ctx, version)
	requireOK(t, err)
	if !changed || len(members) != 0 {
		t.Fatal("removal missing")
	}
	requireOK(t, s.remove(ctx, "node"))
	_, version, changed, err = s.snapshotSince(ctx, next)
	requireOK(t, err)
	if changed || version != next {
		t.Fatal("idempotent removal changed revision")
	}
}
