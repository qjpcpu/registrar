# Redis Registrar for Ergo

`github.com/qjpcpu/registrar/redis` implements `gen.Registrar` and `gen.Resolver` for Ergo using Redis leases and periodic snapshots. It provides node discovery, application routes, registration-order leader election, and the shared `github.com/qjpcpu/registrar/events` notifications.

## Quick start

Create a Redis registrar and assign it to the Ergo node's network options. Nodes using the same Redis deployment and `Cluster` namespace discover each other.

```go
import "github.com/qjpcpu/registrar/redis"

registrar, err := redis.Create(redis.Options{
    Endpoints: []string{"127.0.0.1:6379"},
    Cluster: "my-ergo-cluster",
    SupportRegisterApplication: true,
})
if err != nil {
    return err
}
options.Network.Registrar = registrar
// Start the node with ergo.StartNode(name, options).
// If node startup fails, call registrar.Terminate() to release the client.
```

`Create` configures the client. Redis connectivity is established when Ergo calls `Register` during network startup. `Terminate` closes the owned client and removes this instance's registration.

## Deployment options

Single instance:

```go
redis.Options{Endpoints: []string{"127.0.0.1:6379"}}
```

Sentinel:

```go
redis.Options{
    Endpoints: []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"},
    MasterName: "mymaster",
}
```

Redis Cluster, including a single seed address:

```go
redis.Options{
    Endpoints: []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"},
    RedisCluster: true,
}
```

`MasterName` and `RedisCluster` are mutually exclusive. Ordinary single-instance mode uses the first endpoint; an empty list uses the go-redis default `127.0.0.1:6379`.

| Option | Default | Meaning |
|---|---|---|
| `Cluster` | `default` | Shared Ergo cluster namespace |
| `Username` | empty | Optional Redis ACL username |
| `Password` | empty | Optional Redis password |
| `SessionTimeout` | 10 seconds | Lease lifetime, using Redis server time |
| `PollInterval` | 1 second | Interval between discovery snapshots |
| `SupportRegisterApplication` | `false` | Enable application registration and discovery |

Renewal runs every 30% of `SessionTimeout`, or every 3 seconds by default. Each Redis operation has a deadline of the smaller of that interval and one second.

Optional Redis authentication works in all three deployment modes. Set only `Password` for password-only authentication, or both fields for an ACL user:

```go
redis.Options{
    Endpoints: []string{"127.0.0.1:6379"},
    Username: "registrar",
    Password: redisPassword,
}
```

In Sentinel mode these credentials authenticate to the Redis data servers.

## Discovery, leadership, and events

Each registrar instance publishes its node routes and current applications as one record. Registration retries retain the sequence while the lease is valid. An expired registration receives a new sequence when it returns. Discovery selects the latest registration for each node name, then elects the earliest registration among those nodes. `Nodes()` excludes the local node; `Resolve()` can resolve it.

Application routes include name, node, weight, mode, state, and tags. Updates are asynchronous: registrar methods update the local desired state and wake the publisher; failed writes are retried. Application resolution includes local and remote applications and sorts results by node name.

Snapshot differences produce node and application events. `Event()` returns the local Ergo event named `/ergo/{cluster}/nodes`, with a buffer of 64. Subscribe using the returned event, and query `ConfigItem(constants.LeaderNodeConfigItem)` for the observed leader. Application disappearance produces `EventApplicationStopped`. For a remote node present in both snapshots, a changed instance ID or registration sequence emits `EventNodeLeft` followed by `EventNodeJoined`. This also detects a restart or lease re-registration completed between polls. Stable snapshots do not emit repeated events.

Normal changes are generally visible within one poll interval. A crashed node disappears after its lease expires and the next successful snapshot is read. Multiple registration changes between polls are represented by one left/join pair; other intermediate changes may be combined; notifications describe observed state transitions, not a durable history.

On Redis errors, the registrar retains its last successful route snapshot, clears the observed leader, and emits a follower event if it was leader. Successful synchronization restores registration and applications as needed and reevaluates leadership. During an outage, route queries can therefore return stale information. A failed shutdown cleanup is completed by lease expiry and a later snapshot.

Redis failover can lose recently acknowledged writes because replication is asynchronous. Re-registration restores discovery, but the leader signal has Redis's consistency semantics and is not a guarantee of exclusive business execution.

## Storage

For each namespace, three keys share a hash tag derived from the base64url encoding of `Cluster`:

```text
ergo:{encoded-cluster}:sequence  # increasing registration sequence
ergo:{encoded-cluster}:members   # instance ID -> sequence and JSON record
ergo:{encoded-cluster}:leases    # instance ID -> expiry in Redis milliseconds
```

Lua atomically publishes, renews, removes, or cleans expired records and reads a snapshot. Each Ergo cluster's data occupies one Redis Cluster slot. Expiry is enforced when snapshots are read; with no active registrars, expired records remain until the next snapshot. The sequence counter remains after all nodes leave.

## Tests

Ordinary tests start an in-process Redis implementation and real Ergo nodes:

```sh
go test ./redis
go test -race ./redis
go vet ./redis
```

Run against real deployments by providing the corresponding variables:

```sh
REDIS_ENDPOINTS=127.0.0.1:6379 go test -race ./redis

REDIS_SENTINEL_ENDPOINTS=127.0.0.1:26379 \
REDIS_MASTER_NAME=mymaster \
go test -race ./redis -run TestRedisDeployments

REDIS_CLUSTER_ENDPOINTS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
go test -race ./redis -run TestRedisDeployments
```

On dedicated disposable deployments, add `REDIS_TEST_FAILOVER=1` to exercise Sentinel promotion, Cluster slot migration, and Cluster replica promotion. These tests change Redis topology; the Cluster must have a replica for every master. External tests skip when their endpoint variable is absent.
