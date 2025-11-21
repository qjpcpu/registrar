# ZooKeeper Registrar for Ergo Framework

[![GoDoc](https://godoc.org/ergo.services/registrar/zk?status.svg)](https://godoc.org/ergo.services/registrar/zk)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This package provides a `gen.Registrar` implementation for the [Ergo Framework](https://ergo.services/), leveraging [Apache ZooKeeper](https://zookeeper.apache.org/) for distributed service discovery, leader election, and real-time cluster eventing.

It enables Ergo nodes to form a cluster without a single point of failure, ensures automatic cleanup of failed nodes, and supports advanced configurations for modern containerized environments.

## Features

- **Distributed Service Discovery**: No central point of failure for node registration.
- **Leader Election**: Automatic leader election using ephemeral sequential znodes. This is ideal for singleton actors that must be globally unique across the cluster.
- **Real-time Events**: Near real-time cluster change notifications via ZooKeeper watchers.
- **Automatic Cleanup**: Ephemeral znodes ensure that disconnected or failed nodes are automatically removed from the cluster.
- **Security**: Supports ZooKeeper ACLs for secure access control.
- **High Availability**: Built on ZooKeeper's battle-tested distributed consensus algorithm.
- **Customizable Routes**: Supports advertising a different address for nodes, essential for NAT or containerized environments.
- **Cluster Namespace**: Allows multiple Ergo clusters to share the same ZooKeeper ensemble by using a `Cluster` name.

## Quick Start

To get started, create a ZooKeeper registrar instance and assign it to your node options.

```go
import "ergo.services/registrar/zk"

// Connect to a ZooKeeper ensemble
registrar, err := zk.Create(zk.Options{
    Endpoints: []string{"localhost:2181"},
    Cluster: "my-ergo-cluster",
})
if err != nil {
    log.Fatalf("Failed to create registrar: %v", err)
}

// Set the registrar in your node options
options.Network.Registrar = registrar

// Start the Ergo node
node, err := ergo.StartNode("demo@localhost", options)
```

## Advanced Usage

### NAT and Containerized Environments

When running nodes behind a NAT or in containers (like Docker or Kubernetes), you often need to advertise a public or reachable IP address. Use a `RoutesMapper` to modify the route information before it's published to ZooKeeper.

```go
// Advertise a specific IP and port
registrar, err := zk.Create(zk.Options{
    Endpoints: []string{"localhost:2181"},
    RoutesMapper: zk.MapRoutesByAdvertiseAddress("8.8.8.8", 8080),
})
```

### Leader Election and Role-Based Actors

The registrar automatically performs leader election. You can listen for role changes to create actors that run only on the leader node. This is useful for singleton services or cluster-wide management tasks.

```go
type MyLeaderActor struct {
    gen.Actor
}

func (a *MyLeaderActor) Init(pid gen.PID, args ...any) (gen.ActorState, error) {
    // ... initialize leader-specific state ...
    return state, nil
}

// ---

var leaderPID gen.PID

roleListener := func(role zk.RoleType) {
    if role == zk.Leader {
        // We are the leader, spawn the leader-specific actor
        leaderPID, _ = node.Spawn(&MyLeaderActor{}, gen.ProcessOptions{})
    } else {
        // We are a follower, stop the actor if it was running
        if leaderPID.IsAlive() {
            node.Kill(leaderPID)
        }
    }
}

registrar, err := zk.Create(zk.Options{
    Endpoints:     []string{"localhost:2181"},
    RoleChanged:   zk.OnRoleChangedFunc(roleListener),
})
```

### Authentication

You can provide authentication credentials to connect to a secured ZooKeeper cluster.

```go
registrar, err := zk.Create(zk.Options{
    Endpoints: []string{"secure-zk:2181"},
    Auth: zk.AuthConfig{
        Scheme:     "digest",
        Credential: "myuser:mypassword",
    },
})
```

### Custom Logging

By default, the registrar is silent. You can provide a custom logging function to integrate with your application's logging infrastructure.

```go
logFn := func(format string, v ...any) {
    log.Printf("[ZK-REGISTRAR] " + format, v...)
}

registrar, err := zk.Create(zk.Options{
    Endpoints: []string{"localhost:2181"},
    LogFn:     logFn,
})
```

## How It Works

This registrar uses ZooKeeper's core features to manage an Ergo cluster.

### Path Structure

- **Nodes**: `/services/ergo/{cluster}/nodes/{node-sequential}`
- **Applications**: `/services/ergo/{cluster}/apps/{app}/{node-sequential}`

The `{cluster}` segment is configurable via `zk.Options` and defaults to `"default"`.

### Node Discovery and Lifecyle

1.  **Registration**: When an Ergo node starts, it connects to ZooKeeper and creates an **ephemeral sequential znode** under the `/services/ergo/{cluster}/nodes/` path. The znode name is automatically assigned a suffix with an incrementing sequence number (e.g., `node-0000000001`). The znode's data contains the node's name, advertised routes, and other metadata.
2.  **Discovery**: Each node in the cluster places a **watch** on the `/nodes` path. When a new node joins or an existing one leaves, ZooKeeper notifies all watching nodes. They then re-fetch the list of children to get an updated view of the cluster.
3.  **Health Checking**: The use of ephemeral znodes provides a lease-based health checking mechanism. If a node disconnects from ZooKeeper and its session times out, its ephemeral znode is automatically deleted. This triggers the watch, notifying all other nodes that the node has left the cluster.

### Leader Election

Leader election is achieved using the sequential nature of the znodes created under the `/nodes` path. The node that owns the znode with the **lowest sequence number** is considered the **leader**. All nodes watch for changes and re-evaluate leadership whenever a node joins or leaves, ensuring a new leader is elected immediately if the current one fails.

### Application Route Discovery

Application routes are also registered as ephemeral sequential znodes under the `/apps` path. This allows for fine-grained discovery of specific services running on nodes and enables load balancing or direct communication with specific application instances.

## Testing

To run the tests, first export the `ZK_ENDPOINTS` environment variable:

```bash
export ZK_ENDPOINTS=localhost:2181
```

Then, run the tests:

```bash
go test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
