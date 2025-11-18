# ZooKeeper Registrar

A `gen.Registrar` implementation for [Apache ZooKeeper](https://zookeeper.apache.org/) - a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services.

## Features

- **Distributed Service Discovery**: No single point of failure.
- **Real-time Events**: Cluster change notifications via watchers.
- **Automatic Cleanup**: Ephemeral nodes for lease-based node registration.
- **Security**: Supports ZooKeeper ACLs for access control.
- **High Availability**: Built on ZooKeeper's proven consensus algorithm.
- **Advertised Address**: Supports advertising a different address for nodes, useful in NAT or containerized environments.

## Quick Start

```go
import "ergo.services/registrar/zk"

registrar, err := zk.Create([]string{"localhost:2181"})

options.Network.Registrar = registrar
node, err := ergo.StartNode("demo@localhost", options)
```

If you are in a NAT environment, you can register an advertised address for each node:

```go
registrar, err := zk.Create([]string{"localhost:2181"}, zk.WithAdvertiseAddress("8.8.8.8", 8080))
```

## Path Structure

### Routes & Service Discovery
- **Nodes**: `/services/ergo/{cluster}/nodes/{node}`
- **Applications**: `/services/ergo/{cluster}/apps/{app}/{node}`

## Testing

To run the tests, first export the `ZK_ENDPOINTS` environment variable:

```bash
export ZK_ENDPOINTS=localhost:2181
```

Then, run the tests:

```bash
go test
```

## Documentation

TBD
