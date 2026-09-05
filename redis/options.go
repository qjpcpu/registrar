// Package redis provides Redis-backed registration and discovery for Ergo nodes.
package redis

import "time"

// Options configures the Redis connection and discovery intervals.
type Options struct {
	// Cluster isolates Ergo clusters sharing Redis. Defaults to "default".
	Cluster string
	// Endpoints contains Redis addresses, or Sentinel addresses with MasterName.
	Endpoints []string
	// DB selects the database for single-instance and Sentinel modes. Defaults to 0.
	DB int
	// Username and Password optionally authenticate to Redis.
	Username string
	Password string
	// MasterName selects Sentinel; Endpoints then contains Sentinel addresses.
	MasterName string
	// RedisCluster selects Redis Cluster and is mutually exclusive with MasterName.
	RedisCluster bool
	// SessionTimeout defaults to 10 seconds. Renewal runs every 30% of this duration.
	SessionTimeout time.Duration
	// PollInterval defaults to one second.
	PollInterval time.Duration
	// SupportRegisterApplication enables publishing and resolving applications.
	SupportRegisterApplication bool
}
