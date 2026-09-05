package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"ergo.services/ergo/gen"
	goredis "github.com/redis/go-redis/v9"
)

type registration struct {
	Name   gen.Atom               `json:"name"`
	Routes []gen.Route            `json:"routes"`
	Apps   []gen.ApplicationRoute `json:"apps"`
}

type member struct {
	ID  string
	Seq int64
	registration
}

type store struct {
	client goredis.UniversalClient
	keys   []string
}

func newStore(client goredis.UniversalClient, cluster string) *store {
	prefix := "ergo:{" + base64.RawURLEncoding.EncodeToString([]byte(cluster)) + "}:"
	return &store{client: client, keys: []string{prefix + "sequence", prefix + "members", prefix + "leases"}}
}

// A live retry preserves its sequence, including when the previous reply was lost.
var writeScript = goredis.NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local deadline = redis.call('ZSCORE', KEYS[3], ARGV[1])
local old = redis.call('HGET', KEYS[2], ARGV[1])
local seq
if old and deadline and tonumber(deadline) > now then
 seq = cjson.decode(old).seq
else
 seq = redis.call('INCR', KEYS[1])
end
redis.call('HSET', KEYS[2], ARGV[1], cjson.encode({seq=seq, data=ARGV[2]}))
redis.call('ZADD', KEYS[3], now+tonumber(ARGV[3]), ARGV[1])
return seq
`)

var snapshotScript = goredis.NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now)
for _, id in ipairs(expired) do
 redis.call('HDEL', KEYS[2], id)
 redis.call('ZREM', KEYS[3], id)
end
return redis.call('HGETALL', KEYS[2])
`)

var removeScript = goredis.NewScript(`
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

func (s *store) write(ctx context.Context, id string, record registration, ttl time.Duration) error {
	data, err := encodeRegistration(record)
	if err != nil {
		return err
	}
	return writeScript.Run(ctx, s.client, s.keys, id, string(data), ttl.Milliseconds()).Err()
}

func (s *store) snapshot(ctx context.Context) ([]member, error) {
	values, err := snapshotScript.Run(ctx, s.client, s.keys).Slice()
	if err != nil {
		return nil, err
	}
	members := make([]member, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		var envelope struct {
			Seq  int64  `json:"seq"`
			Data string `json:"data"`
		}
		if err := json.Unmarshal([]byte(values[i+1].(string)), &envelope); err != nil {
			return nil, err
		}
		m := member{ID: values[i].(string), Seq: envelope.Seq}
		if err := decodeRegistration([]byte(envelope.Data), &m.registration); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, nil
}

func (s *store) remove(ctx context.Context, id string) error {
	return removeScript.Run(ctx, s.client, s.keys, id).Err()
}

// Ergo's mode and state marshal to display strings without matching decoders.
// Store their numeric values, as the ZooKeeper registrar does.
type applicationRecord struct {
	Name   gen.Atom   `json:"name"`
	Node   gen.Atom   `json:"node"`
	Weight int        `json:"weight"`
	Mode   int        `json:"mode"`
	State  int32      `json:"state"`
	Tags   []gen.Atom `json:"tags,omitempty"`
}
type registrationRecord struct {
	Name   gen.Atom            `json:"name"`
	Routes []gen.Route         `json:"routes"`
	Apps   []applicationRecord `json:"apps"`
}

func encodeRegistration(r registration) ([]byte, error) {
	wire := registrationRecord{Name: r.Name, Routes: r.Routes}
	for _, a := range r.Apps {
		wire.Apps = append(wire.Apps, applicationRecord{Name: a.Name, Node: a.Node, Weight: a.Weight, Mode: int(a.Mode), State: int32(a.State), Tags: a.Tags})
	}
	return json.Marshal(wire)
}
func decodeRegistration(data []byte, r *registration) error {
	var wire registrationRecord
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*r = registration{Name: wire.Name, Routes: wire.Routes}
	for _, a := range wire.Apps {
		r.Apps = append(r.Apps, gen.ApplicationRoute{Name: a.Name, Node: a.Node, Weight: a.Weight, Mode: gen.ApplicationMode(a.Mode), State: gen.ApplicationState(a.State), Tags: a.Tags})
	}
	return nil
}
