package zk

import (
	"encoding/json"
	"ergo.services/ergo/gen"
)

const (
	metaKeyID  = "id"
	metaKeySeq = "seq"
)

type Node struct {
	Name   gen.Atom          `json:"name"`
	Routes []gen.Route       `json:"routes"`
	Meta   map[string]string `json:"-"`
}

func NewNode(name gen.Atom, routes []gen.Route) *Node {
	return &Node{
		Name:   name,
		Routes: routes,
		Meta:   map[string]string{},
	}
}

func (n *Node) Equal(other *Node) bool {
	if n == nil || other == nil {
		return false
	}
	if n == other {
		return true
	}
	return n.Name == other.Name
}

func (n *Node) GetMeta(name string) (string, bool) {
	if n.Meta == nil {
		return "", false
	}
	val, ok := n.Meta[name]
	return val, ok
}

func (n *Node) GetSeq() int {
	if seqStr, ok := n.GetMeta(metaKeySeq); ok {
		return strToInt(seqStr)
	}
	return 0
}

func (n *Node) SetMeta(name string, val string) {
	if n.Meta == nil {
		n.Meta = map[string]string{}
	}
	n.Meta[name] = val
}

func (n *Node) Serialize() ([]byte, error) {
	data, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (n *Node) Deserialize(data []byte) error {
	return json.Unmarshal(data, n)
}
