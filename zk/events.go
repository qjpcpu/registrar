package zk

import (
	"fmt"

	"ergo.services/ergo/gen"
)

// Configuration update events
type EventConfigUpdate struct {
	Item  string
	Value any
}

// Node lifecycle events
type EventNodeJoined struct {
	Name gen.Atom
}

func (e EventNodeJoined) String() string {
	return fmt.Sprintf("EventNodeJoined<Name:%s>", e.Name.String())
}

type EventNodeLeft struct {
	Name gen.Atom
}

func (e EventNodeLeft) String() string {
	return fmt.Sprintf("EventNodeLeft<Name:%s>", e.Name.String())
}

type EventNodeSwitchedToLeader struct {
	Name gen.Atom
}

func (e EventNodeSwitchedToLeader) String() string {
	return fmt.Sprintf("EventNodeSwitchedToLeader<Name:%s>", e.Name.String())
}

type EventNodeSwitchedToFollower struct {
	Name gen.Atom
}

func (e EventNodeSwitchedToFollower) String() string {
	return fmt.Sprintf("EventNodeSwitchedToFollower<Name:%s>", e.Name.String())
}

type EventNodeRoleHeartbeat struct {
	Name gen.Atom
	Role string
}

func (e EventNodeRoleHeartbeat) String() string {
	return fmt.Sprintf("EventNodeRoleHeartbeat<Name:%s,Role:%s>", e.Name.String(), e.Role)
}

// Application lifecycle events
type EventApplicationLoaded struct {
	Name   gen.Atom
	Node   gen.Atom
	Weight int
}

func (e EventApplicationLoaded) String() string {
	return fmt.Sprintf("EventApplicationLoaded<App:%s,Node:%s,Weight:%d>", e.Name.String(), e.Node.String(), e.Weight)
}

type EventApplicationStarted struct {
	Name   gen.Atom
	Node   gen.Atom
	Weight int
	Mode   gen.ApplicationMode
}

func (e EventApplicationStarted) String() string {
	return fmt.Sprintf("EventApplicationStarted<App:%s,Node:%s,Weight:%d,Mode:%s>", e.Name.String(), e.Node.String(), e.Weight, e.Mode.String())
}

type EventApplicationStopping struct {
	Name gen.Atom
	Node gen.Atom
}

func (e EventApplicationStopping) String() string {
	return fmt.Sprintf("EventApplicationStopping<App:%s,Node:%s>", e.Name.String(), e.Node.String())
}

type EventApplicationStopped struct {
	Name gen.Atom
	Node gen.Atom
}

func (e EventApplicationStopped) String() string {
	return fmt.Sprintf("EventApplicationStopped<App:%s,Node:%s>", e.Name.String(), e.Node.String())
}

type EventApplicationUnloaded struct {
	Name gen.Atom
	Node gen.Atom
}

func (e EventApplicationUnloaded) String() string {
	return fmt.Sprintf("EventApplicationUnloaded<App:%s,Node:%s>", e.Name.String(), e.Node.String())
}
