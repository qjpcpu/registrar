package redis

import (
	"reflect"
	"sort"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/events"
)

func cloneApp(app gen.ApplicationRoute) gen.ApplicationRoute {
	app.Tags = append([]gen.Atom(nil), app.Tags...)
	return app
}

func (c *client) applySnapshot(snapshot []member) {
	members := make(map[gen.Atom]member)
	for _, m := range snapshot {
		old, ok := members[m.Name]
		if !ok || m.Seq > old.Seq {
			members[m.Name] = m
		}
	}
	var leader member
	apps := make(map[gen.Atom][]gen.ApplicationRoute)
	type appKey struct{ name, node gen.Atom }
	newApps := make(map[appKey]gen.ApplicationRoute)
	for _, m := range members {
		if leader.ID == "" || m.Seq < leader.Seq {
			leader = m
		}
		if c.options.SupportRegisterApplication {
			for _, app := range m.Apps {
				newApps[appKey{app.Name, app.Node}] = app
			}
		}
	}
	for _, app := range newApps {
		apps[app.Name] = append(apps[app.Name], app)
	}
	for _, routes := range apps {
		sort.Slice(routes, func(i, j int) bool { return routes[i].Node < routes[j].Node })
	}
	var notifications []any
	c.mu.Lock()
	for name, current := range members {
		if name == c.local.Name {
			continue
		}
		if old, ok := c.members[name]; !ok {
			notifications = append(notifications, events.EventNodeJoined{Name: name})
		} else if old.ID != current.ID || old.Seq != current.Seq {
			notifications = append(notifications,
				events.EventNodeLeft{Name: name},
				events.EventNodeJoined{Name: name},
			)
		}
	}
	for name := range c.members {
		if _, ok := members[name]; !ok && name != c.local.Name {
			notifications = append(notifications, events.EventNodeLeft{Name: name})
		}
	}
	oldApps := make(map[appKey]gen.ApplicationRoute)
	for _, routes := range c.appRoutes {
		for _, app := range routes {
			oldApps[appKey{app.Name, app.Node}] = app
		}
	}
	for key, app := range newApps {
		if old, ok := oldApps[key]; ok && reflect.DeepEqual(old, app) {
			continue
		}
		switch app.State {
		case gen.ApplicationStateLoaded:
			notifications = append(notifications, events.EventApplicationLoaded{Name: app.Name, Node: app.Node, Weight: app.Weight})
		case gen.ApplicationStateRunning:
			notifications = append(notifications, events.EventApplicationStarted{Name: app.Name, Node: app.Node, Weight: app.Weight, Mode: app.Mode})
		case gen.ApplicationStateStopping:
			notifications = append(notifications, events.EventApplicationStopping{Name: app.Name, Node: app.Node})
		}
	}
	for key := range oldApps {
		if _, ok := newApps[key]; !ok {
			notifications = append(notifications, events.EventApplicationStopped{Name: key.name, Node: key.node})
		}
	}
	isLeader := leader.ID == c.id
	if isLeader != c.isLeader {
		if isLeader {
			notifications = append(notifications, events.EventNodeSwitchedToLeader{Name: c.local.Name})
		} else {
			notifications = append(notifications, events.EventNodeSwitchedToFollower{Name: c.local.Name})
		}
	}
	c.members, c.appRoutes, c.leader, c.isLeader = members, apps, leader.Name, isLeader
	c.mu.Unlock()
	c.notify(notifications)
}

func (c *client) demote() {
	c.mu.Lock()
	wasLeader := c.isLeader
	c.isLeader = false
	c.leader = ""
	name := c.local.Name
	c.mu.Unlock()
	if wasLeader {
		c.notify([]any{events.EventNodeSwitchedToFollower{Name: name}})
	}
}

func (c *client) notify(messages []any) {
	for _, message := range messages {
		if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, message); err != nil {
			c.node.Log().Error("(registrar/redis) send event failed: %v", err)
		}
	}
}
