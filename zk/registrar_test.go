package zk

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/zk"
)

func getTestEndpoints() ([]string, error) {
	endpoints := os.Getenv("ZK_ENDPOINTS")
	if endpoints == "" {
		return nil, errors.New("please set ZK_ENDPOINTS first")
	}
	return strings.Split(endpoints, ","), nil
}

func TestCreate(t *testing.T) {
	result := NewResult()
	node1 := startNode(t, result, "node1@localhost", "consumer")
	node2 := startNode(t, result, "node2@localhost", "producer")
	defer node1.StopForce()
	defer node2.StopForce()

	select {
	case <-time.After(time.Second * 10):
	case <-result.done:
	}

	mustTrue(t, result.sendHelloSuccess.Load(), "send `hello` fail")
	mustTrue(t, result.recvWorldSuccess.Load(), "receive `world` fail")
	mustTrue(t, result.recvNodeJoinEventSuccess.Load(), "receive `EventNodeJoined` fail")
}

func mustSuccess(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func mustTrue(t *testing.T, b bool, msg string) {
	if !b {
		t.Fatal(msg)
	}
}

type nozklog struct{}

func (nozklog) Printf(string, ...any) {}

const (
	roleProducer = "producer"
	roleConsumer = "consumer"
)

func startNode(t *testing.T, result *Result, nodeName, role string) gen.Node {
	var options gen.NodeOptions
	eds, err := getTestEndpoints()
	mustSuccess(t, err)
	registrar, err := Create(eds, WithCluster("basic_test"), WithZKOption(zk.WithLogger(nozklog{})))
	mustSuccess(t, err)
	options.Network.Registrar = registrar
	options.Network.Acceptors = []gen.AcceptorOptions{{Host: "0.0.0.0", TCP: "tcp"}}
	options.Network.Cookie = "test-cookie-123"
	apps := []gen.ApplicationBehavior{
		CreateApp(role, result),
	}
	options.Applications = apps
	options.Log.DefaultLogger.Disable = true

	node, err := ergo.StartNode(gen.Atom(nodeName), options)
	mustSuccess(t, err)
	return node
}

type Result struct {
	sendHelloSuccess         atomic.Bool
	recvWorldSuccess         atomic.Bool
	recvNodeJoinEventSuccess atomic.Bool
	check                    int32
	done                     chan struct{}
}

func NewResult() *Result {
	return &Result{done: make(chan struct{})}
}

func (r *Result) Check() {
	if atomic.AddInt32(&r.check, 1) == 3 {
		close(r.done)
	}
}

func CreateApp(role string, result *Result) gen.ApplicationBehavior {
	return &MyApp{role: role, result: result}
}

type MyApp struct {
	role   string
	result *Result
}

func (app *MyApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	return gen.ApplicationSpec{
		Name:        "myapp",
		Description: "test application with myactor",
		Mode:        gen.ApplicationModePermanent,
		Group: []gen.ApplicationMemberSpec{
			{
				Name:    "myactor",
				Factory: factory,
				Args:    []any{app.role, app.result},
			},
		},
	}, nil
}

func (app *MyApp) Start(mode gen.ApplicationMode) {}

func (app *MyApp) Terminate(reason error) {}

func factory() gen.ProcessBehavior {
	return &myActor{}
}

type myActor struct {
	act.Actor
	role   string
	result *Result
}

func (a *myActor) Init(args ...any) error {
	a.role = args[0].(string)
	a.result = args[1].(*Result)
	a.Send(a.PID(), "init")
	return nil
}

func (a *myActor) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case string:
		switch msg {
		case "init":
			if err := a.setupRegistrarMonitoring(); err != nil {
				a.Log().Error("Failed to setup registrar monitoring: %v", err)
			}
			a.Send(a.PID(), "send_msg")
		case "send_msg":
			if a.role == roleProducer {
				registrar, err := a.Node().Network().Registrar()
				if err != nil {
					a.SendAfter(a.PID(), "send_msg", 5*time.Second)
					return nil
				}

				resolver := registrar.Resolver()
				routes, err := resolver.ResolveApplication("myapp")
				if err != nil {
					a.SendAfter(a.PID(), "send_msg", 5*time.Second)
					return nil
				}
				if len(routes) == 0 {
					a.SendAfter(a.PID(), "send_msg", 5*time.Second)
					return nil
				}

				for _, route := range routes {
					if route.Node != a.Node().Name() {
						a.Send(gen.ProcessID{Name: gen.Atom("myactor"), Node: route.Node}, "hello")
						a.result.sendHelloSuccess.Store(true)
						a.result.Check()
					}
				}
			}
		case "hello":
			if a.role == roleConsumer {
				a.Send(from, "world")
			}
		case "world":
			if a.role == roleProducer {
				a.result.recvWorldSuccess.Store(true)
				a.result.Check()
			}
		}

	}

	return nil
}

func (a *myActor) setupRegistrarMonitoring() error {
	registrar, err := a.Node().Network().Registrar()
	if err != nil {
		return err
	}
	event, err := registrar.Event()
	if err != nil {
		return err
	}
	events, err := a.MonitorEvent(event)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		for _, existingEvent := range events {
			a.HandleEvent(existingEvent)
		}
	}
	return nil
}

func (a *myActor) HandleEvent(event gen.MessageEvent) error {
	switch event.Message.(type) {
	case EventNodeLeft:
	case EventNodeJoined:
		a.result.recvNodeJoinEventSuccess.Store(true)
		a.result.Check()
	}
	return nil
}
