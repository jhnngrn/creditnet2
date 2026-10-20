package creditnet

import (
	"encoding/json"
	"fmt"

	"github.com/jhnngrn/mimesis"
)

// Boot parses flags and creates a mimesis.Server.
// Call RegisterNode and then Register on the returned server before RunAndWait.
var Boot = mimesis.Boot

// RunAndWait initializes the server and blocks until interrupted.
var RunAndWait = mimesis.RunAndWait

// RegisterNode registers the creditnet Node module on the server.
// This must be called before any Register calls, because other modules
// depend on the Node's trustlines and bandwidth.
// children lists the module names that connect under creditnet.
func RegisterNode(srv *mimesis.Server, children []string) {
	srv.Register("creditnet", func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		return NewNode(name, nodeID, node, state)
	})
	srv.DeclareChildren("creditnet", children)
}

// Register registers a module factory on the server.
// If the module implements Reserver, it is automatically registered
// with the trustline Node.
func Register(srv *mimesis.Server, name string, factory mimesis.ModuleFactory) {
	srv.Register(name, func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		m := factory(name, nodeID, node, schedule, state)
		if r, ok := m.(Reserver); ok {
			mod := node.ModuleMap()["creditnet"]
			if mod == nil {
				panic(fmt.Sprintf("creditnet.Register: creditnet module not found for %s — call RegisterNode before Register", nodeID))
			}
			mod.(*Node).RegisterReserver(r)
		}
		return m
	})
}
