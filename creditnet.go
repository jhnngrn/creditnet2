package creditnet

import (
	"encoding/json"

	"github.com/jhnngrn/mimesis"
)

// Boot parses flags and creates a mimesis.Server.
// Call RegisterNode and then Register on the returned server before RunAndWait.
var Boot = mimesis.Boot

// RunAndWait initializes the server and blocks until interrupted.
var RunAndWait = mimesis.RunAndWait

// RegisterNode registers the trustline Node module on the server.
// This must be called before any Register calls, because other modules
// depend on the Node's trustlines and bandwidth.
// children lists the module names that run under the trustline module.
func RegisterNode(srv *mimesis.Server, children []string) {
	srv.DeclareRoots([]string{"trustline"})
	srv.Register("trustline", func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		return NewNode(name, nodeID, node, state)
	})
	if len(children) > 0 {
		srv.DeclareChildren("trustline", children)
	}
}

// Register registers a module factory on the server.
func Register(srv *mimesis.Server, name string, factory mimesis.ModuleFactory) {
	srv.Register(name, factory)
}
