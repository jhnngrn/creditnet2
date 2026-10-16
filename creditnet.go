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

// AppModuleFactory is the creditnet-specific module factory signature.
// Unlike mimesis.ModuleFactory, it receives trustlines and bandwidth
// from the Node module.
type AppModuleFactory func(
	nodeID string,
	schedule mimesis.ScheduleFunc,
	trustlines map[string]*Trustline,
	bandwidth BandwidthFunc,
	state json.RawMessage,
) mimesis.Module

// RegisterNode registers the trustline Node module on the server.
// This must be called before any Register calls, because other modules
// depend on the Node's trustlines and bandwidth.
func RegisterNode(srv *mimesis.Server) {
	srv.Register("trustline", func(nodeID string, links *mimesis.Links, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		return NewNode(nodeID, links, state)
	}, true)
}

// Register registers a creditnet app module factory on the server.
// The factory receives trustlines and bandwidth from the Node module,
// which must be registered first via RegisterNode.
func Register(srv *mimesis.Server, name string, factory AppModuleFactory) {
	srv.Register(name, func(nodeID string, links *mimesis.Links, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		mod := links.ModuleMap()["trustline"]
		if mod == nil {
			panic(fmt.Sprintf("creditnet.Register: trustline module not found for %s — call RegisterNode before Register", nodeID))
		}
		n := mod.(*Node)
		m := factory(nodeID, schedule, n.Trustlines, n.Bandwidth, state)
		if r, ok := m.(Reserver); ok {
			n.RegisterReserver(r)
		}
		return m
	}, false)
}
