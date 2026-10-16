package creditnet

import (
	"encoding/json"
	"sync"

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

// nodeRegistry holds per-node Node references so that subsequent module
// factories can access the Node's trustlines and bandwidth.
var (
	nodesMu sync.Mutex
	nodes   = map[string]*Node{}
)

func storeNode(nodeID string, n *Node) {
	nodesMu.Lock()
	nodes[nodeID] = n
	nodesMu.Unlock()
}

func getNode(nodeID string) *Node {
	nodesMu.Lock()
	defer nodesMu.Unlock()
	return nodes[nodeID]
}

// RegisterNode registers the trustline Node module on the server.
// This must be called before any Register calls, because other modules
// depend on the Node's trustlines and bandwidth.
func RegisterNode(srv *mimesis.Server) {
	srv.Register("trustline", func(nodeID string, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		// We need the Links for this node. The Node module receives
		// its Links via NewNode — but at registration time we don't
		// have the Links yet. The mimesis server calls the factory
		// from newLinks, and by then the Links is being built.
		//
		// The Node needs a *mimesis.Links to call Get/Create/Full/Map etc.
		// But mimesis.newLinks creates the Links and then calls factories.
		// The factory receives (nodeID, schedule, state) — no Links.
		//
		// Solution: create Node without Links, store it, and set Links later
		// via a SetLinks method. The Links ref is set before any sync happens.
		n := NewNode(nodeID, nil, state)
		storeNode(nodeID, n)
		return n
	}, true)
}

// Register registers a creditnet app module factory on the server.
// The factory receives trustlines and bandwidth from the Node module,
// which must be registered first via RegisterNode.
func Register(srv *mimesis.Server, name string, factory AppModuleFactory) {
	srv.Register(name, func(nodeID string, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
		n := getNode(nodeID)
		mod := factory(nodeID, schedule, n.Trustlines, n.Bandwidth, state)
		// If the module is a Reserver, register it with the Node.
		if r, ok := mod.(Reserver); ok {
			n.RegisterReserver(r)
		}
		return mod
	}, false)
}
