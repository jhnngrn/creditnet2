package creditnet

import (
	"encoding/json"
	"log"
	"time"

	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Node — thin shell that wires Links to transport and persistence
// ---------------------------------------------------------------------------

type Node struct {
	id    string
	links *link.Links
	store Store
}

type nodeState struct {
	ID         string                     `json:"id"`
	Links      map[string]*link.Link      `json:"links"`
	InviteMode link.InviteMode            `json:"invite_mode,omitempty"`
	Handlers   map[string]json.RawMessage `json:"handlers,omitempty"`
}

func newNode(id string, raw json.RawMessage, transport Transport, store Store) *Node {
	var st nodeState
	if raw != nil {
		json.Unmarshal(raw, &st)
	}
	n := &Node{id: id, store: store}

	commit := func() {
		s := nodeState{ID: n.id, Links: n.links.Map(), InviteMode: n.links.InviteMode}
		s.Handlers = make(map[string]json.RawMessage)
		for name, mod := range n.links.ModuleMap() {
			if data, err := json.Marshal(mod); err == nil {
				s.Handlers[name] = data
			}
		}
		if err := store.SaveNode(n.id, s); err != nil {
			log.Fatalf("node %s: persist failed: %v", n.id, err)
		}
	}

	send := func(peer string, msg link.SyncRequest) ([]byte, error) {
		return transport.Send(PeerDomain(peer), msg)
	}

	n.links = link.NewLinks(id, st.Links, st.InviteMode,
		map[string]struct{}{"trustline": {}},
		link.EngineConfig{
			Send:   send,
			Commit: commit,
			Now:    NowSecs,
		},
	)
	return n
}

// ---------------------------------------------------------------------------
// Public API — delegates to Links
// ---------------------------------------------------------------------------

func (n *Node) ID() string        { return n.id }
func (n *Node) Links() *link.Links { return n.links }

// Lock/Unlock/Commit delegate to Links for callers that need direct access.
func (n *Node) Lock()   { n.links.Lock() }
func (n *Node) Unlock() { n.links.Unlock() }
func (n *Node) Commit() { n.links.Commit() }

// RegisterModule registers a module with the link engine.
func (n *Node) RegisterModule(m link.Module) { n.links.RegisterModule(m) }

// ScheduleFunc returns a ScheduleFunc bound to this node's Links.
func (n *Node) ScheduleFunc() ScheduleFunc {
	return func(delay time.Duration, fn func() []string) {
		n.links.Schedule(delay, fn)
	}
}
