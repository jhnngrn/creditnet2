package link

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Node — sync engine and dispatcher
// ---------------------------------------------------------------------------

type Node struct {
	mu           sync.Mutex
	id           string
	links        map[string]*Link
	inFlight     map[string]bool
	allowInvites bool

	handlers      []handlerEntry
	handlerMap    map[string]Handler
	handlerStates map[string]json.RawMessage

	transport Transport
	store     NodeStore
}

type handlerEntry struct {
	name    string
	handler Handler
}

type nodeState struct {
	ID           string                     `json:"id"`
	Links        map[string]*Link           `json:"links"`
	AllowInvites *bool                      `json:"allow_invites,omitempty"`
	Handlers     map[string]json.RawMessage `json:"handlers,omitempty"`
}

func NewNode(id string, raw json.RawMessage, transport Transport, store NodeStore) *Node {
	var st nodeState
	if raw != nil {
		json.Unmarshal(raw, &st)
	}
	if st.Links == nil {
		st.Links = map[string]*Link{}
	}
	invites := true
	if st.AllowInvites != nil {
		invites = *st.AllowInvites
	}
	return &Node{
		id:            id,
		links:         st.Links,
		inFlight:      map[string]bool{},
		allowInvites:  invites,
		handlerMap:    map[string]Handler{},
		handlerStates: st.Handlers,
		transport:     transport,
		store:         store,
	}
}

// ---------------------------------------------------------------------------
// Handler registration
// ---------------------------------------------------------------------------

func (n *Node) RegisterHandler(name string, h Handler) {
	n.handlers = append(n.handlers, handlerEntry{name: name, handler: h})
	n.handlerMap[name] = h
}

// HandlerState returns the persisted state for a handler namespace.
// Used by callers to pass saved state when constructing a handler.
func (n *Node) HandlerState(name string) json.RawMessage {
	if n.handlerStates == nil {
		return nil
	}
	return n.handlerStates[name]
}

// Links returns the node's links map. Caller must hold Lock.
func (n *Node) Links() map[string]*Link {
	return n.links
}

// DeleteLink removes a link. Caller must hold Lock.
func (n *Node) DeleteLink(peer string) {
	delete(n.links, peer)
}

// RefLink increments the reference count on a link. Caller must hold Lock.
func (n *Node) RefLink(peer string) {
	if lk := n.links[peer]; lk != nil {
		lk.Refs++
	}
}

// UnrefLink decrements the reference count. Caller must hold Lock.
func (n *Node) UnrefLink(peer string) {
	if lk := n.links[peer]; lk != nil && lk.Refs > 0 {
		lk.Refs--
	}
}

// ---------------------------------------------------------------------------
// Locking & persistence
// ---------------------------------------------------------------------------

func (n *Node) Lock()         { n.mu.Lock() }
func (n *Node) Unlock()       { n.mu.Unlock() }
func (n *Node) ID() string    { return n.id }
func (n *Node) AllowInvites() bool { return n.allowInvites }

// HandleAccount dispatches a user-facing API call by namespace.
// Caller must hold Lock.
func (n *Node) HandleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	ns, method := SplitMethod(fullMethod)
	handler := n.handlerMap[ns]
	if handler == nil {
		return nil, ErrBadMethod
	}
	ah, ok := handler.(AccountHandler)
	if !ok {
		return nil, ErrBadMethod
	}
	return ah.HandleAccount(method, payload)
}

// Commit persists the node state. Caller must hold Lock.
func (n *Node) Commit() error {
	ai := n.allowInvites
	st := nodeState{ID: n.id, Links: n.links, AllowInvites: &ai}
	if len(n.handlers) > 0 {
		st.Handlers = make(map[string]json.RawMessage)
		for _, entry := range n.handlers {
			if data, err := json.Marshal(entry.handler); err == nil {
				st.Handlers[entry.name] = data
			}
		}
	}
	if err := n.store.Save(n.id, st); err != nil {
		// Rollback from persisted state.
		raw, lerr := n.store.Load(n.id)
		if lerr != nil {
			log.Fatalf("node %s: save failed and cannot reload state: %v (save: %v)", n.id, lerr, err)
		}
		var reloaded nodeState
		if json.Unmarshal(raw, &reloaded) == nil {
			if reloaded.Links != nil {
				n.links = reloaded.Links
			}
			if reloaded.AllowInvites != nil {
				n.allowInvites = *reloaded.AllowInvites
			}
			for _, entry := range n.handlers {
				if state, ok := reloaded.Handlers[entry.name]; ok {
					entry.handler.ResetState(state)
				}
			}
		}
		return err
	}
	return nil
}

func (n *Node) Kick(peer string) { go n.sendPending(peer) }

// ---------------------------------------------------------------------------
// Incoming sync
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect).
func (n *Node) QuickReject(from, method string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.links[from] == nil && method != "link.connect"
}

// HandleSync processes an incoming sync request. The caller is responsible
// for authentication (signature verification).
func (n *Node) HandleSync(env SyncRequest) (*SyncResponse, error) {
	n.mu.Lock()

	link := n.links[env.From]
	if link == nil {
		if env.Method == "link.remove" {
			n.mu.Unlock()
			return &SyncResponse{Counter: env.Counter, OK: true, Timestamp: NowSecs()}, nil
		}
		if env.Method != "link.connect" {
			n.mu.Unlock()
			return nil, ErrNoLink
		}
		if !n.allowInvites {
			n.mu.Unlock()
			return nil, ErrInvitesDisabled
		}
		if len(n.links) >= MaxLinks {
			n.mu.Unlock()
			return nil, ErrMaxLinks
		}
		link = NewLink(n.id, env.From)
		link.State.Status = Incoming
		n.links[env.From] = link
	}

	collision := link.Pending != nil

	// Piggybacked response.
	if env.Resp != nil && link.Pending != nil && env.Resp.Counter == link.Counter {
		pns, pmethod := SplitMethod(link.Pending.Method)
		if ph := n.handlerMap[pns]; ph != nil {
			ph.HandleResponse(env.From, link, pmethod, link.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		}
		link.Counter++
		link.RespTimestamp = env.Resp.Timestamp
		link.Pending = nil
		if n.links[env.From] != nil {
			if msg := n.nextTx(env.From); msg != nil {
				link.Pending = msg
			}
		}
		if err := n.Commit(); err != nil {
			n.mu.Unlock()
			return nil, ErrPersistFailed
		}
		collision = false
	}

	// Counter checks.
	if env.Counter < link.Counter {
		if cr := link.CachedResponse; cr != nil && cr.Counter == env.Counter {
			resp := *cr
			n.mu.Unlock()
			return &resp, nil
		}
		n.mu.Unlock()
		return nil, ErrStaleCounter
	}
	if env.Counter > link.Counter {
		n.mu.Unlock()
		return nil, ErrCounterAhead
	}
	if collision && link.Dominant() {
		n.mu.Unlock()
		return nil, ErrCollision
	}

	ts := env.Timestamp
	if link.Dominant() {
		ts = NowSecs()
	}

	// Dispatch by namespace.
	ns, method := SplitMethod(env.Method)
	handler := n.handlerMap[ns]
	if handler == nil {
		n.mu.Unlock()
		return nil, ErrBadMethod
	}
	ok, reqErr := handler.HandleRequest(env.From, link, method, env.Payload, ts)
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}

	link.Counter++
	link.ReqTimestamp = ts
	if collision {
		link.Pending = nil
	}

	resp := SyncResponse{Counter: env.Counter, OK: ok, Timestamp: ts}
	link.CachedResponse = &resp
	if err := n.Commit(); err != nil {
		n.mu.Unlock()
		return nil, ErrPersistFailed
	}
	n.mu.Unlock()
	n.Kick(env.From)
	return &resp, nil
}

// ---------------------------------------------------------------------------
// Outgoing sync
// ---------------------------------------------------------------------------

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	link := n.links[peer]
	if link == nil || n.inFlight[peer] {
		n.mu.Unlock()
		return
	}
	promoted := false
	if link.Pending == nil {
		msg := n.nextTx(peer)
		if msg == nil {
			n.mu.Unlock()
			return
		}
		link.Pending = msg
		promoted = true
	}
	n.inFlight[peer] = true
	msg := link.Pending
	counter := link.Counter
	env := SyncRequest{
		From: n.id, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
		Timestamp: NowSecs(),
	}
	if cr := link.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		if err := n.Commit(); err != nil {
			delete(n.inFlight, peer)
			n.mu.Unlock()
			return
		}
	}
	n.mu.Unlock()

	body, err := n.transport.Send(PeerDomain(peer), env)

	n.mu.Lock()
	delete(n.inFlight, peer)
	if err != nil {
		log.Printf("[link] %s->%s %s: %v", n.id, peer, msg.Method, err)
		n.mu.Unlock()
		return
	}
	var resp SyncResponse
	if json.Unmarshal(body, &resp) != nil {
		n.mu.Unlock()
		return
	}
	link = n.links[peer]
	if link == nil || link.Pending != msg || link.Counter != counter || resp.Counter != counter {
		n.mu.Unlock()
		return
	}

	rns, rmethod := SplitMethod(msg.Method)
	if rh := n.handlerMap[rns]; rh != nil {
		rh.HandleResponse(peer, link, rmethod, msg.Payload, resp.OK, resp.Timestamp)
	}
	link.Counter++
	link.RespTimestamp = resp.Timestamp
	link.Pending = nil

	var kickPeer bool
	if n.links[peer] != nil {
		if next := n.nextTx(peer); next != nil {
			link.Pending = next
			kickPeer = true
		}
	}
	if err := n.Commit(); err != nil {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	if kickPeer {
		n.Kick(peer)
	}
}

// nextTx returns the next message to send for a peer, asking each
// handler in registration order. The namespace prefix is added.
func (n *Node) nextTx(peer string) *QueuedMsg {
	link := n.links[peer]
	if link == nil {
		return nil
	}
	for _, entry := range n.handlers {
		if msg := entry.handler.NextTx(peer, link); msg != nil {
			msg.Method = entry.name + "." + msg.Method
			return msg
		}
	}
	return nil
}

// RetryPending kicks all peers that aren't already in flight.
func (n *Node) RetryPending() {
	n.mu.Lock()
	var kicks []string
	for peer := range n.links {
		if !n.inFlight[peer] {
			kicks = append(kicks, peer)
		}
	}
	n.mu.Unlock()
	for _, p := range kicks {
		n.Kick(p)
	}
}

// ---------------------------------------------------------------------------
// Account operations (caller must hold Lock)
// ---------------------------------------------------------------------------

func (n *Node) AddLink(peer string) error {
	if _, _, err := ParseIdentity(peer); err != nil {
		return err
	}
	peer = strings.ToLower(peer)
	if peer == n.id {
		return fmt.Errorf("cannot link to self")
	}
	if n.links[peer] != nil {
		return fmt.Errorf("link to %s already exists", peer)
	}
	if len(n.links) >= MaxLinks {
		return fmt.Errorf("max links reached")
	}
	lk := NewLink(n.id, peer)
	lk.State.Status = Waiting
	lk.State.Intent = IntentConnect
	n.links[peer] = lk
	n.Kick(peer)
	return nil
}

func (n *Node) RemoveLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.Refs > 0 {
		return fmt.Errorf("link to %s still in use (%d refs)", peer, link.Refs)
	}
	link.State.Intent = IntentTeardown
	n.Kick(peer)
	return nil
}

func (n *Node) AcceptLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.State.Status != Incoming {
		return fmt.Errorf("link to %s is not incoming", peer)
	}
	link.State.Status = Waiting
	link.State.Intent = IntentConnect
	n.Kick(peer)
	return nil
}

func (n *Node) RejectLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.State.Status != Incoming {
		return fmt.Errorf("link to %s is not incoming", peer)
	}
	link.State.Intent = IntentTeardown
	n.Kick(peer)
	return nil
}

func (n *Node) SetAllowInvites(enabled bool) {
	n.allowInvites = enabled
}

func (n *Node) Status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.links {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	return Enc(map[string]any{
		"links":         links,
		"allow_invites": n.allowInvites,
	})
}
