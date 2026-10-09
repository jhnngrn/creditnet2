package link

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const retryDelay = 60 * time.Second

// ---------------------------------------------------------------------------
// Node — sync engine and dispatcher
// ---------------------------------------------------------------------------

type Node struct {
	mu           sync.Mutex
	id           string
	links        map[string]*Link
	sends        map[string]*peerSend
	allowInvites bool

	handlers      []handlerEntry
	handlerMap    map[string]Handler
	handlerStates map[string]json.RawMessage

	owners map[string]LinkOwner

	transport Transport
	store     NodeStore
}

// peerSend tracks the outgoing send lifecycle for a peer at a given counter.
// inFlight means a goroutine is blocked on Transport.Send.
// After the send completes, a retry timer is set: refractory after error
// (blocks kicks), non-refractory after success (fallback, kicks bypass).
// Both are scoped to counter — if counter advances via HandleSync,
// the entry is stale and ignored.
type peerSend struct {
	counter    uint64
	inFlight   bool
	timer      *time.Timer
	refractory bool
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
		sends:         map[string]*peerSend{},
		allowInvites:  invites,
		handlerMap:    map[string]Handler{},
		handlerStates: st.Handlers,
		owners:        map[string]LinkOwner{},
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

// RegisterOwner registers lifecycle callbacks for a link protocol.
func (n *Node) RegisterOwner(name string, owner LinkOwner) {
	n.owners[name] = owner
}

// NotifyLinkConnected calls the OnConnect callback for the link's owner.
// Caller must hold Lock.
func (n *Node) NotifyLinkConnected(peer string) {
	lk := n.links[peer]
	if lk == nil || lk.Owner == "" {
		return
	}
	if owner, ok := n.owners[lk.Owner]; ok && owner.OnConnect != nil {
		owner.OnConnect(peer, lk)
	}
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

// DeleteLink removes a link, calling OnClose first. Caller must hold Lock.
func (n *Node) DeleteLink(peer string) {
	lk := n.links[peer]
	if lk != nil && lk.Owner != "" {
		if owner, ok := n.owners[lk.Owner]; ok && owner.OnClose != nil {
			owner.OnClose(peer, lk)
		}
	}
	if s := n.sends[peer]; s != nil {
		if s.timer != nil {
			s.timer.Stop()
		}
		delete(n.sends, peer)
	}
	delete(n.links, peer)
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

// setRetry sets a per-peer retry timer. Caller must hold Lock.
func (n *Node) setRetry(peer string, counter uint64, refractory bool) {
	if s := n.sends[peer]; s != nil && s.timer != nil {
		s.timer.Stop()
	}
	n.sends[peer] = &peerSend{
		counter:    counter,
		refractory: refractory,
		timer: time.AfterFunc(retryDelay, func() {
			n.mu.Lock()
			delete(n.sends, peer)
			n.mu.Unlock()
			n.sendPending(peer)
		}),
	}
}

// ---------------------------------------------------------------------------
// Incoming sync
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect/close).
func (n *Node) QuickReject(from, method string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.links[from] == nil && method != "link.connect" && method != "link.close"
}

// HandleSync processes an incoming sync request. The caller is responsible
// for authentication (signature verification).
func (n *Node) HandleSync(env SyncRequest) (*SyncResponse, error) {
	n.mu.Lock()

	link := n.links[env.From]
	if link == nil {
		if env.Method == "link.close" {
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
		// Read protocol from connect payload.
		var cp struct {
			Protocol string `json:"protocol"`
		}
		if env.Payload != nil {
			json.Unmarshal(env.Payload, &cp)
		}
		if _, ok := n.owners[cp.Protocol]; !ok {
			n.mu.Unlock()
			return nil, fmt.Errorf("unknown protocol %q", cp.Protocol)
		}
		link = NewLink(n.id, env.From)
		link.State.Status = Incoming
		link.Owner = cp.Protocol
		n.links[env.From] = link
	}

	hasPending := link.Pending != nil
	var extraKicks []string

	// Piggybacked response.
	if env.Resp != nil && link.Pending != nil && env.Resp.Counter == link.Counter {
		pns, pmethod := SplitMethod(link.Pending.Method)
		if ph := n.handlerMap[pns]; ph != nil {
			extraKicks = append(extraKicks, ph.HandleResponse(env.From, link, pmethod, link.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)...)
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
		hasPending = false
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
	if hasPending && link.Dominant() {
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
	ok, kicks, reqErr := handler.HandleRequest(env.From, link, method, env.Payload, ts)
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}
	extraKicks = append(extraKicks, kicks...)

	link.Counter++
	link.ReqTimestamp = ts
	link.Pending = nil

	resp := SyncResponse{Counter: env.Counter, OK: ok, Timestamp: ts}
	link.CachedResponse = &resp
	if err := n.Commit(); err != nil {
		n.mu.Unlock()
		return nil, ErrPersistFailed
	}
	n.mu.Unlock()
	n.Kick(env.From)
	for _, k := range extraKicks {
		n.Kick(k)
	}
	return &resp, nil
}

// ---------------------------------------------------------------------------
// Outgoing sync
// ---------------------------------------------------------------------------

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	link := n.links[peer]
	if link == nil {
		n.mu.Unlock()
		return
	}
	if s := n.sends[peer]; s != nil && s.counter == link.Counter {
		if s.inFlight || s.refractory {
			n.mu.Unlock()
			return
		}
		s.timer.Stop()
		delete(n.sends, peer)
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
	counter := link.Counter
	n.sends[peer] = &peerSend{counter: counter, inFlight: true}
	msg := link.Pending
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
			delete(n.sends, peer)
			n.mu.Unlock()
			return
		}
	}
	n.mu.Unlock()

	body, err := n.transport.Send(PeerDomain(peer), env)

	n.mu.Lock()
	if err != nil {
		log.Printf("[link] %s->%s %s: %v", n.id, peer, msg.Method, err)
		n.setRetry(peer, counter, true)
		n.mu.Unlock()
		return
	}
	var resp SyncResponse
	if json.Unmarshal(body, &resp) != nil {
		n.setRetry(peer, counter, true)
		n.mu.Unlock()
		return
	}
	link = n.links[peer]
	if link == nil || link.Pending != msg || link.Counter != counter || resp.Counter != counter {
		delete(n.sends, peer)
		n.mu.Unlock()
		return
	}

	var extraKicks []string
	rns, rmethod := SplitMethod(msg.Method)
	if rh := n.handlerMap[rns]; rh != nil {
		extraKicks = rh.HandleResponse(peer, link, rmethod, msg.Payload, resp.OK, resp.Timestamp)
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
		delete(n.sends, peer)
		n.mu.Unlock()
		return
	}
	n.setRetry(peer, counter, false)
	n.mu.Unlock()
	if kickPeer {
		n.Kick(peer)
	}
	for _, k := range extraKicks {
		n.Kick(k)
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
		if entry.name != "link" && link.State.Status != Connected {
			continue
		}
		if msg := entry.handler.NextTx(peer, link); msg != nil {
			msg.Method = entry.name + "." + msg.Method
			return msg
		}
	}
	return nil
}

// KickAll kicks all peers. Used at boot to resume pending work.
func (n *Node) KickAll() {
	n.mu.Lock()
	peers := make([]string, 0, len(n.links))
	for peer := range n.links {
		peers = append(peers, peer)
	}
	n.mu.Unlock()
	for _, p := range peers {
		n.Kick(p)
	}
}

// ---------------------------------------------------------------------------
// Account operations (caller must hold Lock)
// ---------------------------------------------------------------------------

func (n *Node) AddLink(peer string, owner string) error {
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
	if _, ok := n.owners[owner]; !ok {
		return fmt.Errorf("unknown protocol %q", owner)
	}
	lk := NewLink(n.id, peer)
	lk.State.Status = Waiting
	lk.Owner = owner
	Enqueue(&lk.State.Queue, "connect")
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
	Enqueue(&link.State.Queue, "close")
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
	Enqueue(&link.State.Queue, "connect")
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
	Enqueue(&link.State.Queue, "close")
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
			"state":    l.State,
			"protocol": l.Owner,
		}
	}
	return Enc(map[string]any{
		"links":         links,
		"allow_invites": n.allowInvites,
	})
}
