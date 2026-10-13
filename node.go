package creditnet

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"creditnet/link"
)

const retryDelay = 60 * time.Second

// ---------------------------------------------------------------------------
// Node — sync engine and dispatcher
// ---------------------------------------------------------------------------

type Node struct {
	mu           sync.Mutex
	id         string
	links      *link.Links
	sends      map[string]*peerSend
	inviteMode link.InviteMode

	trustlines map[string]*Trustline
	reservers  []Reserver

	modules   []moduleEntry
	moduleMap map[string]Module
	supported map[string]struct{}

	transport Transport
	store     Store
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

type moduleEntry struct {
	name   string
	module Module
}

type nodeState struct {
	ID         string                     `json:"id"`
	Links      map[string]*link.Link      `json:"links"`
	InviteMode link.InviteMode             `json:"invite_mode,omitempty"`
	Handlers   map[string]json.RawMessage `json:"handlers,omitempty"`
}

func newNode(id string, raw json.RawMessage, transport Transport, store Store) *Node {
	var st nodeState
	if raw != nil {
		json.Unmarshal(raw, &st)
	}
	mode := st.InviteMode
	if mode == "" {
		mode = link.InviteModeInvite
	}
	n := &Node{
		id:         id,
		links:      link.NewLinks(id, st.Links),
		sends:      map[string]*peerSend{},
		inviteMode: mode,
		trustlines: loadTrustlines(st.Handlers["trustline"]),
		moduleMap:  map[string]Module{},
		supported:  map[string]struct{}{"trustline": {}},
		transport:  transport,
		store:      store,
	}

	return n
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

func (n *Node) RegisterModule(m Module) {
	name := m.Name()
	n.modules = append(n.modules, moduleEntry{name: name, module: m})
	n.moduleMap[name] = m
	n.supported[name] = struct{}{}
}

// Links returns the node's links. Caller must hold Lock.
func (n *Node) Links() *link.Links {
	return n.links
}

// DeleteLink removes a link. Caller must hold Lock.
func (n *Node) DeleteLink(peer string) {
	n.trustlineLinkDeleted(peer)
	for _, entry := range n.modules {
		entry.module.LinkDeleted(peer)
	}
	if s := n.sends[peer]; s != nil {
		if s.timer != nil {
			s.timer.Stop()
		}
		delete(n.sends, peer)
	}
	n.links.Delete(peer)
}

// ---------------------------------------------------------------------------
// Locking & persistence
// ---------------------------------------------------------------------------

func (n *Node) Lock()                 { n.mu.Lock() }
func (n *Node) Unlock()               { n.mu.Unlock() }
func (n *Node) ID() string            { return n.id }

// HandleAccount dispatches a user-facing API call by namespace.
// Caller must hold Lock.
func (n *Node) HandleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	ns, method := SplitMethod(fullMethod)
	switch ns {
	case "link":
		return n.handleLinkAccount(method, payload)
	case "trustline":
		return n.handleTrustlineAccount(method, payload)
	}
	mod := n.moduleMap[ns]
	if mod == nil {
		return nil, ErrBadMethod
	}
	return mod.HandleAccount(method, payload)
}

// Commit persists the node state. Caller must hold Lock.
func (n *Node) Commit() {
	st := nodeState{ID: n.id, Links: n.links.Map(), InviteMode: n.inviteMode}
	st.Handlers = make(map[string]json.RawMessage)
	// Trustline state.
	if data, err := json.Marshal(trustlineState{Trustlines: n.trustlines}); err == nil {
		st.Handlers["trustline"] = data
	}
	// External modules.
	for _, entry := range n.modules {
		if data, err := json.Marshal(entry.module); err == nil {
			st.Handlers[entry.name] = data
		}
	}
	if err := n.store.SaveNode(n.id, st); err != nil {
		log.Fatalf("node %s: persist failed: %v", n.id, err)
	}
}

func (n *Node) Kick(peer string) { go n.sendPending(peer) }

// Schedule runs fn after delay under the node's lock.
// fn returns a list of peers to kick. The node commits and kicks.
func (n *Node) Schedule(delay time.Duration, fn func() []string) {
	time.AfterFunc(delay, func() {
		n.mu.Lock()
		kicks := fn()
		n.Commit()
		n.mu.Unlock()
		for _, k := range kicks {
			n.Kick(k)
		}
	})
}

// ScheduleFunc returns a ScheduleFunc bound to this node.
func (n *Node) ScheduleFunc() ScheduleFunc {
	return n.Schedule
}

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
	return n.links.Get(from) == nil && method != "link.connect" && method != "link.close"
}

// HandleSync processes an incoming sync request. The caller is responsible
// for authentication (signature verification).
func (n *Node) HandleSync(env link.SyncRequest) (*link.SyncResponse, error) {
	n.mu.Lock()

	lk := n.links.Get(env.From)
	if lk == nil {
		if env.Method == "link.close" {
			n.mu.Unlock()
			return &link.SyncResponse{Counter: env.Counter, OK: true, Timestamp: NowSecs()}, nil
		}
		if env.Method != "link.connect" {
			n.mu.Unlock()
			return nil, ErrNoLink
		}
		if n.inviteMode == link.InviteModeClosed {
			n.mu.Unlock()
			return nil, ErrInvitesDisabled
		}
		if n.links.Full() {
			n.mu.Unlock()
			return nil, ErrMaxLinks
		}
		lk = n.links.Create(env.From)
		lk.State.Status = link.Incoming
	}

	// Piggybacked response.
	if env.Resp != nil && lk.Pending != nil && env.Resp.Counter == lk.Counter {
		pbKicks := n.dispatchResponse(env.From, lk.Pending, env.Resp)
		lk.AcceptResponse(env.Resp.Timestamp)
		n.Commit()
		for _, k := range pbKicks {
			n.Kick(k)
		}
	}

	// Validate counter and collision.
	if cached, err := lk.ValidateRequest(env.Counter); err != nil {
		n.mu.Unlock()
		return nil, err
	} else if cached != nil {
		n.mu.Unlock()
		return cached, nil
	}

	ts := env.Timestamp
	if lk.Dominant() {
		ts = NowSecs()
	}

	// Dispatch by namespace.
	ns, method := SplitMethod(env.Method)
	var ok bool
	var kicks []string
	var reqErr error
	switch ns {
	case "link":
		var del bool
		ok, del, reqErr = lk.HandleRequest(method, env.Payload, n.supported, n.inviteMode)
		if del {
			n.DeleteLink(env.From)
		}
	case "trustline":
		if !lk.ModuleEnabled(ns) {
			n.mu.Unlock()
			return nil, ErrBadMethod
		}
		ok, kicks, reqErr = n.handleTrustlineRequest(env.From, method, env.Payload, ts)
	default:
		mod := n.moduleMap[ns]
		if mod == nil || !lk.ModuleEnabled(ns) {
			n.mu.Unlock()
			return nil, ErrBadMethod
		}
		ok, kicks, reqErr = mod.HandleRequest(env.From, n.GetTrustline(env.From), method, env.Payload, ts)
	}
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}

	resp := lk.AcceptRequest(env.Counter, ok, ts)
	n.Commit()
	n.mu.Unlock()
	n.Kick(env.From)
	for _, k := range kicks {
		n.Kick(k)
	}
	return resp, nil
}

// dispatchResponse dispatches a piggybacked response to the appropriate handler.
// Caller must hold Lock.
func (n *Node) dispatchResponse(peer string, msg *link.QueuedMsg, resp *link.SyncResponse) []string {
	ns, method := SplitMethod(msg.Method)
	switch ns {
	case "link":
		lk := n.links.Get(peer)
		if lk != nil && lk.HandleResponse(method, msg.Payload, resp.OK) {
			n.DeleteLink(peer)
		}
		return nil
	case "trustline":
		return n.handleTrustlineResponse(peer, method, msg.Payload, resp.OK, resp.Timestamp)
	}
	if rh := n.moduleMap[ns]; rh != nil {
		return rh.HandleResponse(peer, n.GetTrustline(peer), method, msg.Payload, resp.OK, resp.Timestamp)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Outgoing sync
// ---------------------------------------------------------------------------

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	lk := n.links.Get(peer)
	if lk == nil {
		n.mu.Unlock()
		return
	}
	if s := n.sends[peer]; s != nil && s.counter == lk.Counter {
		if s.inFlight || s.refractory {
			n.mu.Unlock()
			return
		}
		s.timer.Stop()
		delete(n.sends, peer)
	}
	promoted := false
	if lk.Pending == nil {
		lk.Pending = n.nextTx(peer)
		if lk.Pending == nil {
			n.mu.Unlock()
			return
		}
		promoted = true
	}
	counter := lk.Counter
	n.sends[peer] = &peerSend{counter: counter, inFlight: true}
	msg := lk.Pending
	env := link.SyncRequest{
		From: n.id, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
		Timestamp: NowSecs(),
	}
	if cr := lk.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		n.Commit()
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
	var resp link.SyncResponse
	if json.Unmarshal(body, &resp) != nil {
		n.setRetry(peer, counter, true)
		n.mu.Unlock()
		return
	}
	lk = n.links.Get(peer)
	if lk == nil || lk.Pending != msg || lk.Counter != counter || resp.Counter != counter {
		delete(n.sends, peer)
		n.mu.Unlock()
		return
	}

	extraKicks := n.dispatchResponse(peer, msg, &resp)
	lk.AcceptResponse(resp.Timestamp)

	var kickPeer bool
	if n.links.Get(peer) != nil {
		lk.Pending = n.nextTx(peer)
		kickPeer = lk.Pending != nil
	}
	n.Commit()
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
func (n *Node) nextTx(peer string) *link.QueuedMsg {
	lk := n.links.Get(peer)
	if lk == nil {
		return nil
	}
	// Link handler runs regardless of status.
	if msg := lk.NextTx(); msg != nil {
		msg.Method = "link." + msg.Method
		return msg
	}
	// Trustline runs on links with trustline module enabled.
	if lk.ModuleEnabled("trustline") {
		if msg := n.trustlineNextTx(peer); msg != nil {
			msg.Method = "trustline." + msg.Method
			return msg
		}
	}
	// External modules only run on connected links with the module enabled.
	if lk.State.Status != link.Connected {
		return nil
	}
	for _, entry := range n.modules {
		if !lk.ModuleEnabled(entry.name) {
			continue
		}
		if msg := entry.module.NextTx(peer); msg != nil {
			msg.Method = entry.name + "." + msg.Method
			return msg
		}
	}
	return nil
}

// KickAll kicks all peers. Used at boot to resume pending work.
func (n *Node) KickAll() {
	n.mu.Lock()
	peers := make([]string, 0, n.links.Len())
	for peer := range n.links.Map() {
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

func (n *Node) AddLink(peer string) error {
	if _, _, err := ParseIdentity(peer); err != nil {
		return err
	}
	peer = strings.ToLower(peer)
	if peer == n.id {
		return fmt.Errorf("cannot link to self")
	}
	if n.links.Get(peer) != nil {
		return fmt.Errorf("link to %s already exists", peer)
	}
	if n.links.Full() {
		return fmt.Errorf("max links reached")
	}
	lk := n.links.Create(peer)
	lk.State.IntentConnect = true
	n.Kick(peer)
	return nil
}

func (n *Node) RemoveLink(peer string) error {
	peer = strings.ToLower(peer)
	lk := n.links.Get(peer)
	if lk == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	lk.State.IntentClose = true
	n.Kick(peer)
	return nil
}

func (n *Node) AcceptLink(peer string) error {
	peer = strings.ToLower(peer)
	lk := n.links.Get(peer)
	if lk == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if lk.State.Status != link.Incoming {
		return fmt.Errorf("link to %s is not incoming", peer)
	}
	lk.State.IntentConnect = true
	n.Kick(peer)
	return nil
}

func (n *Node) RejectLink(peer string) error {
	peer = strings.ToLower(peer)
	lk := n.links.Get(peer)
	if lk == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if lk.State.Status != link.Incoming {
		return fmt.Errorf("link to %s is not incoming", peer)
	}
	lk.State.IntentClose = true
	n.Kick(peer)
	return nil
}

func (n *Node) SetInviteMode(mode link.InviteMode) {
	n.inviteMode = mode
}

func (n *Node) Status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.links.Map() {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	return Enc(map[string]any{
		"links":       links,
		"invite_mode": n.inviteMode,
	})
}
