// Package mimesis is a bilateral sync framework for federated p2p applications.
// It provides the sync primitive, identity format, server infrastructure,
// authentication and persistence for building p2p apps as modules.
package mimesis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Errors — protocol-level errors
// ---------------------------------------------------------------------------

var (
	errStaleCounter    = errors.New("stale counter")
	errCounterAhead    = errors.New("counter ahead")
	errCollision       = errors.New("collision")
	errRefsHeld        = errors.New("close with refs > 0")
	errNoLink          = errors.New("no link")
	errMaxLinks        = errors.New("max links")
	errInvitesDisabled = errors.New("invites disabled")
	errTimestamp       = errors.New("timestamp out of range")
	ErrBadMethod       = errors.New("unknown sync method")
	ErrNotFound        = errors.New("not found")
)

// maxTimestampFuture is the maximum number of seconds a peer's
// timestamp may be ahead of our local clock.  Generous to tolerate
// clock drift; the main defence against exploitation is the backward
// check (peer timestamp must not go below previously stored value).
const maxTimestampFuture int64 = 600 // 10 minutes

// ---------------------------------------------------------------------------
// Link — bilateral sync state
// ---------------------------------------------------------------------------

// ConnectionStatus — what the link IS.
type ConnectionStatus string

const (
	waiting   ConnectionStatus = "waiting"   // proposed, awaiting peer decision
	Incoming  ConnectionStatus = "incoming"  // peer proposed, awaiting local decision
	Connected ConnectionStatus = "connected" // bilaterally established
)

// ConnectionState is the bilateral connection state: what IS (Status)
// and what to DO next (IntentConnect/IntentClose).
type ConnectionState struct {
	Status        ConnectionStatus `json:"status,omitempty"`
	IntentConnect bool             `json:"intent_connect,omitempty"`
	IntentClose   bool             `json:"intent_close,omitempty"`
}


// LinkState is a ConnectionState (for the link and its app) plus per-module connections.
type LinkState struct {
	ConnectionState
	App     string                      `json:"app,omitempty"`
	Modules map[string]*ConnectionState `json:"modules,omitempty"`
}

// Module processes sync methods and account API calls for a namespace.
// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
// that need kicking on other links (cross-link side effects).
// HandleResponse returns kicks for the same purpose.
// HandleAccount handles user-facing API calls.
// NextTx returns the next queued message to send on a peer.
// Returns (method, payload, queuedAt). Empty method means nothing to send.
// queuedAt is the unix-second timestamp of when the message was first queued;
// when multiple modules return a candidate for the same peer, the oldest wins.
type Module interface {
	HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)
	NextTx(peer string) (string, json.RawMessage, int64)
	Status() json.RawMessage
	Busy(peer string) bool
}

// App is a Module that owns the lifecycle of a link. It is notified
// when the bilateral app.connect and app.close handshakes complete.
// A module passed to DeclareApp must implement App.
type App interface {
	Module
	OnConnect(peer string)
	OnClose(peer string)
}

// queuedMsg is a message queued for sending on a link.
// QueuedAt is a unix-second timestamp of when the message was queued.
// When multiple modules return a candidate for the same peer, the
// oldest QueuedAt wins. Zero means unset — unset always loses to any
// set value; when both are unset (or equal), registration order wins.
type queuedMsg struct {
	Method   string          `json:"method"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	QueuedAt int64           `json:"queued_at,omitempty"`
}

// peerSend tracks the outgoing send lifecycle for a link at a given counter.
// InFlight means a goroutine is blocked on send. After a failed send,
// a retry Timer is set that blocks kicks until it fires.
// Scoped to Counter — if counter advances via HandleIncoming, the entry
// is stale and ignored.
type peerSend struct {
	Counter  uint64
	InFlight bool
	Timer    *time.Timer
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *queuedMsg    `json:"pending,omitempty"`
	CachedResponse *txResponse `json:"cached_response,omitempty"`
	Timestamp      int64         `json:"ts"`
	Sending        *peerSend     `json:"-"`
}

// anyBusy returns true if any registered module reports being busy with
// the given peer. Used to defer link.close until all modules are idle.
func (n *Node) anyBusy(peer string) bool {
	for _, mod := range n.moduleMap {
		if mod.Busy(peer) {
			return true
		}
	}
	return false
}

// moduleBusy returns true if the named module reports being busy with
// the given peer. Used to defer module.close until the module is idle.
// If the module is an app, it also checks all its child modules.
func (n *Node) moduleBusy(name, peer string) bool {
	if mod := n.moduleMap[name]; mod != nil {
		if mod.Busy(peer) {
			return true
		}
	}
	if children, ok := n.apps[name]; ok {
		for _, child := range children {
			if mod := n.moduleMap[child]; mod != nil && mod.Busy(peer) {
				return true
			}
		}
	}
	return false
}

// Closing returns true if this link is closing (IntentClose is set).
func (l *Link) Closing() bool { return l.State.IntentClose }

// Close marks this link for closing. The sync loop defers sending
// link.close until no module reports Busy.
func (l *Link) Close() { l.State.IntentClose = true }

// isChildModule returns true if name is a declared child module of the
// given app. The app name itself is never a valid child module.
func isChildModule(name, app string, apps map[string][]string) bool {
	for _, child := range apps[app] {
		if child == name {
			return true
		}
	}
	return false
}

// moduleAvailable returns true if the named module is bilaterally connected
// and neither the module nor the link is closing. Use this to decide whether
// to start new work. Mimesis dispatch uses moduleConnected instead,
// so a closing module can still finish its in-flight messages.
func (l *Link) moduleAvailable(name string) bool {
	if l.State.IntentClose {
		return false
	}
	if name == l.State.App {
		return l.State.Status == Connected
	}
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected && !c.IntentClose
}

// moduleConnected returns true if the module is bilaterally connected,
// regardless of IntentClose. Used internally for dispatch and NextTx.
func (l *Link) moduleConnected(name string) bool {
	if name == l.State.App {
		return l.State.Status == Connected
	}
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected
}

// Dominant returns true when this side owns the current counter slot.
func (l *Link) dominant() bool { return l.Counter%2 == l.TurnBit }

// newLink creates a Link with the turn bit derived from identity ordering.
func newLink(self, peer string) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{TurnBit: bit}
}

// ---------------------------------------------------------------------------
// Node — collection of links for a node, including the sync loop
// ---------------------------------------------------------------------------

const retryDelay = 60 * time.Second

// nowSecs returns the current time as unix seconds.
func nowSecs() int64 { return time.Now().Unix() }

// Node manages the set of bilateral links for a node.
type Node struct {
	mu         sync.Mutex             `json:"-"`
	ID         string                 `json:"id"`
	Links      map[string]*Link       `json:"links"`
	InviteMode inviteMode             `json:"invite_mode,omitempty"`
	Handlers   map[string]json.RawMessage `json:"handlers,omitempty"`
	apps       map[string][]string    `json:"-"` // app name → supported module names
	moduleMap  map[string]Module      `json:"-"`
	// send delivers a sync message to a peer and returns the response body.
	send   func(peer string, msg txRequest) ([]byte, error) `json:"-"`
	// commit persists the current state. Called under the mutex.
	commit func()        `json:"-"`
	timers  []*time.Timer `json:"-"`
	stopped bool          `json:"-"`
}

// newNode creates a Node for the given identity.
func newNode(id string, links map[string]*Link, mode inviteMode, send func(string, txRequest) ([]byte, error), commit func()) *Node {
	if links == nil {
		links = map[string]*Link{}
	}
	if mode == "" {
		mode = inviteModeInvite
	}
	return &Node{ID: id, Links: links, InviteMode: mode, apps: map[string][]string{}, moduleMap: map[string]Module{}, send: send, commit: commit}
}

// registerModule adds a module to the node under the given name.
func (n *Node) registerModule(name string, m Module) {
	n.moduleMap[name] = m
}

// isApp returns true if the named module is a registered app.
func (n *Node) isApp(name string) bool {
	_, ok := n.apps[name]
	return ok
}

// appFor returns the app name that owns the given module, or "" if the
// module is not owned by any app (or is itself an app).
func (n *Node) appFor(module string) string {
	for app, mods := range n.apps {
		for _, m := range mods {
			if m == module {
				return app
			}
		}
	}
	return ""
}

// ModuleMap returns the registered modules for serialization.
func (n *Node) ModuleMap() map[string]Module { return n.moduleMap }

// Apps returns the app→children map so callers can pass it to ConnectModules.
func (n *Node) Apps() map[string][]string { return n.apps }

func (n *Node) Get(peer string) *Link { return n.Links[peer] }

// ModuleAvailable returns true if the named module on the peer's link is
// bilaterally connected and neither the module nor the link is closing.
func (n *Node) ModuleAvailable(peer, module string) bool {
	if lk := n.Links[peer]; lk != nil {
		return lk.moduleAvailable(module)
	}
	return false
}
func (n *Node) len() int              { return len(n.Links) }
func (n *Node) full() bool            { return len(n.Links) >= maxLinks }
func (n *Node) delete(peer string)    { delete(n.Links, peer) }

// Connect creates a new outgoing link to peer running the named app.
// The link is created with IntentConnect set, so the sync loop
// will initiate a connection. Returns errMaxLinks if at capacity.
func (n *Node) Connect(peer, app string) (*Link, error) {
	_, _, err := ParseIdentity(peer)
	if err != nil {
		return nil, err
	}
	if !n.isApp(app) {
		return nil, fmt.Errorf("unknown app %q", app)
	}
	peer = strings.ToLower(peer)
	if n.Links[peer] != nil {
		return nil, fmt.Errorf("link to %s already exists", peer)
	}
	if n.full() {
		return nil, errMaxLinks
	}
	lk := newLink(n.ID, peer)
	lk.State.App = app
	lk.State.IntentConnect = true
	n.Links[peer] = lk
	return lk, nil
}

// ---------------------------------------------------------------------------
// Sync lifecycle — request/response validation
// ---------------------------------------------------------------------------

// ValidateRequest checks whether an incoming request at the given counter
// is valid for this link. Returns:
//   - (*txResponse, nil) — stale counter but cached response exists
//   - (nil, error) — invalid (stale without cache, ahead, or collision)
//   - (nil, nil) — valid, proceed with dispatch
func (l *Link) ValidateRequest(counter uint64) (*txResponse, error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && l.CachedResponse.Counter == counter {
			resp := *l.CachedResponse
			return &resp, nil
		}
		return nil, errStaleCounter
	}
	if counter > l.Counter {
		return nil, errCounterAhead
	}
	if l.Pending != nil && l.dominant() {
		return nil, errCollision
	}
	return nil, nil
}

// ValidateTimestamp checks that ts is within acceptable bounds:
//   - not before the link's latest timestamp (monotonicity)
//   - not more than maxTimestampFuture seconds ahead of now
func (l *Link) ValidateTimestamp(ts, now int64) error {
	if ts < l.Timestamp {
		return errTimestamp
	}
	if ts > now+maxTimestampFuture {
		return errTimestamp
	}
	return nil
}

// AcceptRequest finalizes an incoming request. Call after the handler
// has processed the request. Returns the response to send back.
func (l *Link) AcceptRequest(reqCounter uint64, ok bool, ts int64) *txResponse {
	resp := txResponse{Counter: reqCounter, OK: ok, Timestamp: ts}
	l.CachedResponse = &resp
	l.Counter++
	l.Timestamp = ts
	l.Pending = nil
	return &resp
}

// AcceptResponse finalizes a received response to our outgoing message.
// Call after the response handler has processed the response.
func (l *Link) AcceptResponse(ts int64) {
	l.Counter++
	l.Timestamp = ts
	l.Pending = nil
}


// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

// txRequest is the wire envelope for a peer-to-peer transaction.
type txRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *txResponse   `json:"resp,omitempty"`
}

// txResponse is the wire envelope for a transaction response.
type txResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// inviteMode — node-level policy for incoming connections/modules
// ---------------------------------------------------------------------------

type inviteMode string

const (
	inviteModeClosed inviteMode = "closed" // reject all incoming
	inviteModeInvite inviteMode = "invite" // incoming, manual accept
	inviteModeOpen   inviteMode = "open"   // auto-accept all
)

const maxLinks = 64

func enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }

func splitMethod(method string) (ns, name string) {
	if i := strings.IndexByte(method, '.'); i >= 0 {
		return method[:i], method[i+1:]
	}
	return "", method
}

// olderQueuedMsg reports whether a should win over b.
// Zero QueuedAt means unset — unset loses to any set value.
// Equal values (including both zero) return false, preserving
// the earlier candidate.
func olderQueuedMsg(a, b *queuedMsg) bool {
	if b == nil {
		return true
	}
	if a.QueuedAt == 0 && b.QueuedAt == 0 {
		return false
	}
	if a.QueuedAt == 0 {
		return false
	}
	if b.QueuedAt == 0 {
		return true
	}
	return a.QueuedAt < b.QueuedAt
}

// ---------------------------------------------------------------------------
// Link-level request/response handling
// ---------------------------------------------------------------------------

// dispatchRequest dispatches an incoming sync request by namespace.
// Link-level methods are handled inline; registered modules are
// dispatched by name. Returns (ok, kicks, error).
func (n *Node) dispatchRequest(peer, fullMethod string, payload json.RawMessage, ts int64) (bool, []string, error) {
	ns, method := splitMethod(fullMethod)
	if ns == "app" {
		l := n.Get(peer)
		if l == nil {
			return false, nil, fmt.Errorf("no link to %s", peer)
		}
		switch method {
		case "connect":
			var p struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(payload, &p); err != nil || p.Name == "" {
				return false, nil, fmt.Errorf("app.connect requires name")
			}
			if !n.isApp(p.Name) {
				return false, nil, fmt.Errorf("unknown app %q", p.Name)
			}
			if l.State.IntentConnect || l.State.Status == waiting {
				// Both sides must agree on the app.
				if l.State.App != "" && l.State.App != p.Name {
					return false, nil, nil
				}
				if l.State.App == "" {
					l.State.App = p.Name
				}
				l.State.Status = Connected
				l.State.IntentConnect = false
				if app, ok := n.moduleMap[l.State.App].(App); ok {
					app.OnConnect(peer)
				}
				return true, nil, nil
			}
			if n.InviteMode == inviteModeOpen {
				l.State.App = p.Name
				l.State.Status = Connected
				if app, ok := n.moduleMap[p.Name].(App); ok {
					app.OnConnect(peer)
				}
				return true, nil, nil
			}
			return false, nil, nil
		case "close":
			if n.anyBusy(peer) {
				return false, nil, errRefsHeld
			}
			if app, ok := n.moduleMap[l.State.App].(App); ok {
				app.OnClose(peer)
			}
			n.delete(peer)
			return true, nil, nil
		}
		return false, nil, fmt.Errorf("unknown link method %q", method)
	}
	if ns == "module" {
		l := n.Get(peer)
		if l == nil {
			return false, nil, fmt.Errorf("no link to %s", peer)
		}
		switch method {
		case "connect":
			ok, err := l.reqModule(payload, n.moduleMap, n.InviteMode, n.apps)
			return ok, nil, err
		case "close":
			ok, err := l.reqModuleClose(peer, payload, n.moduleMap, n.apps)
			return ok, nil, err
		}
		return false, nil, fmt.Errorf("unknown module method %q", method)
	}
	lk := n.Get(peer)
	if lk == nil {
		return false, nil, fmt.Errorf("no link to %s", peer)
	}
	mod := n.moduleMap[ns]
	if mod == nil || !lk.moduleConnected(ns) {
		return false, nil, fmt.Errorf("unknown method %q", fullMethod)
	}
	return mod.HandleRequest(peer, method, payload, ts)
}

// dispatchResponse dispatches a sync response by namespace.
func (n *Node) dispatchResponse(peer, fullMethod string, payload json.RawMessage, ok bool, ts int64) []string {
	ns, method := splitMethod(fullMethod)
	if ns == "app" {
		l := n.Get(peer)
		if l == nil {
			return nil
		}
		switch method {
		case "connect":
			if ok {
				l.State.Status = Connected
				if app, ok := n.moduleMap[l.State.App].(App); ok {
					app.OnConnect(peer)
				}
			} else {
				l.State.Status = waiting
			}
			l.State.IntentConnect = false
		case "close":
			if ok {
				if app, ok := n.moduleMap[l.State.App].(App); ok {
					app.OnClose(peer)
				}
				n.delete(peer)
			} else {
				l.State.IntentClose = false
			}
		}
		return nil
	}
	if ns == "module" {
		l := n.Get(peer)
		if l == nil {
			return nil
		}
		switch method {
		case "connect":
			l.respModule(payload, ok)
		case "close":
			l.respModuleClose(payload, ok)
		}
		return nil
	}
	mod := n.moduleMap[ns]
	if mod == nil {
		return nil
	}
	return mod.HandleResponse(peer, method, payload, ok, ts)
}

// nextTx returns the next message to send on a peer link.
// Link and module-level messages have absolute priority.
// Among registered modules, the oldest QueuedAt wins.
func (n *Node) nextTx(peer string) *queuedMsg {
	// Link-level messages have absolute priority.
	lk := n.Get(peer)
	if lk == nil {
		return nil
	}
	// Cancel: both intents set before any message sent — they annihilate.
	if lk.State.IntentClose && lk.State.IntentConnect {
		lk.State.IntentClose = false
		lk.State.IntentConnect = false
	}
	if lk.State.IntentClose && !n.anyBusy(peer) {
		return &queuedMsg{Method: "app.close"}
	}
	if lk.State.IntentConnect {
		return &queuedMsg{Method: "app.connect", Payload: enc(map[string]string{"name": lk.State.App})}
	}
	if lk.State.Status == Connected {
		// Cancel annihilating intents.
		for _, mod := range lk.State.Modules {
			if mod.IntentClose && mod.IntentConnect {
				mod.IntentClose = false
				mod.IntentConnect = false
			}
		}
		// Module close/connect — app lifecycle is at link level.
		// Close modules first, then connect.
		for name, mod := range lk.State.Modules {
			if mod.IntentClose && !n.moduleBusy(name, peer) {
				return &queuedMsg{
					Method:  "module.close",
					Payload: enc(map[string]string{"name": name}),
				}
			}
		}
		for name, mod := range lk.State.Modules {
			if mod.IntentConnect {
				return &queuedMsg{
					Method:  "module.connect",
					Payload: enc(map[string]string{"name": name}),
				}
			}
		}
	}
	if lk.State.Status != Connected {
		return nil
	}
	var best *queuedMsg
	for name, mod := range n.moduleMap {
		if !lk.moduleConnected(name) {
			continue
		}
		method, payload, queuedAt := mod.NextTx(peer)
		if method != "" {
			msg := &queuedMsg{
				Method:   name + "." + method,
				Payload:  payload,
				QueuedAt: queuedAt,
			}
			if olderQueuedMsg(msg, best) {
				best = msg
			}
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Module negotiation (internal)
// ---------------------------------------------------------------------------

func (l *Link) reqModule(payload json.RawMessage, modules map[string]Module, inviteMode inviteMode, apps map[string][]string) (bool, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, fmt.Errorf("bad payload")
	}
	// Only allow modules declared as children of this link's app.
	if !isChildModule(p.Name, l.State.App, apps) {
		return false, nil
	}
	if _, ok := modules[p.Name]; !ok {
		return false, nil // we don't speak this module
	}
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	mod := l.State.Modules[p.Name]
	// Already connected — idempotent ACK.
	if mod != nil && mod.Status == Connected {
		return true, nil
	}
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == waiting) {
		mod.Status = Connected
		mod.IntentConnect = false
		return true, nil
	}
	// Open mode — auto-accept supported modules.
	if inviteMode == inviteModeOpen {
		if mod == nil {
			mod = &ConnectionState{}
			l.State.Modules[p.Name] = mod
		}
		mod.Status = Connected
		return true, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	if mod == nil {
		mod = &ConnectionState{}
		l.State.Modules[p.Name] = mod
	}
	mod.Status = Incoming
	return false, nil // NACK — not accepted yet
}

func (l *Link) respModule(payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return
	}
	if ok {
		mod.Status = Connected
	} else {
		mod.Status = waiting
	}
	mod.IntentConnect = false
}

func (l *Link) reqModuleClose(peer string, payload json.RawMessage, modules map[string]Module, apps map[string][]string) (bool, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, fmt.Errorf("bad payload")
	}
	if !isChildModule(p.Name, l.State.App, apps) {
		return false, nil
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return true, nil // already gone
	}
	if m := modules[p.Name]; m != nil && m.Busy(peer) {
		return false, errRefsHeld
	}
	delete(l.State.Modules, p.Name)
	return true, nil
}

func (l *Link) respModuleClose(payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return
	}
	if ok {
		delete(l.State.Modules, p.Name)
	} else {
		mod.IntentClose = false
	}
}

// ---------------------------------------------------------------------------
// Account operations
// ---------------------------------------------------------------------------

// HandleAccount dispatches an account API call by namespace.
// Handles locking, commit, and kicks internally — like HandleIncoming.
func (n *Node) handleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	n.mu.Lock()
	ns, method := splitMethod(fullMethod)

	var result json.RawMessage
	var kicks []string
	var err error

	switch {
	case ns == "app" && method == "set_invite_mode":
		var p struct {
			Mode inviteMode `json:"mode"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case inviteModeClosed, inviteModeInvite, inviteModeOpen:
		default:
			n.mu.Unlock()
			return nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		n.InviteMode = p.Mode
		result = enc("ok")

	case ns == "app" && method == "set_modules":
		var p struct {
			Peer    string   `json:"peer"`
			Modules []string `json:"modules"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		lk.setModules(p.Modules, n.moduleMap, n.apps)
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "app" && method == "accept":
		var p struct {
			Peer string `json:"peer"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if lk.State.Status != Incoming {
			n.mu.Unlock()
			return nil, fmt.Errorf("link to %s is not incoming", p.Peer)
		}
		lk.State.IntentConnect = true
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "app" && method == "reject":
		var p struct {
			Peer string `json:"peer"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if lk.State.Status != Incoming {
			n.mu.Unlock()
			return nil, fmt.Errorf("link to %s is not incoming", p.Peer)
		}
		lk.State.IntentClose = true
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "module" && method == "accept":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.AcceptModule(p.Module); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "module" && method == "reject":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.RejectModule(p.Module); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		result = enc("ok")

	case ns == "" && method == "status":
		result = n.status()

	case ns == "app":
		n.mu.Unlock()
		return nil, fmt.Errorf("unknown link method %q", method)

	case ns == "module":
		n.mu.Unlock()
		return nil, fmt.Errorf("unknown module method %q", method)

	default:
		mod := n.moduleMap[ns]
		if mod == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("unknown method %q", fullMethod)
		}
		var modKicks []string
		result, modKicks, err = mod.HandleAccount(method, payload)
		kicks = append(kicks, modKicks...)
	}

	if err == nil {
		n.commit()
	}
	n.mu.Unlock()
	for _, k := range kicks {
		n.kick(k)
	}
	return result, err
}

// Status returns the link collection status as JSON, including module status.
func (n *Node) status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.Links {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	result := map[string]any{
		"links":       links,
		"invite_mode": n.InviteMode,
	}
	for name, mod := range n.moduleMap {
		if extra := mod.Status(); extra != nil {
			var m map[string]any
			if json.Unmarshal(extra, &m) == nil {
				result[name] = m
			}
		}
	}
	return enc(result)
}

// ConnectModules requests connection for the named modules on this link,
// filtering by registered modules. Existing modules are left untouched.
func (l *Link) ConnectModules(modules []string, registered map[string]Module, apps map[string][]string) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	for _, name := range modules {
		if !isChildModule(name, l.State.App, apps) {
			continue
		}
		if _, ok := registered[name]; !ok {
			continue
		}
		mod := l.State.Modules[name]
		if mod == nil {
			l.State.Modules[name] = &ConnectionState{IntentConnect: true}
		} else if mod.Status != Connected {
			mod.IntentConnect = true
		}
	}
}

// setModules proposes the given modules on this link, filtering by supported.
// Modules not in the desired set are marked IntentClose. The sync loop
// defers sending module.close until the module reports !Busy.
func (l *Link) setModules(modules []string, registered map[string]Module, apps map[string][]string) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	desired := map[string]bool{}
	for _, name := range modules {
		if !isChildModule(name, l.State.App, apps) {
			continue
		}
		if _, ok := registered[name]; ok {
			desired[name] = true
		}
	}
	for name := range desired {
		mod := l.State.Modules[name]
		if mod == nil {
			l.State.Modules[name] = &ConnectionState{IntentConnect: true}
		} else if mod.Status != Connected {
			mod.IntentConnect = true
		} else if mod.IntentClose {
			// Already connected but closing — set IntentConnect so
			// nextTx can cancel both intents.
			mod.IntentConnect = true
		}
	}
	for name, mod := range l.State.Modules {
		if !desired[name] {
			mod.IntentClose = true
		}
	}
}

// AcceptModule accepts an incoming module proposal.
func (l *Link) AcceptModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	mod.IntentConnect = true
	return nil
}

// RejectModule rejects an incoming module proposal.
// Sets IntentClose so the sync loop sends module_close to the peer.
func (l *Link) RejectModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	mod.IntentClose = true
	return nil
}

// ---------------------------------------------------------------------------
// Sync loop — incoming
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect/close).
func (n *Node) quickReject(from, method string) bool {
	from = strings.ToLower(from)
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Get(from) == nil && method != "app.connect" && method != "app.close"
}

// HandleIncoming processes an incoming transaction. The caller is responsible
// for authentication (signature verification).
func (n *Node) handleIncoming(env txRequest) (*txResponse, error) {
	env.From = strings.ToLower(env.From)
	n.mu.Lock()

	lk := n.Get(env.From)
	if lk == nil {
		if env.Method == "app.close" {
			n.mu.Unlock()
			return &txResponse{Counter: env.Counter, OK: true, Timestamp: nowSecs()}, nil
		}
		if env.Method != "app.connect" {
			n.mu.Unlock()
			return nil, errNoLink
		}
		if n.InviteMode == inviteModeClosed {
			n.mu.Unlock()
			return nil, errInvitesDisabled
		}
		if n.full() {
			n.mu.Unlock()
			return nil, errMaxLinks
		}
		lk = newLink(n.ID, env.From)
		n.Links[env.From] = lk
		// Extract and validate app name from payload.
		var p struct {
			Name string `json:"name"`
		}
		if env.Payload != nil {
			json.Unmarshal(env.Payload, &p)
		}
		if p.Name == "" || !n.isApp(p.Name) {
			n.mu.Unlock()
			return nil, fmt.Errorf("unknown app %q", p.Name)
		}
		lk.State.App = p.Name
		lk.State.Status = Incoming
	}

	now := nowSecs()

	// Piggybacked response.
	if env.Resp != nil && lk.Pending != nil && env.Resp.Counter == lk.Counter {
		if err := lk.ValidateTimestamp(env.Resp.Timestamp, now); err != nil {
			n.mu.Unlock()
			return nil, err
		}
		pbKicks := n.dispatchResponse(env.From, lk.Pending.Method, lk.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		lk.AcceptResponse(env.Resp.Timestamp)
		n.commit()
		for _, k := range pbKicks {
			n.kick(k)
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
	if lk.dominant() {
		ts = now
		if ts < lk.Timestamp {
			ts = lk.Timestamp
		}
	}

	// Validate peer timestamp (monotonicity + forward cap).
	if err := lk.ValidateTimestamp(ts, now); err != nil {
		n.mu.Unlock()
		return nil, err
	}

	// Dispatch to handler.
	ok, kicks, reqErr := n.dispatchRequest(env.From, env.Method, env.Payload, ts)
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}

	resp := lk.AcceptRequest(env.Counter, ok, ts)
	n.commit()
	n.mu.Unlock()
	n.kick(env.From)
	for _, k := range kicks {
		n.kick(k)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Sync loop — outgoing
// ---------------------------------------------------------------------------

// Kick triggers a sendPending for the given peer.
func (n *Node) kick(peer string) { go n.sendPending(peer) }

// KickAll kicks all peers. Used at boot to resume pending work.
func (n *Node) kickAll() {
	n.mu.Lock()
	peers := make([]string, 0, n.len())
	for peer := range n.Links {
		peers = append(peers, peer)
	}
	n.mu.Unlock()
	for _, p := range peers {
		n.kick(p)
	}
}

// Schedule runs fn after delay under the Node mutex.
// fn returns a list of peers to kick. Node commits and kicks.
// The timer is tracked so Stop() can cancel it.
//
// Schedule is called from two contexts: during init (no mutex held)
// and from within a callback (mutex held). The timer append is safe
// in both cases — init has no concurrency, callbacks hold the mutex.
func (n *Node) schedule(delay time.Duration, fn func() []string) {
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		kicks := fn()
		n.commit()
		n.mu.Unlock()
		for _, k := range kicks {
			n.kick(k)
		}
	})
	// No mutex lock here — called under mutex (callback) or during
	// init (no concurrency). Locking would deadlock in the callback case.
	n.timers = append(n.timers, t)
}

// Stop cancels all scheduled and retry timers and marks the Node as
// stopped. After Stop, Schedule callbacks and sendPending are no-ops.
// Must be called when deleting a node to prevent zombie timers from
// re-persisting the node's state.
func (n *Node) stop() {
	n.mu.Lock()
	n.stopped = true
	for _, t := range n.timers {
		t.Stop()
	}
	n.timers = nil
	for _, lk := range n.Links {
		if lk.Sending != nil && lk.Sending.Timer != nil {
			lk.Sending.Timer.Stop()
			lk.Sending = nil
		}
	}
	n.mu.Unlock()
}

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	lk := n.Get(peer)
	if lk == nil {
		n.mu.Unlock()
		return
	}
	if s := lk.Sending; s != nil {
		if s.Counter == lk.Counter && (s.InFlight || s.Timer != nil) {
			n.mu.Unlock()
			return
		}
		if s.Timer != nil {
			s.Timer.Stop()
		}
		lk.Sending = nil
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
	lk.Sending = &peerSend{Counter: counter, InFlight: true}
	msg := lk.Pending
	env := txRequest{
		From: n.ID, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
	}
	if lk.dominant() {
		env.Timestamp = nowSecs()
		if env.Timestamp < lk.Timestamp {
			env.Timestamp = lk.Timestamp
		}
	}
	if cr := lk.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		n.commit()
	}
	n.mu.Unlock()

	body, err := n.send(peer, env)

	n.mu.Lock()
	if err != nil {
		log.Printf("[link] %s->%s %s: %v", n.ID, peer, msg.Method, err)
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}
	var resp txResponse
	if json.Unmarshal(body, &resp) != nil {
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}
	lk = n.Get(peer)
	if lk == nil || lk.Pending != msg || lk.Counter != counter || resp.Counter != counter {
		if lk != nil {
			lk.Sending = nil
		}
		n.mu.Unlock()
		return
	}

	if err := lk.ValidateTimestamp(resp.Timestamp, nowSecs()); err != nil {
		log.Printf("[link] %s->%s: bad response timestamp: %v", n.ID, peer, err)
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}

	extraKicks := n.dispatchResponse(peer, msg.Method, msg.Payload, resp.OK, resp.Timestamp)
	lk.AcceptResponse(resp.Timestamp)

	var kickPeer bool
	if n.Get(peer) != nil {
		lk.Pending = n.nextTx(peer)
		kickPeer = lk.Pending != nil
	}
	n.commit()
	lk.Sending = nil
	n.mu.Unlock()
	if kickPeer {
		n.kick(peer)
	}
	for _, k := range extraKicks {
		n.kick(k)
	}
}

// setRetry sets a per-peer retry timer. Caller must hold Lock.
func (n *Node) setRetry(peer string, counter uint64) {
	if n.stopped {
		return
	}
	lk := n.Get(peer)
	if lk == nil || lk.Counter != counter {
		return
	}
	if lk.Sending != nil && lk.Sending.Timer != nil {
		lk.Sending.Timer.Stop()
	}
	ps := &peerSend{
		Counter:    counter,
	}
	ps.Timer = time.AfterFunc(retryDelay, func() {
		n.mu.Lock()
		if lk := n.Get(peer); lk != nil && lk.Sending == ps {
			lk.Sending = nil
		}
		n.mu.Unlock()
		n.sendPending(peer)
	})
	lk.Sending = ps
}
