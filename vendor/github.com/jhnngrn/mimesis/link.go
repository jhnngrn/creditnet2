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


// LinkState is a ConnectionState (for the link and its root module) plus per-module connections.
type LinkState struct {
	ConnectionState
	Root    string                      `json:"root,omitempty"`
	Modules map[string]*ConnectionState `json:"modules,omitempty"`
}

// Module processes sync methods and account API calls for a namespace.
// HandleRequest returns (ok, routes, err) where routes is a list of peers
// that need kicking on other links (cross-link side effects).
// HandleResponse returns routes for the same purpose.
// HandleAccount handles user-facing API calls.
// NextTx returns the next queued message to send on a peer.
// Returns (method, payload, queuedAt). Empty method means nothing to send.
// queuedAt is the unix-second timestamp of when the message was first queued;
// when multiple modules return a candidate for the same peer, the oldest wins.
type Module interface {
	OnConnect(peer string)
	OnClose(peer string)
	HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)
	NextTx(peer string) (string, json.RawMessage, int64)
	Status() json.RawMessage
	Busy(peer string) bool
	ForceRemove(peer string)
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
// If the module is a root, it also checks all its child modules.
func (n *Node) moduleBusy(name, peer string) bool {
	if mod := n.moduleMap[name]; mod != nil {
		if mod.Busy(peer) {
			return true
		}
	}
	if children, ok := n.roots[name]; ok {
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
// given root. The root name itself is never a valid child module.
func isChildModule(name, root string, roots map[string][]string) bool {
	for _, child := range roots[root] {
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
	if name == l.State.Root {
		return l.State.Status == Connected
	}
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected && !c.IntentClose
}

// moduleConnected returns true if the module is bilaterally connected,
// regardless of IntentClose. Used internally for dispatch and NextTx.
func (l *Link) moduleConnected(name string) bool {
	if name == l.State.Root {
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
	roots      map[string][]string    `json:"-"` // root module name → child module names
	moduleMap  map[string]Module      `json:"-"`
	// send delivers a sync message to a peer and returns the response body.
	send   func(peer string, msg txRequest) ([]byte, error) `json:"-"`
	// commit persists the current state. Called under the mutex.
	commit func()        `json:"-"`
	timers  map[*time.Timer]struct{} `json:"-"`
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
	return &Node{ID: id, Links: links, InviteMode: mode, roots: map[string][]string{}, moduleMap: map[string]Module{}, send: send, commit: commit, timers: map[*time.Timer]struct{}{}}
}

// registerModule adds a module to the node under the given name.
func (n *Node) registerModule(name string, m Module) {
	n.moduleMap[name] = m
}

// isRoot returns true if the named module is a declared root module.
func (n *Node) isRoot(name string) bool {
	_, ok := n.roots[name]
	return ok
}

// rootFor returns the root module that owns the given child module, or ""
// if the module is not a child of any root (or is itself a root).
func (n *Node) rootFor(module string) string {
	for root, mods := range n.roots {
		for _, m := range mods {
			if m == module {
				return root
			}
		}
	}
	return ""
}

// ModuleMap returns the registered modules for serialization.
func (n *Node) ModuleMap() map[string]Module { return n.moduleMap }

// ConnectModules requests connection for the named child modules on the
// peer's link, filtering by registered modules and root declarations.
// Existing modules are left untouched.
func (n *Node) ConnectModules(peer string, modules []string) {
	lk := n.Links[peer]
	if lk == nil {
		return
	}
	lk.connectModules(modules, n.moduleMap, n.roots)
}

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

// ForceRemove unilaterally removes a link without bilateral consensus.
// Unlike Close, which goes through the normal req/resp close handshake,
// ForceRemove deletes the link immediately. Each module's ForceRemove
// is called to clean up state that would normally be garbage collected
// through the bilateral close cycle.
// Must be called under the node lock (from a handler or schedule callback).
func (n *Node) ForceRemove(peer string) {
	lk := n.Get(peer)
	if lk == nil {
		return
	}
	for name := range lk.State.Modules {
		if mod := n.moduleMap[name]; mod != nil {
			mod.ForceRemove(peer)
		}
	}
	if mod := n.moduleMap[lk.State.Root]; mod != nil {
		mod.ForceRemove(peer)
	}
	if lk.Sending != nil && lk.Sending.Timer != nil {
		lk.Sending.Timer.Stop()
	}
	n.delete(peer)
}

// Connect creates a new outgoing link to peer running the named root module.
// The link is created with IntentConnect set, so the sync loop
// will initiate a connection. Returns errMaxLinks if at capacity.
func (n *Node) Connect(peer, root string) (*Link, error) {
	_, _, err := ParseIdentity(peer)
	if err != nil {
		return nil, err
	}
	if !n.isRoot(root) {
		return nil, fmt.Errorf("unknown root module %q", root)
	}
	peer = strings.ToLower(peer)
	if n.Links[peer] != nil {
		return nil, fmt.Errorf("link to %s already exists", peer)
	}
	if n.full() {
		return nil, errMaxLinks
	}
	lk := newLink(n.ID, peer)
	lk.State.Root = root
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
		if l.CachedResponse != nil && counter == l.Counter-1 {
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
	resp := txResponse{OK: ok, Timestamp: ts}
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
	l.CachedResponse = nil
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
	OK        bool  `json:"ok"`
	Timestamp int64 `json:"timestamp"`
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
// dispatched by name. Returns (ok, routes, error).
func (n *Node) dispatchRequest(peer, fullMethod string, payload json.RawMessage, ts int64) (bool, []string, error) {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
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
				return false, nil, fmt.Errorf("link.connect requires name")
			}
			if !n.isRoot(p.Name) {
				return false, nil, fmt.Errorf("unknown root module %q", p.Name)
			}
			if l.State.Status == Connected {
				return false, nil, fmt.Errorf("link already connected")
			}
			if l.State.IntentConnect || l.State.Status == waiting {
				// Both sides must agree on the root module.
				if l.State.Root != "" && l.State.Root != p.Name {
					return false, nil, nil
				}
				if l.State.Root == "" {
					l.State.Root = p.Name
				}
				l.State.Status = Connected
				l.State.IntentConnect = false
				if mod := n.moduleMap[l.State.Root]; mod != nil {
					mod.OnConnect(peer)
				}
				return true, nil, nil
			}
			if n.InviteMode == inviteModeOpen {
				l.State.Root = p.Name
				l.State.Status = Connected
				if mod := n.moduleMap[p.Name]; mod != nil {
					mod.OnConnect(peer)
				}
				return true, nil, nil
			}
			return false, nil, nil
		case "close":
			if n.anyBusy(peer) {
				return false, nil, errRefsHeld
			}
			for name := range l.State.Modules {
				if mod := n.moduleMap[name]; mod != nil {
					mod.OnClose(peer)
				}
			}
			if mod := n.moduleMap[l.State.Root]; mod != nil {
				mod.OnClose(peer)
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
			ok, connected, err := l.reqModule(payload, n.moduleMap, n.InviteMode, n.roots)
			if connected != "" {
				if mod := n.moduleMap[connected]; mod != nil {
					mod.OnConnect(peer)
				}
			}
			return ok, nil, err
		case "close":
			ok, closed, err := l.reqModuleClose(peer, payload, n.moduleMap, n.roots)
			if closed != "" {
				if mod := n.moduleMap[closed]; mod != nil {
					mod.OnClose(peer)
				}
			}
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
	if ns == "link" {
		l := n.Get(peer)
		if l == nil {
			return nil
		}
		switch method {
		case "connect":
			if ok {
				l.State.Status = Connected
				if mod := n.moduleMap[l.State.Root]; mod != nil {
					mod.OnConnect(peer)
				}
			} else {
				l.State.Status = waiting
			}
			l.State.IntentConnect = false
		case "close":
			if ok {
				for name := range l.State.Modules {
					if mod := n.moduleMap[name]; mod != nil {
						mod.OnClose(peer)
					}
				}
				if mod := n.moduleMap[l.State.Root]; mod != nil {
					mod.OnClose(peer)
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
			if connected := l.respModule(payload, ok); connected != "" {
				if mod := n.moduleMap[connected]; mod != nil {
					mod.OnConnect(peer)
				}
			}
		case "close":
			if closed := l.respModuleClose(payload, ok); closed != "" {
				if mod := n.moduleMap[closed]; mod != nil {
					mod.OnClose(peer)
				}
			}
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
	// Cancel opposing intents on established links.
	if lk.State.IntentClose && lk.State.IntentConnect && lk.State.Status != "" {
		lk.State.IntentClose = false
		lk.State.IntentConnect = false
	}
	if lk.State.IntentClose && !n.anyBusy(peer) {
		return &queuedMsg{Method: "link.close"}
	}
	if lk.State.IntentConnect {
		return &queuedMsg{Method: "link.connect", Payload: enc(map[string]string{"name": lk.State.Root})}
	}
	if lk.State.Status == Connected {
		// Module close/connect — root lifecycle is at link level.
		// Close modules first, then connect.
		for _, mod := range lk.State.Modules {
			if mod.IntentClose && mod.IntentConnect && mod.Status != "" {
				mod.IntentClose = false
				mod.IntentConnect = false
			}
		}
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

// reqModule returns (ok, connectedModule, error). connectedModule is non-empty
// when the module just became bilaterally connected, so the caller can fire OnConnect.
func (l *Link) reqModule(payload json.RawMessage, modules map[string]Module, inviteMode inviteMode, roots map[string][]string) (bool, string, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, "", fmt.Errorf("bad payload")
	}
	// Only allow modules declared as children of this link's root.
	if !isChildModule(p.Name, l.State.Root, roots) {
		return false, "", nil
	}
	if _, ok := modules[p.Name]; !ok {
		return false, "", nil // we don't speak this module
	}
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	mod := l.State.Modules[p.Name]
	// Already connected — idempotent ACK.
	if mod != nil && mod.Status == Connected {
		return true, "", nil
	}
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == waiting) {
		mod.Status = Connected
		mod.IntentConnect = false
		return true, p.Name, nil
	}
	// Open mode — auto-accept supported modules.
	if inviteMode == inviteModeOpen {
		if mod == nil {
			mod = &ConnectionState{}
			l.State.Modules[p.Name] = mod
		}
		mod.Status = Connected
		return true, p.Name, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	if mod == nil {
		mod = &ConnectionState{}
		l.State.Modules[p.Name] = mod
	}
	mod.Status = Incoming
	return false, "", nil // NACK — not accepted yet
}

// respModule returns the module name when it just became bilaterally connected,
// so the caller can fire OnConnect.
func (l *Link) respModule(payload json.RawMessage, ok bool) string {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return ""
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return ""
	}
	if ok {
		mod.Status = Connected
		mod.IntentConnect = false
		return p.Name
	}
	mod.Status = waiting
	mod.IntentConnect = false
	return ""
}

// reqModuleClose returns (ok, closedModule, error). closedModule is non-empty
// when the module was just removed, so the caller can fire OnClose.
func (l *Link) reqModuleClose(peer string, payload json.RawMessage, modules map[string]Module, roots map[string][]string) (bool, string, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, "", fmt.Errorf("bad payload")
	}
	if !isChildModule(p.Name, l.State.Root, roots) {
		return false, "", nil
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return true, "", nil // already gone
	}
	if m := modules[p.Name]; m != nil && m.Busy(peer) {
		return false, "", errRefsHeld
	}
	delete(l.State.Modules, p.Name)
	return true, p.Name, nil
}

// respModuleClose returns the module name when it was just removed,
// so the caller can fire OnClose.
func (l *Link) respModuleClose(payload json.RawMessage, ok bool) string {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return ""
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return ""
	}
	if ok {
		delete(l.State.Modules, p.Name)
		return p.Name
	}
	mod.IntentClose = false
	return ""
}

// ---------------------------------------------------------------------------
// Account operations
// ---------------------------------------------------------------------------

// HandleAccount dispatches an account API call by namespace.
// Handles locking, commit, and kicks internally — like HandleIncoming.
func (n *Node) handleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	ns, method := splitMethod(fullMethod)

	var result json.RawMessage
	var kicks []string
	var err error

	switch {
	case ns == "link" && method == "set_invite_mode":
		var p struct {
			Mode inviteMode `json:"mode"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case inviteModeClosed, inviteModeInvite, inviteModeOpen:
		default:
			return nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		n.InviteMode = p.Mode
		result = enc("ok")

	case ns == "link" && method == "set_modules":
		var p struct {
			Peer    string   `json:"peer"`
			Modules []string `json:"modules"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		lk.setModules(p.Modules, n.moduleMap, n.roots)
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "link" && method == "accept":
		var p struct {
			Peer string `json:"peer"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if lk.State.Status != Incoming {
			return nil, fmt.Errorf("link to %s is not incoming", p.Peer)
		}
		lk.State.IntentConnect = true
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "link" && method == "reject":
		var p struct {
			Peer string `json:"peer"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if lk.State.Status != Incoming {
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
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.AcceptModule(p.Module); err != nil {
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
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.RejectModule(p.Module); err != nil {
			return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "" && method == "status":
		result = n.status()

	case ns == "link":
		return nil, fmt.Errorf("unknown link method %q", method)

	case ns == "module":
		return nil, fmt.Errorf("unknown module method %q", method)

	default:
		mod := n.moduleMap[ns]
		if mod == nil {
			return nil, fmt.Errorf("unknown method %q", fullMethod)
		}
		result, kicks, err = mod.HandleAccount(method, payload)
	}

	if err == nil {
		n.commit()
		for _, k := range kicks {
			n.kick(k)
		}
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

// connectModules requests connection for the named modules on this link,
// filtering by registered modules. Existing modules are left untouched.
func (l *Link) connectModules(modules []string, registered map[string]Module, roots map[string][]string) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	for _, name := range modules {
		if !isChildModule(name, l.State.Root, roots) {
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
func (l *Link) setModules(modules []string, registered map[string]Module, roots map[string][]string) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	desired := map[string]bool{}
	for _, name := range modules {
		if !isChildModule(name, l.State.Root, roots) {
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
	return n.Get(from) == nil && method != "link.connect" && method != "link.close"
}

// HandleIncoming processes an incoming transaction. The caller is responsible
// for authentication (signature verification).
// Kicks are dispatched under the lock to guarantee they are never lost on
// error paths after a commit.
func (n *Node) handleIncoming(env txRequest) (*txResponse, error) {
	env.From = strings.ToLower(env.From)
	n.mu.Lock()
	defer n.mu.Unlock()

	lk := n.Get(env.From)
	if lk == nil {
		if env.Method == "link.close" {
			return &txResponse{OK: true, Timestamp: nowSecs()}, nil
		}
		if env.Method != "link.connect" {
			return nil, errNoLink
		}
		if n.InviteMode == inviteModeClosed {
			return nil, errInvitesDisabled
		}
		if n.full() {
			return nil, errMaxLinks
		}
		// Validate root module name before creating any state, so a rejected
		// link.connect never leaves a phantom link in n.Links.
		var p struct {
			Name string `json:"name"`
		}
		if env.Payload != nil {
			json.Unmarshal(env.Payload, &p)
		}
		if p.Name == "" || !n.isRoot(p.Name) {
			return nil, fmt.Errorf("unknown root module %q", p.Name)
		}
		lk = newLink(n.ID, env.From)
		lk.State.Root = p.Name
		lk.State.Status = Incoming
		n.Links[env.From] = lk
	}

	now := nowSecs()

	// Piggybacked response.
	if env.Resp != nil && lk.Pending != nil && env.Counter == lk.Counter+1 {
		if err := lk.ValidateTimestamp(env.Resp.Timestamp, now); err != nil {
			return nil, err
		}
		pbRoutes := n.dispatchResponse(env.From, lk.Pending.Method, lk.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		lk.AcceptResponse(env.Resp.Timestamp)
		n.commit()
		for _, r := range pbRoutes {
			n.kick(r)
		}
		n.kick(env.From)
	}

	// Validate counter and collision.
	if cached, err := lk.ValidateRequest(env.Counter); err != nil {
		return nil, err
	} else if cached != nil {
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
		return nil, err
	}

	// Dispatch to handler.
	ok, routes, reqErr := n.dispatchRequest(env.From, env.Method, env.Payload, ts)
	if reqErr != nil {
		return nil, reqErr
	}

	resp := lk.AcceptRequest(env.Counter, ok, ts)
	n.commit()
	n.kick(env.From)
	for _, r := range routes {
		n.kick(r)
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
// fn returns a list of peers to route to. Node commits and dispatches
// under the lock to guarantee routes are never lost.
// The timer is tracked so Stop() can cancel it.
//
// Schedule is called from two contexts: during init (no mutex held)
// and from within a callback (mutex held). The timer append is safe
// in both cases — init has no concurrency, callbacks hold the mutex.
func (n *Node) schedule(delay time.Duration, fn func() []string) {
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.timers, t)
		if n.stopped {
			return
		}
		routes := fn()
		n.commit()
		for _, r := range routes {
			n.kick(r)
		}
	})
	// No mutex lock here — called under mutex (callback) or during
	// init (no concurrency). Locking would deadlock in the callback case.
	n.timers[t] = struct{}{}
}

// Stop cancels all scheduled and retry timers and marks the Node as
// stopped. After Stop, Schedule callbacks and sendPending are no-ops.
// Must be called when deleting a node to prevent zombie timers from
// re-persisting the node's state.
func (n *Node) stop() {
	n.mu.Lock()
	n.stopped = true
	for t := range n.timers {
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
	if n.stopped {
		n.mu.Unlock()
		return
	}
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
	if lk == nil || lk.Pending != msg || lk.Counter != counter {
		n.mu.Unlock()
		return
	}

	if err := lk.ValidateTimestamp(resp.Timestamp, nowSecs()); err != nil {
		log.Printf("[link] %s->%s: bad response timestamp: %v", n.ID, peer, err)
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}

	routes := n.dispatchResponse(peer, msg.Method, msg.Payload, resp.OK, resp.Timestamp)
	lk.AcceptResponse(resp.Timestamp)

	var kickPeer bool
	if n.Get(peer) != nil {
		lk.Pending = n.nextTx(peer)
		kickPeer = lk.Pending != nil
	}
	n.commit()
	lk.Sending = nil
	for _, r := range routes {
		n.kick(r)
	}
	if kickPeer {
		n.kick(peer)
	}
	n.mu.Unlock()
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
