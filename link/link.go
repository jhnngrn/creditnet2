// Package link defines the bilateral sync primitive — the data structures
// and protocol types for a single link between two parties.
package link

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ---------------------------------------------------------------------------
// Errors — link-level protocol errors
// ---------------------------------------------------------------------------

var (
	ErrStaleCounter = errors.New("stale counter")
	ErrCounterAhead = errors.New("counter ahead")
	ErrCollision    = errors.New("collision")
)

// ---------------------------------------------------------------------------
// Link — bilateral sync state
// ---------------------------------------------------------------------------

// LinkStatus — what the link IS.
type ConnectionStatus string

const (
	Waiting   ConnectionStatus = "waiting"   // proposed, awaiting peer decision
	Incoming  ConnectionStatus = "incoming"  // peer proposed, awaiting local decision
	Connected ConnectionStatus = "connected" // bilaterally established
)

// ConnectionState is the bilateral connection state: what IS (Status),
// what to DO next (IntentConnect/IntentClose), and who holds it open (Refs).
// Close is deferred while Refs > 0.
type ConnectionState struct {
	Status        ConnectionStatus `json:"status,omitempty"`
	IntentConnect bool             `json:"intent_connect,omitempty"`
	IntentClose   bool             `json:"intent_close,omitempty"`
	Refs          uint             `json:"refs,omitempty"`
}

// Ref increments the reference count.
func (c *ConnectionState) Ref()   { c.Refs++ }

// Unref decrements the reference count.
func (c *ConnectionState) Unref() { if c.Refs > 0 { c.Refs-- } }

// UnrefClose decrements refs and sets IntentClose when refs reach 0.
func (c *ConnectionState) UnrefClose() {
	if c.Refs > 0 {
		c.Refs--
	}
	if c.Refs == 0 {
		c.IntentClose = true
	}
}

// LinkState is a ConnectionState (for the link itself) plus per-module connections.
type LinkState struct {
	ConnectionState
	Modules map[string]*ConnectionState `json:"modules,omitempty"`
}

// QueuedMsg is a message queued for sending on a link.
type QueuedMsg struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *SyncResponse `json:"cached_response,omitempty"`
	ReqTimestamp   int64         `json:"req_ts"`
	RespTimestamp  int64         `json:"resp_ts"`
}

// ModuleEnabled returns true if the named module is bilaterally active on this link.
func (l *Link) ModuleEnabled(name string) bool {
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected
}

// Dominant returns true when this side owns the current counter slot.
func (l *Link) Dominant() bool { return l.Counter%2 == l.TurnBit }

// NewLink creates a Link with the turn bit derived from identity ordering.
func NewLink(self, peer string) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{TurnBit: bit}
}

// ---------------------------------------------------------------------------
// Links — collection of links for a node
// ---------------------------------------------------------------------------

// Links manages the set of bilateral links for a node.
type Links struct {
	m    map[string]*Link
	self string
}

// NewLinks creates a Links collection for the given node identity.
func NewLinks(self string, m map[string]*Link) *Links {
	if m == nil {
		m = map[string]*Link{}
	}
	return &Links{m: m, self: self}
}

func (ls *Links) Get(peer string) *Link { return ls.m[peer] }
func (ls *Links) Len() int              { return len(ls.m) }
func (ls *Links) Full() bool            { return len(ls.m) >= MaxLinks }
func (ls *Links) Delete(peer string)    { delete(ls.m, peer) }
func (ls *Links) Map() map[string]*Link { return ls.m }

// Create adds a new link to peer and returns it.
func (ls *Links) Create(peer string) *Link {
	lk := NewLink(ls.self, peer)
	ls.m[peer] = lk
	return lk
}

// ---------------------------------------------------------------------------
// Sync lifecycle
// ---------------------------------------------------------------------------

// ValidateRequest checks whether an incoming request at the given counter
// is valid for this link. Returns:
//   - (*SyncResponse, nil) — stale counter but cached response exists
//   - (nil, error) — invalid (stale without cache, ahead, or collision)
//   - (nil, nil) — valid, proceed with dispatch
func (l *Link) ValidateRequest(counter uint64) (*SyncResponse, error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && l.CachedResponse.Counter == counter {
			resp := *l.CachedResponse
			return &resp, nil
		}
		return nil, ErrStaleCounter
	}
	if counter > l.Counter {
		return nil, ErrCounterAhead
	}
	if l.Pending != nil && l.Dominant() {
		return nil, ErrCollision
	}
	return nil, nil
}

// AcceptRequest finalizes an incoming request. Call after the handler
// has processed the request. Returns the response to send back.
func (l *Link) AcceptRequest(reqCounter uint64, ok bool, ts int64) *SyncResponse {
	resp := SyncResponse{Counter: reqCounter, OK: ok, Timestamp: ts}
	l.CachedResponse = &resp
	l.Counter++
	l.ReqTimestamp = ts
	l.Pending = nil
	return &resp
}

// AcceptResponse finalizes a received response to our outgoing message.
// Call after the response handler has processed the response.
func (l *Link) AcceptResponse(ts int64) {
	l.Counter++
	l.RespTimestamp = ts
	l.Pending = nil
}


// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

// SyncRequest is the wire envelope for an outgoing sync message.
type SyncRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *SyncResponse   `json:"resp,omitempty"`
}

// SyncResponse is the wire envelope for a sync response.
type SyncResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Queue helpers
// ---------------------------------------------------------------------------

func Enqueue(q *[]string, method string) { *q = append(*q, method) }
func Dequeue(q *[]string)                { if len(*q) > 0 { *q = (*q)[1:] } }
func QueueHead(q []string) string        { if len(q) > 0 { return q[0] }; return "" }

// ---------------------------------------------------------------------------
// InviteMode — node-level policy for incoming connections/modules
// ---------------------------------------------------------------------------

type InviteMode string

const (
	InviteModeClosed InviteMode = "closed" // reject all incoming
	InviteModeInvite InviteMode = "invite" // incoming, manual accept
	InviteModeOpen   InviteMode = "open"   // auto-accept all
)

const MaxLinks = 64

func enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }

// ---------------------------------------------------------------------------
// Link-level request/response handling
// ---------------------------------------------------------------------------

// HandleRequest processes an incoming link-namespace request.
// Returns (ok, deleteLink, error). When deleteLink is true the caller
// must remove the link and notify modules.
func (l *Link) HandleRequest(method string, payload json.RawMessage, supported map[string]struct{}, inviteMode InviteMode) (bool, bool, error) {
	switch method {
	case "connect":
		if l.State.IntentConnect || l.State.Status == Waiting {
			l.State.Status = Connected
			l.State.IntentConnect = false
			return true, false, nil
		}
		if inviteMode == InviteModeOpen {
			l.State.Status = Connected
			return true, false, nil
		}
		return false, false, nil

	case "close":
		return true, true, nil

	case "module":
		ok, err := l.reqModule(payload, supported, inviteMode)
		return ok, false, err
	}
	return false, false, fmt.Errorf("unknown link method %q", method)
}

// HandleResponse processes a link-namespace response.
// Returns true when the link should be deleted by the caller.
func (l *Link) HandleResponse(method string, payload json.RawMessage, ok bool) bool {
	switch method {
	case "connect":
		if ok {
			l.State.Status = Connected
		} else {
			l.State.Status = Waiting
		}
		l.State.IntentConnect = false

	case "close":
		if ok {
			return true // caller deletes link
		}
		l.State.IntentClose = false

	case "module":
		l.respModule(payload, ok)
	}
	return false
}

// NextTx returns the next link-level message to send, or nil.
func (l *Link) NextTx() *QueuedMsg {
	if l.State.IntentClose && l.State.Refs == 0 {
		return &QueuedMsg{Method: "close"}
	}
	if l.State.IntentConnect {
		return &QueuedMsg{Method: "connect"}
	}
	if l.State.Status == Connected {
		for name, mod := range l.State.Modules {
			if mod.IntentConnect {
				return &QueuedMsg{
					Method:  "module",
					Payload: enc(map[string]string{"name": name}),
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Module negotiation (internal)
// ---------------------------------------------------------------------------

func (l *Link) reqModule(payload json.RawMessage, supported map[string]struct{}, inviteMode InviteMode) (bool, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, fmt.Errorf("bad payload")
	}
	if _, ok := supported[p.Name]; !ok {
		return false, nil // we don't speak this module
	}
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	mod := l.State.Modules[p.Name]
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == Waiting) {
		mod.Status = Connected
		mod.IntentConnect = false
		return true, nil
	}
	// Open mode — auto-accept supported modules.
	if inviteMode == InviteModeOpen {
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
		mod.Status = Waiting
	}
	mod.IntentConnect = false
}

// ---------------------------------------------------------------------------
// Module account operations
// ---------------------------------------------------------------------------

// SetModules proposes the given modules on this link, filtering by supported.
// Modules not in the desired set and not yet connected are removed.
func (l *Link) SetModules(modules []string, supported map[string]struct{}) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	desired := map[string]bool{}
	for _, name := range modules {
		if _, ok := supported[name]; ok {
			desired[name] = true
		}
	}
	for name := range desired {
		mod := l.State.Modules[name]
		if mod == nil {
			l.State.Modules[name] = &ConnectionState{IntentConnect: true}
		} else if mod.Status != Connected {
			mod.IntentConnect = true
		}
	}
	for name, mod := range l.State.Modules {
		if name == "trustline" {
			continue
		}
		if !desired[name] && mod.Status != Connected {
			delete(l.State.Modules, name)
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
func (l *Link) RejectModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	delete(l.State.Modules, name)
	return nil
}
