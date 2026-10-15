package creditnet

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Trustline — per-peer credit data
// ---------------------------------------------------------------------------

type Trustline struct {
	State            link.ConnectionState `json:"state"`
	Queue            []string             `json:"queue,omitempty"`
	CreditLimit      int64    `json:"credit_limit"`
	CreditLimitIn    int64    `json:"credit_limit_in"`
	Balance          int64    `json:"balance"`
	Demurrage        int64    `json:"demurrage"`        // my rate (promille/year)
	DemurrageIn      int64    `json:"demurrage_in"`     // peer's rate
	DemurrageTime    int64    `json:"demurrage_time"`   // last materialized (secs)
	PendingLimit     *int64   `json:"pending_limit,omitempty"`
	PendingPayment   *int64   `json:"pending_payment,omitempty"`
	PendingDemurrage *int64   `json:"pending_demurrage,omitempty"`
}

// ---------------------------------------------------------------------------
// Node — the app-level node, holding trustlines and providing bandwidth
// ---------------------------------------------------------------------------

// Node is the central app-level entity. It implements link.Module for
// trustline protocol messages, and provides the Trustlines map and
// Bandwidth function that other modules depend on.
type Node struct {
	Trustlines map[string]*Trustline `json:"trustlines"`
	nodeID     string
	links      *link.Links
	reservers  []Reserver
	kick       func(string)
}

// NewNode creates a Node from persisted state.
func NewNode(nodeID string, links *link.Links, kick func(string), state json.RawMessage) *Node {
	h := &Node{
		Trustlines: map[string]*Trustline{},
		nodeID:     nodeID,
		links:      links,
		kick:       kick,
	}
	if state != nil {
		json.Unmarshal(state, h)
	}
	if h.Trustlines == nil {
		h.Trustlines = map[string]*Trustline{}
	}
	return h
}

func (h *Node) Name() string { return "trustline" }

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth.
func (h *Node) RegisterReserver(r Reserver) {
	h.reservers = append(h.reservers, r)
}

// GetTrustline returns the trustline data for a connected peer, or nil.
func (h *Node) GetTrustline(peer string) *Trustline {
	p := h.Trustlines[peer]
	if p == nil || p.State.Status != link.Connected {
		return nil
	}
	return p
}

// Bandwidth returns available (in, out) capacity for a peer,
// accounting for effective balance, credit limits, and all reservations.
func (h *Node) Bandwidth(peer string) (in, out int64) {
	p := h.GetTrustline(peer)
	if p == nil {
		return 0, 0
	}
	eff := p.EffectiveBalance(time.Now().Unix())
	var ri, ro int64
	for _, r := range h.reservers {
		i, o := r.Reserved(peer)
		ri += i
		ro += o
	}
	in = p.CreditLimit - eff - ri
	out = p.CreditLimitIn + eff - ro
	return
}

// refLink increments the ref count on the link's ConnectionState.
func (h *Node) refLink(peer string) {
	if lk := h.links.Get(peer); lk != nil {
		lk.State.Ref()
	}
}

// unrefLink decrements the ref count on the link's ConnectionState.
func (h *Node) unrefLink(peer string) {
	if lk := h.links.Get(peer); lk != nil {
		lk.State.UnrefClose()
	}
}

// ---------------------------------------------------------------------------
// link.Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Node) HandleRequest(peerID, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "connect":
		peer := h.Trustlines[peerID]
		if peer == nil {
			peer = &Trustline{}
			h.Trustlines[peerID] = peer
		}
		if peer.State.IntentConnect || peer.State.Status == link.Waiting {
			peer.State.Status = link.Connected
			peer.State.IntentConnect = false
			h.refLink(peerID)
			return true, nil, nil
		}
		if h.links.InviteMode == link.InviteModeOpen {
			peer.State.Status = link.Connected
			h.refLink(peerID)
			return true, nil, nil
		}
		peer.State.Status = link.Incoming
		return false, nil, nil
	case "close":
		peer := h.Trustlines[peerID]
		if peer == nil {
			return true, nil, nil // already gone
		}
		h.unrefLink(peerID)
		peer.State = link.ConnectionState{}
		return true, nil, nil
	case "set_credit_limit":
		ok, err := h.onSetCreditLimit(peerID, payload)
		return ok, nil, err
	case "set_demurrage":
		ok, err := h.onSetDemurrage(peerID, payload, ts)
		return ok, nil, err
	case "payment":
		ok, err := h.onPayment(peerID, payload, ts)
		return ok, nil, err
	}
	return false, nil, ErrBadMethod
}

func (h *Node) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.Trustlines[peerID]
	if peer == nil || peer.State.Status != link.Connected {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Amount < 0 {
		return false, nil
	}
	peer.CreditLimitIn = p.Amount
	return true, nil
}

func (h *Node) onSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Rate int64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.Trustlines[peerID]
	if peer == nil || peer.State.Status != link.Connected {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Rate < 0 {
		return false, nil
	}
	peer.MaterializeDemurrage(ts)
	peer.DemurrageIn = p.Rate
	return true, nil
}

func (h *Node) onPayment(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	if p.Amount <= 0 {
		return false, nil
	}
	peer := h.Trustlines[peerID]
	if peer == nil || peer.State.Status != link.Connected {
		return false, fmt.Errorf("peer not connected")
	}
	peer.MaterializeDemurrage(ts)
	newBalance := peer.Balance + p.Amount
	if newBalance > peer.CreditLimit {
		return false, nil
	}
	peer.Balance = newBalance
	return true, nil
}

// ---------------------------------------------------------------------------
// link.Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Node) HandleResponse(peerID, method string, payload json.RawMessage, ok bool, ts int64) []string {
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil
	}
	switch method {
	case "connect":
		if ok {
			peer.State.Status = link.Connected
			h.refLink(peerID)
		} else {
			peer.State.Status = link.Waiting
		}
		peer.State.IntentConnect = false
		return nil
	case "close":
		if ok {
			h.unrefLink(peerID)
			peer.State = link.ConnectionState{}
		} else {
			peer.State.IntentClose = false
		}
		return nil
	case "set_credit_limit":
		if ok && peer.PendingLimit != nil {
			peer.CreditLimit = *peer.PendingLimit
		}
		peer.PendingLimit = nil
	case "set_demurrage":
		if ok && peer.PendingDemurrage != nil {
			peer.MaterializeDemurrage(ts)
			peer.Demurrage = *peer.PendingDemurrage
		}
		peer.PendingDemurrage = nil
	case "payment":
		if ok && peer.PendingPayment != nil {
			peer.AddBalance(-*peer.PendingPayment, ts)
		}
		peer.PendingPayment = nil
	}
	if h.Trustlines[peerID] != nil {
		Dequeue(&peer.Queue)
	}
	return nil
}

// ---------------------------------------------------------------------------
// link.Module — NextTx
// ---------------------------------------------------------------------------

func (h *Node) NextTx(peerID string) *link.QueuedMsg {
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil
	}
	// Trustline lifecycle.
	if peer.State.IntentClose && peer.State.Refs == 0 {
		return &link.QueuedMsg{Method: "close"}
	}
	if peer.State.IntentConnect {
		return &link.QueuedMsg{Method: "connect"}
	}
	if peer.State.Status != link.Connected {
		return nil
	}
	// Operations only when peer is connected.
	method := QueueHead(peer.Queue)
	if method == "" {
		return nil
	}
	switch method {
	case "set_credit_limit":
		if peer.PendingLimit == nil {
			return nil
		}
		return &link.QueuedMsg{
			Method:  "set_credit_limit",
			Payload: Enc(map[string]int64{"amount": *peer.PendingLimit}),
		}
	case "set_demurrage":
		if peer.PendingDemurrage == nil {
			return nil
		}
		return &link.QueuedMsg{
			Method:  "set_demurrage",
			Payload: Enc(map[string]int64{"rate": *peer.PendingDemurrage}),
		}
	case "payment":
		if peer.PendingPayment == nil {
			return nil
		}
		return &link.QueuedMsg{
			Method:  "payment",
			Payload: Enc(map[string]int64{"amount": *peer.PendingPayment}),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// link.Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Node) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "add_peer":
		return h.addPeer(payload)
	case "accept_peer":
		return h.acceptPeer(payload)
	case "reject_peer":
		return h.rejectPeer(payload)
	case "remove_peer":
		return h.removePeer(payload)
	case "set_credit_limit":
		return h.setCreditLimit(payload)
	case "set_demurrage":
		return h.setDemurrage(payload)
	case "payment":
		return h.payment(payload)
	case "status":
		return h.trustlineStatus()
	}
	return nil, ErrBadMethod
}

func (h *Node) addPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules,omitempty"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if peer := h.Trustlines[peerID]; peer != nil && peer.State.Status == link.Connected {
		return nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if lk := h.links.Get(peerID); lk != nil {
		if lk.State.IntentClose {
			return nil, fmt.Errorf("link to %s is closing", peerID)
		}
	} else {
		// Create link (inline AddLink logic).
		if _, _, err := ParseIdentity(peerID); err != nil {
			return nil, err
		}
		if peerID == h.nodeID {
			return nil, fmt.Errorf("cannot link to self")
		}
		if h.links.Full() {
			return nil, fmt.Errorf("max links reached")
		}
		lk := h.links.Create(peerID)
		lk.State.IntentConnect = true
	}
	// Request trustline + chosen modules on this link.
	lk := h.links.Get(peerID)
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	lk.State.Modules["trustline"] = &link.ConnectionState{IntentConnect: true}
	for _, name := range p.Modules {
		if h.links.ModuleMap()[name] != nil {
			lk.State.Modules[name] = &link.ConnectionState{IntentConnect: true}
		}
	}
	peer := h.Trustlines[peerID]
	if peer == nil {
		peer = &Trustline{}
		h.Trustlines[peerID] = peer
	}
	peer.State.IntentConnect = true
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) acceptPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules,omitempty"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	// Accept link (inline AcceptLink logic).
	lk := h.links.Get(peerID)
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", peerID)
	}
	if lk.State.Status != link.Incoming {
		return nil, fmt.Errorf("link to %s is not incoming", peerID)
	}
	lk.State.IntentConnect = true
	// Request trustline + chosen modules on this link.
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	if lk.State.Modules["trustline"] == nil {
		lk.State.Modules["trustline"] = &link.ConnectionState{}
	}
	lk.State.Modules["trustline"].IntentConnect = true
	for _, name := range p.Modules {
		if h.links.ModuleMap()[name] != nil {
			if lk.State.Modules[name] == nil {
				lk.State.Modules[name] = &link.ConnectionState{}
			}
			lk.State.Modules[name].IntentConnect = true
		}
	}
	// Accept peer too — pre-set for mutual detection when trustline.connect arrives.
	peer := h.Trustlines[peerID]
	if peer == nil {
		peer = &Trustline{}
		h.Trustlines[peerID] = peer
	}
	peer.State.IntentConnect = true
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) rejectPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	// Reject link (inline RejectLink logic).
	lk := h.links.Get(peerID)
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", peerID)
	}
	if lk.State.Status != link.Incoming {
		return nil, fmt.Errorf("link to %s is not incoming", peerID)
	}
	lk.State.IntentClose = true
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) removePeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State.Status != link.Connected {
		// Already disconnected — clean up local peer data.
		delete(h.Trustlines, peerID)
		return Enc("ok"), nil
	}
	peer.State.IntentClose = true
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) setCreditLimit(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State.Status != link.Connected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Amount < 0 {
		return nil, fmt.Errorf("credit limit must be non-negative")
	}
	if peer.PendingLimit != nil {
		return nil, fmt.Errorf("credit limit change already pending")
	}
	peer.PendingLimit = &p.Amount
	Enqueue(&peer.Queue, "set_credit_limit")
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) setDemurrage(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State.Status != link.Connected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if peer.PendingDemurrage != nil {
		return nil, fmt.Errorf("demurrage change already pending")
	}
	peer.PendingDemurrage = &p.Rate
	Enqueue(&peer.Queue, "set_demurrage")
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) payment(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State.Status != link.Connected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingPayment != nil {
		return nil, fmt.Errorf("payment already pending")
	}
	if p.Amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	available := peer.EffectiveBalance(time.Now().Unix()) + peer.CreditLimitIn
	if p.Amount > available {
		return nil, fmt.Errorf("insufficient credit")
	}
	peer.PendingPayment = &p.Amount
	Enqueue(&peer.Queue, "payment")
	h.kick(peerID)
	return Enc("ok"), nil
}

func (h *Node) trustlineStatus() (json.RawMessage, error) {
	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range h.Trustlines {
		info := map[string]any{
			"peer_state":        p.State,
			"credit_limit":      p.CreditLimit,
			"credit_limit_in":   p.CreditLimitIn,
			"balance":           p.Balance,
			"effective_balance": p.EffectiveBalance(now),
			"demurrage":         p.Demurrage,
			"demurrage_in":      p.DemurrageIn,
		}
		if p.PendingLimit != nil {
			info["pending_limit"] = *p.PendingLimit
		}
		if p.PendingPayment != nil {
			info["pending_payment"] = *p.PendingPayment
		}
		if p.PendingDemurrage != nil {
			info["pending_demurrage"] = *p.PendingDemurrage
		}
		if l := h.links.Get(id); l != nil {
			info["link_state"] = l.State
		}
		peers[id] = info
	}

	// Show links without a peer yet (waiting or incoming).
	for id, l := range h.links.Map() {
		if h.Trustlines[id] == nil {
			peers[id] = map[string]any{
				"status":     string(l.State.Status),
				"link_state": l.State,
			}
		}
	}

	return Enc(map[string]any{
		"peers":       peers,
		"invite_mode": h.links.InviteMode,
	}), nil
}
