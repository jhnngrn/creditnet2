package creditnet

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jhnngrn/mimesis"
)

// ---------------------------------------------------------------------------
// Trustline — per-peer credit data
// ---------------------------------------------------------------------------

type Trustline struct {
	State            string   `json:"state,omitempty"` // "", "connected", "shutdown"
	Queue            Queue `json:"queue"`
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
	name       string
	nodeID     string
	links      *mimesis.Node
	reservers  []Reserver
}

// NewNode creates a Node from persisted state.
func NewNode(name, nodeID string, links *mimesis.Node, state json.RawMessage) *Node {
	h := &Node{
		Trustlines: map[string]*Trustline{},
		name:       name,
		nodeID:     nodeID,
		links:      links,
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("node %s: corrupt trustline state: %v", nodeID, err)
		}
	}
	if h.Trustlines == nil {
		h.Trustlines = map[string]*Trustline{}
	}
	return h
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth.
func (h *Node) RegisterReserver(r Reserver) {
	h.reservers = append(h.reservers, r)
}

// GetTrustline returns the trustline data for a connected peer, or nil.
func (h *Node) GetTrustline(peer string) *Trustline {
	p := h.Trustlines[peer]
	if p == nil || p.State != "connected" {
		return nil
	}
	return p
}

// PendingBalance returns effective balance minus reservations.
// Positive means debt exists: peer owes us (OUT) or we owe them (IN).
func (h *Node) PendingBalance(peer string, side int) int64 {
	p := h.GetTrustline(peer)
	if p == nil {
		return 0
	}
	eff := p.EffectiveBalance(time.Now().Unix())
	var debt int64
	if side == OUT {
		debt = eff
	} else {
		debt = -eff
	}
	for _, r := range h.reservers {
		debt -= r.Reserved(peer, side)
	}
	return debt
}

// Bandwidth returns available capacity for a peer on a given side.
// bw = CreditLimit + PendingBalance, floored at 0.
func (h *Node) Bandwidth(peer string, side int) int64 {
	p := h.GetTrustline(peer)
	if p == nil {
		return 0
	}
	var limit int64
	if side == OUT {
		limit = p.CreditLimitIn
	} else {
		limit = p.CreditLimit
	}
	total := limit + h.PendingBalance(peer, side)
	if total < 0 {
		return 0
	}
	return total
}


// OnConnect is called by the framework when app.connect completes bilaterally.
func (h *Node) OnConnect(peer string) {
	p := h.Trustlines[peer]
	if p == nil {
		p = &Trustline{}
		h.Trustlines[peer] = p
	}
	p.State = "connected"
}

// Busy reports whether this module has active bilateral state with the peer.
// A trustline is busy from connect handshake until close handshake completes.
func (h *Node) Busy(peer string) bool {
	p := h.Trustlines[peer]
	return p != nil && p.State == "connected"
}

// ---------------------------------------------------------------------------
// link.Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Node) HandleRequest(peerID, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "connect":
		// Module consent is sufficient — auto-accept.
		peer := h.Trustlines[peerID]
		if peer == nil {
			peer = &Trustline{}
			h.Trustlines[peerID] = peer
		}
		if peer.State == "shutdown" {
			return false, nil, nil // tombstone — rejected
		}
		peer.State = "connected"
		return true, nil, nil
	case "close":
		peer := h.Trustlines[peerID]
		if peer == nil {
			return true, nil, nil // already gone
		}
		peer.State = "shutdown"
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
	if peer == nil || peer.State != "connected" {
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
	if peer == nil || peer.State != "connected" {
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
	if peer == nil || peer.State != "connected" {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Amount > h.Bandwidth(peerID, IN) {
		return false, nil
	}
	peer.AddBalance(p.Amount, ts)
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
			peer.State = "connected"
		} else {
			delete(h.Trustlines, peerID)
		}
		return nil
	case "close":
		if ok {
			peer.State = "shutdown"
		}
		// Fall through to dequeue.
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
		peer.Queue.Dequeue()
	}
	return nil
}

// ---------------------------------------------------------------------------
// link.Module — Status
// ---------------------------------------------------------------------------

func (h *Node) Status() json.RawMessage {
	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range h.Trustlines {
		info := map[string]any{
			"state":             p.State,
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
		peers[id] = info
	}
	return Enc(map[string]any{"peers": peers})
}

// link.Module — NextTx
// ---------------------------------------------------------------------------

func (h *Node) NextTx(peerID string) (string, json.RawMessage, int64) {
	peer := h.Trustlines[peerID]
	if peer == nil {
		return "", nil, 0
	}
	// Trustline lifecycle.
	if peer.State == "" {
		return "connect", nil, 0
	}
	if peer.State == "shutdown" {
		return "", nil, 0
	}
	// State == "connected" — process queue.
	method := peer.Queue.Head()
	if method == "" {
		return "", nil, 0
	}
	switch method {
	case "close":
		return "close", nil, 0
	case "set_credit_limit":
		if peer.PendingLimit == nil {
			return "", nil, 0
		}
		return "set_credit_limit", Enc(map[string]int64{"amount": *peer.PendingLimit}), 0
	case "set_demurrage":
		if peer.PendingDemurrage == nil {
			return "", nil, 0
		}
		return "set_demurrage", Enc(map[string]int64{"rate": *peer.PendingDemurrage}), 0
	case "payment":
		if peer.PendingPayment == nil {
			return "", nil, 0
		}
		return "payment", Enc(map[string]int64{"amount": *peer.PendingPayment}), 0
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// link.Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Node) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "add_peer":
		return h.addPeer(payload)
	case "remove_peer":
		return h.removePeer(payload)
	case "set_credit_limit":
		return h.setCreditLimit(payload)
	case "set_demurrage":
		return h.setDemurrage(payload)
	case "payment":
		return h.payment(payload)
	}
	return nil, nil, ErrBadMethod
}

func (h *Node) addPeer(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules,omitempty"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if lk := h.links.Get(peerID); lk != nil {
		return nil, nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if peerID == h.nodeID {
		return nil, nil, fmt.Errorf("cannot link to self")
	}
	if _, err := h.links.Connect(peerID, "trustline"); err != nil {
		return nil, nil, err
	}
	// Request chosen child modules on this link.
	if len(p.Modules) > 0 {
		lk := h.links.Get(peerID)
		lk.ConnectModules(p.Modules, h.links.ModuleMap())
	}
	if h.Trustlines[peerID] == nil {
		h.Trustlines[peerID] = &Trustline{}
	}
	return Enc("ok"), []string{peerID}, nil
}


func (h *Node) removePeer(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, nil, fmt.Errorf("no peer %s", peerID)
	}
	if lk := h.links.Get(peerID); lk != nil {
		// Link exists — close it (app.close fires when modules are done).
		lk.Close()
		return Enc("ok"), []string{peerID}, nil
	}
	// No link — app already closed, safe to remove trustline data.
	delete(h.Trustlines, peerID)
	return Enc("ok"), nil, nil
}

func (h *Node) setCreditLimit(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != "connected" {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Amount < 0 {
		return nil, nil, fmt.Errorf("credit limit must be non-negative")
	}
	if peer.PendingLimit != nil {
		return nil, nil, fmt.Errorf("credit limit change already pending")
	}
	peer.PendingLimit = &p.Amount
	peer.Queue.Enqueue("set_credit_limit", 0)
	return Enc("ok"), []string{peerID}, nil
}

func (h *Node) setDemurrage(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != "connected" {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if peer.PendingDemurrage != nil {
		return nil, nil, fmt.Errorf("demurrage change already pending")
	}
	peer.PendingDemurrage = &p.Rate
	peer.Queue.Enqueue("set_demurrage", 0)
	return Enc("ok"), []string{peerID}, nil
}

func (h *Node) payment(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.Trustlines[peerID]
	if peer == nil {
		return nil, nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != "connected" {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingPayment != nil {
		return nil, nil, fmt.Errorf("payment already pending")
	}
	if p.Amount <= 0 {
		return nil, nil, fmt.Errorf("amount must be positive")
	}
	if p.Amount > h.Bandwidth(peerID, OUT) {
		return nil, nil, fmt.Errorf("insufficient credit")
	}
	peer.PendingPayment = &p.Amount
	peer.Queue.Enqueue("payment", 0)
	return Enc("ok"), []string{peerID}, nil
}

