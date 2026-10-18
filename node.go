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
	Connected        bool     `json:"connected"`
	IntentClose      bool     `json:"intent_close,omitempty"` // !Connected && IntentClose = tombstone
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
	if p == nil || !p.Connected {
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
		if peer.IntentClose && !peer.Connected {
			return false, nil, nil // tombstone — rejected
		}
		peer.Connected = true
		peer.IntentClose = false
		h.links.RefModule(peerID, h.name)
		return true, nil, nil
	case "close":
		peer := h.Trustlines[peerID]
		if peer == nil {
			return true, nil, nil // already gone
		}
		peer.Connected = false
		peer.IntentClose = true
		h.links.UnrefModule(peerID, h.name)
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
	if peer == nil || !peer.Connected {
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
	if peer == nil || !peer.Connected {
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
	if peer == nil || !peer.Connected {
		return false, fmt.Errorf("peer not connected")
	}
	eff := peer.EffectiveBalance(ts)
	if eff+p.Amount > peer.CreditLimit {
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
			peer.Connected = true
			h.links.RefModule(peerID, h.name)
		}
		// If rejected, leave trustline — NextTx will retry.
		return nil
	case "close":
		if ok {
			peer.Connected = false
			h.links.UnrefModule(peerID, h.name)
		} else {
			peer.IntentClose = false
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
			"connected":         p.Connected,
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
	if peer.IntentClose {
		return "close", nil, 0
	}
	if !peer.Connected {
		return "connect", nil, 0
	}
	// Operations only when peer is connected.
	method := peer.Queue.Head()
	if method == "" {
		return "", nil, 0
	}
	switch method {
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
	if peer := h.Trustlines[peerID]; peer != nil && peer.Connected {
		return nil, nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if lk := h.links.Get(peerID); lk != nil {
		if lk.Closing() {
			return nil, nil, fmt.Errorf("link to %s is closing", peerID)
		}
	} else {
		// Create outgoing link.
		if peerID == h.nodeID {
			return nil, nil, fmt.Errorf("cannot link to self")
		}
		if _, err := h.links.Connect(peerID); err != nil {
			return nil, nil, err
		}
	}
	// Request trustline + chosen modules on this link.
	lk := h.links.Get(peerID)
	lk.ConnectModules(append([]string{"trustline"}, p.Modules...), h.links.ModuleMap())
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
	if !peer.Connected {
		// Not connected yet — just remove locally.
		delete(h.Trustlines, peerID)
		return Enc("ok"), nil, nil
	}
	peer.IntentClose = true
	return Enc("ok"), []string{peerID}, nil
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
	if !peer.Connected {
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
	if !peer.Connected {
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
	if !peer.Connected {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingPayment != nil {
		return nil, nil, fmt.Errorf("payment already pending")
	}
	if p.Amount <= 0 {
		return nil, nil, fmt.Errorf("amount must be positive")
	}
	available := peer.EffectiveBalance(time.Now().Unix()) + peer.CreditLimitIn
	if p.Amount > available {
		return nil, nil, fmt.Errorf("insufficient credit")
	}
	peer.PendingPayment = &p.Amount
	peer.Queue.Enqueue("payment", 0)
	return Enc("ok"), []string{peerID}, nil
}

