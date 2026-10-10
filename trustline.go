package creditnet

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// Peer — per-peer data owned by the trustline handler
// ---------------------------------------------------------------------------

type Peer struct {
	State            link.ConnectionState `json:"state"`
	Queue            []string             `json:"queue,omitempty"`
	Disconnected     bool                 `json:"disconnected,omitempty"`
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
// TrustlineHandler — implements Module and TrustlineReader
// ---------------------------------------------------------------------------

type TrustlineHandler struct {
	node      *Node
	peers     map[string]*Peer
	reservers []Reserver
}

type trustlineState struct {
	Peers map[string]*Peer `json:"peers"`
}

// HasPeer returns true if peer is a connected trustline peer.
func (h *TrustlineHandler) HasPeer(peer string) bool {
	p := h.peers[peer]
	return p != nil && !p.Disconnected
}

// Peers returns the list of connected trustline peer IDs.
func (h *TrustlineHandler) Peers() []string {
	out := make([]string, 0, len(h.peers))
	for id, p := range h.peers {
		if !p.Disconnected {
			out = append(out, id)
		}
	}
	return out
}

// GetPeer returns the peer data for a connected peer, or nil.
func (h *TrustlineHandler) GetPeer(peer string) *Peer {
	p := h.peers[peer]
	if p == nil || p.Disconnected {
		return nil
	}
	return p
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth. Call after creating module handlers.
func (h *TrustlineHandler) RegisterReserver(r Reserver) {
	h.reservers = append(h.reservers, r)
}

// Bandwidth returns available (in, out) capacity for a peer,
// accounting for effective balance, credit limits, and all reservations.
func (h *TrustlineHandler) Bandwidth(peer string) (in, out int64) {
	p := h.GetPeer(peer)
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

func NewTrustlineHandler(n *Node, state json.RawMessage) *TrustlineHandler {
	return &TrustlineHandler{
		node:  n,
		peers: loadPeers(state),
	}
}

func loadPeers(raw json.RawMessage) map[string]*Peer {
	if raw != nil {
		var st trustlineState
		if json.Unmarshal(raw, &st) == nil && st.Peers != nil {
			return st.Peers
		}
	}
	return map[string]*Peer{}
}

func (h *TrustlineHandler) MarshalJSON() ([]byte, error) {
	return json.Marshal(trustlineState{Peers: h.peers})
}

// LinkDeleted is called when a link to peer is removed.
func (h *TrustlineHandler) LinkDeleted(peer string) {
	if p := h.peers[peer]; p != nil {
		p.Disconnected = true
	}
}

// ---------------------------------------------------------------------------
// Sync — HandleRequest
// ---------------------------------------------------------------------------

func (h *TrustlineHandler) HandleRequest(peerID string, p *Peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "connect":
		peer := h.peers[peerID]
		if peer == nil {
			peer = &Peer{}
			h.peers[peerID] = peer
		}
		if peer.Disconnected {
			peer.Disconnected = false
		}
		if peer.State.IntentConnect || peer.State.Status == link.Waiting {
			peer.State.Status = link.Connected
			peer.State.IntentConnect = false
			return true, nil, nil
		}
		peer.State.Status = link.Incoming
		return false, nil, nil
	case "close":
		peer := h.peers[peerID]
		if peer == nil {
			return true, nil, nil // already gone
		}
		peer.Disconnected = true
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

func (h *TrustlineHandler) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil || peer.Disconnected {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Amount < 0 {
		return false, nil
	}
	peer.CreditLimitIn = p.Amount
	return true, nil
}

func (h *TrustlineHandler) onSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Rate int64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil || peer.Disconnected {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Rate < 0 {
		return false, nil
	}
	peer.MaterializeDemurrage(ts)
	peer.DemurrageIn = p.Rate
	return true, nil
}

func (h *TrustlineHandler) onPayment(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	if p.Amount <= 0 {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil || peer.Disconnected {
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
// Sync — HandleResponse
// ---------------------------------------------------------------------------

func (h *TrustlineHandler) HandleResponse(peerID string, p *Peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
	switch method {
	case "connect":
		if ok {
			peer.State.Status = link.Connected
		} else {
			peer.State.Status = link.Waiting
		}
		peer.State.IntentConnect = false
		return nil
	case "close":
		if ok {
			peer.Disconnected = true
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
	if h.peers[peerID] != nil {
		link.Dequeue(&peer.Queue)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sync — NextTx
// ---------------------------------------------------------------------------

func (h *TrustlineHandler) NextTx(peerID string) *link.QueuedMsg {
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
	// Peer lifecycle.
	if peer.State.IntentClose {
		return &link.QueuedMsg{Method: "close"}
	}
	if peer.State.IntentConnect {
		return &link.QueuedMsg{Method: "connect"}
	}
	if peer.State.Status != link.Connected {
		return nil
	}
	// Operations only when peer is connected.
	method := link.QueueHead(peer.Queue)
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
// Account API
// ---------------------------------------------------------------------------

func (h *TrustlineHandler) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
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
		return h.status()
	}
	return nil, ErrBadMethod
}

func (h *TrustlineHandler) addPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if peer := h.peers[peerID]; peer != nil && !peer.Disconnected {
		return nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if err := h.node.AddLink(peerID); err != nil {
		return nil, err
	}
	// Request trustline module on this link.
	lk := h.node.links[peerID]
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	mod := &link.ConnectionState{IntentConnect: true}
	lk.State.Modules["trustline"] = mod
	peer := h.peers[peerID]
	if peer != nil {
		peer.Disconnected = false // reconnect
	} else {
		peer = &Peer{}
		h.peers[peerID] = peer
	}
	peer.State.IntentConnect = true
	return Enc("ok"), nil
}

func (h *TrustlineHandler) acceptPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if err := h.node.AcceptLink(peerID); err != nil {
		return nil, err
	}
	// Request trustline module on this link.
	lk := h.node.links[peerID]
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	mod := lk.State.Modules["trustline"]
	if mod == nil {
		mod = &link.ConnectionState{}
		lk.State.Modules["trustline"] = mod
	}
	mod.IntentConnect = true
	// Accept peer too — pre-set for mutual detection when trustline.connect arrives.
	peer := h.peers[peerID]
	if peer == nil {
		peer = &Peer{}
		h.peers[peerID] = peer
	}
	peer.State.IntentConnect = true
	h.node.Kick(peerID)
	return Enc("ok"), nil
}

func (h *TrustlineHandler) rejectPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	if err := h.node.RejectLink(strings.ToLower(p.Peer)); err != nil {
		return nil, err
	}
	return Enc("ok"), nil
}

func (h *TrustlineHandler) removePeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.Disconnected {
		// Already disconnected — clean up local peer data.
		delete(h.peers, peerID)
		return Enc("ok"), nil
	}
	peer.State.IntentClose = true
	h.node.Kick(peerID)
	return Enc("ok"), nil
}

func (h *TrustlineHandler) setCreditLimit(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.Disconnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Amount < 0 {
		return nil, fmt.Errorf("credit limit must be non-negative")
	}
	if peer.PendingLimit != nil {
		return nil, fmt.Errorf("credit limit change already pending")
	}
	peer.PendingLimit = &p.Amount
	link.Enqueue(&peer.Queue, "set_credit_limit")
	h.node.Kick(peerID)
	return Enc("ok"), nil
}

func (h *TrustlineHandler) setDemurrage(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.Disconnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if peer.PendingDemurrage != nil {
		return nil, fmt.Errorf("demurrage change already pending")
	}
	peer.PendingDemurrage = &p.Rate
	link.Enqueue(&peer.Queue, "set_demurrage")
	h.node.Kick(peerID)
	return Enc("ok"), nil
}

func (h *TrustlineHandler) payment(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.Disconnected {
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
	link.Enqueue(&peer.Queue, "payment")
	h.node.Kick(peerID)
	return Enc("ok"), nil
}

func (h *TrustlineHandler) status() (json.RawMessage, error) {
	links := h.node.Links()
	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range h.peers {
		status := "connected"
		if p.Disconnected {
			status = "disconnected"
		}
		info := map[string]any{
			"status":            status,
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
		if l := links[id]; l != nil {
			info["link_state"] = l.State
		}
		peers[id] = info
	}

	// Show links without a peer yet (waiting or incoming).
	for id, l := range links {
		if h.peers[id] == nil {
			peers[id] = map[string]any{
				"status":     string(l.State.Status),
				"link_state": l.State,
			}
		}
	}

	return Enc(map[string]any{
		"peers":         peers,
		"allow_invites": h.node.AllowInvites(),
	}), nil
}
