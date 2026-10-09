package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// Peer — per-peer data owned by base
// ---------------------------------------------------------------------------

type Peer struct {
	Queue            []string `json:"queue,omitempty"`
	Disconnected     bool     `json:"disconnected,omitempty"`
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
// Handler — implements link.Handler and link.AccountHandler
// ---------------------------------------------------------------------------

type Handler struct {
	node  *link.Node
	peers map[string]*Peer
}

type handlerState struct {
	Peers map[string]*Peer `json:"peers"`
}

// HasPeer returns true if peer is a connected trustline peer.
func (h *Handler) HasPeer(peer string) bool {
	p := h.peers[peer]
	return p != nil && !p.Disconnected
}

// Peers returns the list of connected trustline peer IDs.
func (h *Handler) Peers() []string {
	out := make([]string, 0, len(h.peers))
	for id, p := range h.peers {
		if !p.Disconnected {
			out = append(out, id)
		}
	}
	return out
}

// GetPeer returns the peer data for a connected peer, or nil.
func (h *Handler) GetPeer(peer string) *Peer {
	p := h.peers[peer]
	if p == nil || p.Disconnected {
		return nil
	}
	return p
}

func NewHandler(n *link.Node) *Handler {
	h := &Handler{
		node:  n,
		peers: map[string]*Peer{},
	}
	if raw := n.HandlerState("trustline"); raw != nil {
		var st handlerState
		if json.Unmarshal(raw, &st) == nil && st.Peers != nil {
			h.peers = st.Peers
		}
	}
	n.RegisterOwner("trustline", link.LinkOwner{
		OnConnect: h.onConnect,
		OnClose:   h.onClose,
	})
	return h
}

func (h *Handler) MarshalJSON() ([]byte, error) {
	return json.Marshal(handlerState{Peers: h.peers})
}

// ---------------------------------------------------------------------------
// Owner callbacks (constructor / destructor)
// ---------------------------------------------------------------------------

// onConnect is called when a link with protocol "trustline" reaches Connected.
func (h *Handler) onConnect(peer string, lk *link.Link) {
	h.peers[peer] = &Peer{}
}

// onClose is called before a link with protocol "base" is deleted.
func (h *Handler) onClose(peer string, lk *link.Link) {
	if p := h.peers[peer]; p != nil {
		p.Disconnected = true
	}
}

// ---------------------------------------------------------------------------
// Sync — HandleRequest
// ---------------------------------------------------------------------------

func (h *Handler) HandleRequest(peerID string, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
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
	return false, nil, link.ErrBadMethod
}

func (h *Handler) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
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

func (h *Handler) onSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, error) {
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

func (h *Handler) onPayment(peerID string, payload json.RawMessage, ts int64) (bool, error) {
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

func (h *Handler) HandleResponse(peerID string, method string, payload json.RawMessage, ok bool, ts int64) []string {
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
	switch method {
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

func (h *Handler) NextTx(peerID string) *link.QueuedMsg {
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
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
			Payload: link.Enc(map[string]int64{"amount": *peer.PendingLimit}),
		}
	case "set_demurrage":
		if peer.PendingDemurrage == nil {
			return nil
		}
		return &link.QueuedMsg{
			Method:  "set_demurrage",
			Payload: link.Enc(map[string]int64{"rate": *peer.PendingDemurrage}),
		}
	case "payment":
		if peer.PendingPayment == nil {
			return nil
		}
		return &link.QueuedMsg{
			Method:  "payment",
			Payload: link.Enc(map[string]int64{"amount": *peer.PendingPayment}),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sync — ResetState
// ---------------------------------------------------------------------------

func (h *Handler) ResetState(raw json.RawMessage) {
	var st handlerState
	if json.Unmarshal(raw, &st) == nil && st.Peers != nil {
		h.peers = st.Peers
	}
}

// ---------------------------------------------------------------------------
// Account API
// ---------------------------------------------------------------------------

func (h *Handler) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
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
	return nil, link.ErrBadMethod
}

func (h *Handler) addPeer(payload json.RawMessage) (json.RawMessage, error) {
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
	if err := h.node.AddLink(peerID, "trustline"); err != nil {
		return nil, err
	}
	return link.Enc("ok"), nil
}

func (h *Handler) acceptPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	if err := h.node.AcceptLink(strings.ToLower(p.Peer)); err != nil {
		return nil, err
	}
	return link.Enc("ok"), nil
}

func (h *Handler) rejectPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	if err := h.node.RejectLink(strings.ToLower(p.Peer)); err != nil {
		return nil, err
	}
	return link.Enc("ok"), nil
}

func (h *Handler) removePeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer != nil && peer.Disconnected {
		// Link already gone — clean up local peer data.
		delete(h.peers, peerID)
		return link.Enc("ok"), nil
	}
	// Close the link (covers both connected and waiting).
	if err := h.node.RemoveLink(peerID); err != nil {
		return nil, err
	}
	return link.Enc("ok"), nil
}

func (h *Handler) setCreditLimit(payload json.RawMessage) (json.RawMessage, error) {
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
	return link.Enc("ok"), nil
}

func (h *Handler) setDemurrage(payload json.RawMessage) (json.RawMessage, error) {
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
	return link.Enc("ok"), nil
}

func (h *Handler) payment(payload json.RawMessage) (json.RawMessage, error) {
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
	return link.Enc("ok"), nil
}

func (h *Handler) status() (json.RawMessage, error) {
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

	return link.Enc(map[string]any{
		"peers":         peers,
		"allow_invites": h.node.AllowInvites(),
	}), nil
}
