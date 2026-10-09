package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// PeerStatus / PeerState — peer lifecycle
// ---------------------------------------------------------------------------

type PeerStatus string

const (
	PeerWaiting   PeerStatus = "waiting"   // connect queued or NACKed
	PeerIncoming  PeerStatus = "incoming"  // received base.connect, awaiting local decision
	PeerConnected PeerStatus = "connected" // bilaterally confirmed
	PeerDisconnected PeerStatus = "disconnected" // peer closed the relationship
)

type PeerState struct {
	Status PeerStatus `json:"status"`
	Queue  []string   `json:"queue,omitempty"`
}

// ---------------------------------------------------------------------------
// Peer — per-peer data owned by base
// ---------------------------------------------------------------------------

type Peer struct {
	State            PeerState `json:"state"`  // status + queue
	CreditLimit      int64 `json:"credit_limit"`
	CreditLimitIn    int64 `json:"credit_limit_in"`
	Balance          int64 `json:"balance"`
	Demurrage        int64 `json:"demurrage"`        // my rate (promille/year)
	DemurrageIn      int64 `json:"demurrage_in"`     // peer's rate
	DemurrageTime    int64 `json:"demurrage_time"`   // last materialized (secs)
	PendingLimit     *int64 `json:"pending_limit,omitempty"`
	PendingPayment   *int64 `json:"pending_payment,omitempty"`
	PendingDemurrage *int64 `json:"pending_demurrage,omitempty"`
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

func NewHandler(n *link.Node) link.Handler {
	h := &Handler{
		node:  n,
		peers: map[string]*Peer{},
	}
	if raw := n.HandlerState("base"); raw != nil {
		var st handlerState
		if json.Unmarshal(raw, &st) == nil && st.Peers != nil {
			h.peers = st.Peers
		}
	}
	return h
}

func (h *Handler) MarshalJSON() ([]byte, error) {
	return json.Marshal(handlerState{Peers: h.peers})
}

// ---------------------------------------------------------------------------
// Sync — HandleRequest
// ---------------------------------------------------------------------------

func (h *Handler) HandleRequest(peerID string, lk *link.Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "connect":
		return h.onPeerRequest(peerID)
	case "close":
		return h.onPeerClose(peerID)
	case "set_credit_limit":
		return h.onSetCreditLimit(peerID, payload)
	case "set_demurrage":
		return h.onSetDemurrage(peerID, payload, ts)
	case "payment":
		return h.onPayment(peerID, payload, ts)
	}
	return false, link.ErrBadMethod
}

func (h *Handler) onPeerRequest(peerID string) (bool, error) {
	peer := h.peers[peerID]
	if peer != nil {
		if peer.State.Status == PeerWaiting {
			peer.State.Status = PeerConnected
			if link.QueueHead(peer.State.Queue) == "connect" {
				link.Dequeue(&peer.State.Queue) // mutual connect — no need to send own
			}
			h.node.AcquireLink(peerID)
			return true, nil
		}
		return false, nil
	}
	// No peer exists — create incoming.
	h.peers[peerID] = &Peer{State: PeerState{Status: PeerIncoming}}
	return false, nil
}

func (h *Handler) onPeerClose(peerID string) (bool, error) {
	peer := h.peers[peerID]
	if peer == nil {
		return false, nil
	}
	switch peer.State.Status {
	case PeerWaiting, PeerConnected:
		if peer.State.Status == PeerConnected {
			h.node.ReleaseLink(peerID)
		}
		peer.State = PeerState{Status: PeerDisconnected}
		return true, nil
	}
	return false, nil
}

func (h *Handler) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil || peer.State.Status != PeerConnected {
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
	if peer == nil || peer.State.Status != PeerConnected {
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
	if peer == nil || peer.State.Status != PeerConnected {
		return false, fmt.Errorf("peer not connected")
	}
	peer.MaterializeDemurrage(ts)
	// Receiving payment: peer's debt to me increases.
	// Validate: new balance <= my credit_limit for this peer.
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

func (h *Handler) HandleResponse(peerID string, lk *link.Link, method string, payload json.RawMessage, ok bool, ts int64) {
	peer := h.peers[peerID]
	if peer == nil {
		return
	}
	switch method {
	case "connect":
		if ok {
			peer.State.Status = PeerConnected
			h.node.AcquireLink(peerID)
		}
		// !ok: stays Waiting, peer has incoming
	case "close":
		if ok {
			if peer.State.Status == PeerConnected {
				h.node.ReleaseLink(peerID)
			} else {
				h.node.RemoveLink(peerID)
			}
			delete(h.peers, peerID)
		}
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
		link.Dequeue(&peer.State.Queue)
	}
}

// ---------------------------------------------------------------------------
// Sync — NextTx
// ---------------------------------------------------------------------------

func (h *Handler) NextTx(peerID string, lk *link.Link) *link.QueuedMsg {
	if lk.State.Status != link.Connected {
		return nil
	}
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
	method := link.QueueHead(peer.State.Queue)
	if method == "" {
		return nil
	}
	switch method {
	case "connect":
		return &link.QueuedMsg{Method: "connect"}
	case "close":
		return &link.QueuedMsg{Method: "close"}
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
	if h.peers[peerID] != nil {
		return nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if err := h.node.AddLink(peerID); err != nil {
		return nil, err
	}
	h.peers[peerID] = &Peer{State: PeerState{Status: PeerWaiting, Queue: []string{"connect"}}}
	return link.Enc("ok"), nil
}

func (h *Handler) acceptPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer != nil {
		// Existing peer — must be incoming peer_request.
		if peer.State.Status != PeerIncoming {
			return nil, fmt.Errorf("peer %s is not incoming", peerID)
		}
		peer.State.Status = PeerWaiting
		link.Enqueue(&peer.State.Queue, "connect")
		h.node.Kick(peerID)
		return link.Enc("ok"), nil
	}
	// No peer yet — accept the link and create peer.
	if err := h.node.AcceptLink(peerID); err != nil {
		return nil, err
	}
	h.peers[peerID] = &Peer{State: PeerState{Status: PeerWaiting, Queue: []string{"connect"}}}
	return link.Enc("ok"), nil
}

func (h *Handler) rejectPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer != nil {
		// Existing peer — must be incoming peer_request.
		if peer.State.Status != PeerIncoming {
			return nil, fmt.Errorf("peer %s is not incoming", peerID)
		}
		link.Enqueue(&peer.State.Queue, "close")
		h.node.Kick(peerID)
		return link.Enc("ok"), nil
	}
	// No peer — reject the link directly.
	if err := h.node.RejectLink(peerID); err != nil {
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
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	switch peer.State.Status {
	case PeerConnected, PeerWaiting:
		link.Enqueue(&peer.State.Queue, "close")
		h.node.Kick(peerID)
	default:
		delete(h.peers, peerID)
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
	if peer.State.Status != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Amount < 0 {
		return nil, fmt.Errorf("credit limit must be non-negative")
	}
	if peer.PendingLimit != nil {
		return nil, fmt.Errorf("credit limit change already pending")
	}
	peer.PendingLimit = &p.Amount
	link.Enqueue(&peer.State.Queue, "set_credit_limit")
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
	if peer.State.Status != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if peer.PendingDemurrage != nil {
		return nil, fmt.Errorf("demurrage change already pending")
	}
	peer.PendingDemurrage = &p.Rate
	link.Enqueue(&peer.State.Queue, "set_demurrage")
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
	if peer.State.Status != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingPayment != nil {
		return nil, fmt.Errorf("payment already pending")
	}
	if p.Amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	// Available credit: effective balance (after demurrage) + credit_limit_in.
	available := peer.EffectiveBalance(time.Now().Unix()) + peer.CreditLimitIn
	if p.Amount > available {
		return nil, fmt.Errorf("insufficient credit")
	}
	peer.PendingPayment = &p.Amount
	link.Enqueue(&peer.State.Queue, "payment")
	h.node.Kick(peerID)
	return link.Enc("ok"), nil
}

func (h *Handler) status() (json.RawMessage, error) {
	links := h.node.Links()

	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range h.peers {
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
		if l := links[id]; l != nil {
			info["link_state"] = l.State
		}
		peers[id] = info
	}

	// Show incoming invites that have no peer yet.
	for id, l := range links {
		if h.peers[id] == nil && l.State.Status == link.Incoming {
			peers[id] = map[string]any{
				"link_state": l.State,
			}
		}
	}

	return link.Enc(map[string]any{
		"peers":         peers,
		"allow_invites": h.node.AllowInvites(),
	}), nil
}
