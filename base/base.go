package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// PeerState — peer lifecycle
// ---------------------------------------------------------------------------

type PeerState string

const (
	PeerConnecting PeerState = "connecting" // peer_request queued, not yet acked
	PeerWaiting    PeerState = "waiting"    // peer_request nacked, awaiting peer decision
	PeerIncoming   PeerState = "incoming"   // received peer_request, awaiting local decision
	PeerConnected  PeerState = "connected"  // bilaterally confirmed
	PeerRejecting  PeerState = "rejecting"  // peer_reject queued, not yet sent
	PeerRejected   PeerState = "rejected"   // denied by peer
)

// ---------------------------------------------------------------------------
// Peer — per-peer data owned by base
// ---------------------------------------------------------------------------

type Peer struct {
	State            PeerState `json:"state"`
	CreditLimit      uint64  `json:"credit_limit"`
	CreditLimitIn    uint64  `json:"credit_limit_in"`
	Balance          int64   `json:"balance"`
	Demurrage        uint64  `json:"demurrage"`        // my rate (promille/year)
	DemurrageIn      uint64  `json:"demurrage_in"`     // peer's rate
	DemurrageTime    int64   `json:"demurrage_time"`   // last materialized (secs)
	PendingLimit     *uint64 `json:"pending_limit,omitempty"`
	PendingPayment   *uint64 `json:"pending_payment,omitempty"`
	PendingDemurrage *uint64 `json:"pending_demurrage,omitempty"`
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
	case "peer_request":
		return h.onPeerRequest(peerID)
	case "peer_reject":
		return h.onPeerReject(peerID)
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
		switch peer.State {
		case PeerConnecting, PeerWaiting:
			peer.State = PeerConnected
			return true, nil
		default:
			return false, nil
		}
	}
	// No peer exists — create incoming.
	h.peers[peerID] = &Peer{State: PeerIncoming}
	h.node.RefLink(peerID)
	return false, nil
}

func (h *Handler) onPeerReject(peerID string) (bool, error) {
	peer := h.peers[peerID]
	if peer == nil {
		return false, nil
	}
	switch peer.State {
	case PeerConnecting, PeerWaiting:
		peer.State = PeerRejected
		return true, nil
	}
	return false, nil
}

func (h *Handler) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount uint64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil {
		return false, nil
	}
	peer.CreditLimitIn = p.Amount
	return true, nil
}

func (h *Handler) onSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Rate uint64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil {
		return false, nil
	}
	peer.MaterializeDemurrage(ts)
	peer.DemurrageIn = p.Rate
	return true, nil
}

func (h *Handler) onPayment(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Amount uint64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := h.peers[peerID]
	if peer == nil {
		return false, nil
	}
	peer.MaterializeDemurrage(ts)
	// Receiving payment: peer's debt to me increases.
	// Validate: new balance <= my credit_limit for this peer.
	newBalance := peer.Balance + int64(p.Amount)
	if newBalance > int64(peer.CreditLimit) {
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
	case "peer_request":
		if ok {
			peer.State = PeerConnected
		} else {
			peer.State = PeerWaiting
		}
	case "peer_reject":
		if ok {
			delete(h.peers, peerID)
			h.node.UnrefLink(peerID)
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
			peer.AddBalance(-int64(*peer.PendingPayment), ts)
		}
		peer.PendingPayment = nil
	}
}

// ---------------------------------------------------------------------------
// Sync — NextTx
// ---------------------------------------------------------------------------

func (h *Handler) NextTx(peerID string, lk *link.Link) *link.QueuedMsg {
	if lk.State != link.Connected {
		return nil
	}
	peer := h.peers[peerID]
	if peer == nil {
		return nil
	}
	if peer.State == PeerConnecting {
		return &link.QueuedMsg{Method: "peer_request"}
	}
	if peer.State == PeerRejecting {
		return &link.QueuedMsg{Method: "peer_reject"}
	}
	if peer.State != PeerConnected {
		return nil
	}
	if peer.PendingLimit != nil {
		return &link.QueuedMsg{
			Method:  "set_credit_limit",
			Payload: link.Enc(map[string]uint64{"amount": *peer.PendingLimit}),
		}
	}
	if peer.PendingDemurrage != nil {
		return &link.QueuedMsg{
			Method:  "set_demurrage",
			Payload: link.Enc(map[string]uint64{"rate": *peer.PendingDemurrage}),
		}
	}
	if peer.PendingPayment != nil {
		return &link.QueuedMsg{
			Method:  "payment",
			Payload: link.Enc(map[string]uint64{"amount": *peer.PendingPayment}),
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
	h.node.RefLink(peerID)
	h.peers[peerID] = &Peer{State: PeerConnecting}
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
		if peer.State != PeerIncoming {
			return nil, fmt.Errorf("peer %s is not incoming", peerID)
		}
		peer.State = PeerConnecting
		h.node.Kick(peerID)
		return link.Enc("ok"), nil
	}
	// No peer yet — accept the link and create peer.
	if err := h.node.AcceptLink(peerID); err != nil {
		return nil, err
	}
	h.node.RefLink(peerID)
	h.peers[peerID] = &Peer{State: PeerConnecting}
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
		if peer.State != PeerIncoming {
			return nil, fmt.Errorf("peer %s is not incoming", peerID)
		}
		peer.State = PeerRejecting
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
	if h.peers[peerID] == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	delete(h.peers, peerID)
	h.node.UnrefLink(peerID)
	return link.Enc("ok"), nil
}

func (h *Handler) setCreditLimit(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount uint64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingLimit != nil {
		return nil, fmt.Errorf("credit limit change already pending")
	}
	peer.PendingLimit = &p.Amount
	h.node.Kick(peerID)
	return link.Enc("ok"), nil
}

func (h *Handler) setDemurrage(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate uint64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingDemurrage != nil {
		return nil, fmt.Errorf("demurrage change already pending")
	}
	peer.PendingDemurrage = &p.Rate
	h.node.Kick(peerID)
	return link.Enc("ok"), nil
}

func (h *Handler) payment(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount uint64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State != PeerConnected {
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if peer.PendingPayment != nil {
		return nil, fmt.Errorf("payment already pending")
	}
	if p.Amount == 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	// Available credit: effective balance (after demurrage) + credit_limit_in.
	available := peer.EffectiveBalance(time.Now().Unix()) + int64(peer.CreditLimitIn)
	if int64(p.Amount) > available {
		return nil, fmt.Errorf("insufficient credit")
	}
	peer.PendingPayment = &p.Amount
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
		if h.peers[id] == nil && l.State == link.Incoming {
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
