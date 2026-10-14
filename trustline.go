package creditnet

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Trustline — per-peer credit data, owned by Node
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
// Trustline data on Node
// ---------------------------------------------------------------------------

type trustlineState struct {
	Trustlines map[string]*Trustline `json:"trustlines"`
}

// GetTrustline returns the trustline data for a connected peer, or nil.
func (n *Node) GetTrustline(peer string) *Trustline {
	p := n.trustlines[peer]
	if p == nil || p.State.Status != link.Connected {
		return nil
	}
	return p
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth. Call after creating module handlers.
func (n *Node) RegisterReserver(r Reserver) {
	n.reservers = append(n.reservers, r)
}

// Bandwidth returns available (in, out) capacity for a peer,
// accounting for effective balance, credit limits, and all reservations.
func (n *Node) Bandwidth(peer string) (in, out int64) {
	p := n.GetTrustline(peer)
	if p == nil {
		return 0, 0
	}
	eff := p.EffectiveBalance(time.Now().Unix())
	var ri, ro int64
	for _, r := range n.reservers {
		i, o := r.Reserved(peer)
		ri += i
		ro += o
	}
	in = p.CreditLimit - eff - ri
	out = p.CreditLimitIn + eff - ro
	return
}

func loadTrustlines(raw json.RawMessage) map[string]*Trustline {
	if raw != nil {
		var st trustlineState
		if json.Unmarshal(raw, &st) == nil && st.Trustlines != nil {
			return st.Trustlines
		}
	}
	return map[string]*Trustline{}
}

// trustlineLinkDeleted is called when a link to peer is removed.
func (n *Node) trustlineLinkDeleted(peer string) {
	if p := n.trustlines[peer]; p != nil {
		p.State = link.ConnectionState{}
	}
}

// refTrustlineLink increments the ref count on the link's ConnectionState.
func (n *Node) refTrustlineLink(peer string) {
	if lk := n.links.Get(peer); lk != nil {
		lk.State.Ref()
	}
}

// unrefTrustlineLink decrements the ref count on the link's ConnectionState.
func (n *Node) unrefTrustlineLink(peer string) {
	if lk := n.links.Get(peer); lk != nil {
		lk.State.UnrefClose()
	}
}

// ---------------------------------------------------------------------------
// Sync — handleTrustlineRequest
// ---------------------------------------------------------------------------

func (n *Node) handleTrustlineRequest(peerID string, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "connect":
		peer := n.trustlines[peerID]
		if peer == nil {
			peer = &Trustline{}
			n.trustlines[peerID] = peer
		}
		if peer.State.IntentConnect || peer.State.Status == link.Waiting {
			peer.State.Status = link.Connected
			peer.State.IntentConnect = false
			n.refTrustlineLink(peerID)
			return true, nil, nil
		}
		if n.links.InviteMode == link.InviteModeOpen {
			peer.State.Status = link.Connected
			n.refTrustlineLink(peerID)
			return true, nil, nil
		}
		peer.State.Status = link.Incoming
		return false, nil, nil
	case "close":
		peer := n.trustlines[peerID]
		if peer == nil {
			return true, nil, nil // already gone
		}
		n.unrefTrustlineLink(peerID)
		peer.State = link.ConnectionState{}
		return true, nil, nil
	case "set_credit_limit":
		ok, err := n.onSetCreditLimit(peerID, payload)
		return ok, nil, err
	case "set_demurrage":
		ok, err := n.onSetDemurrage(peerID, payload, ts)
		return ok, nil, err
	case "payment":
		ok, err := n.onPayment(peerID, payload, ts)
		return ok, nil, err
	}
	return false, nil, ErrBadMethod
}

func (n *Node) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := n.trustlines[peerID]
	if peer == nil || peer.State.Status != link.Connected {
		return false, fmt.Errorf("peer not connected")
	}
	if p.Amount < 0 {
		return false, nil
	}
	peer.CreditLimitIn = p.Amount
	return true, nil
}

func (n *Node) onSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Rate int64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	peer := n.trustlines[peerID]
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

func (n *Node) onPayment(peerID string, payload json.RawMessage, ts int64) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil
	}
	if p.Amount <= 0 {
		return false, nil
	}
	peer := n.trustlines[peerID]
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
// Sync — handleTrustlineResponse
// ---------------------------------------------------------------------------

func (n *Node) handleTrustlineResponse(peerID string, method string, payload json.RawMessage, ok bool, ts int64) []string {
	peer := n.trustlines[peerID]
	if peer == nil {
		return nil
	}
	switch method {
	case "connect":
		if ok {
			peer.State.Status = link.Connected
			n.refTrustlineLink(peerID)
		} else {
			peer.State.Status = link.Waiting
		}
		peer.State.IntentConnect = false
		return nil
	case "close":
		if ok {
			n.unrefTrustlineLink(peerID)
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
	if n.trustlines[peerID] != nil {
		Dequeue(&peer.Queue)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sync — trustlineNextTx
// ---------------------------------------------------------------------------

func (n *Node) trustlineNextTx(peerID string) *link.QueuedMsg {
	peer := n.trustlines[peerID]
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
// Account API (trustline namespace)
// ---------------------------------------------------------------------------

func (n *Node) handleTrustlineAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "add_peer":
		return n.addPeer(payload)
	case "accept_peer":
		return n.acceptPeer(payload)
	case "reject_peer":
		return n.rejectPeer(payload)
	case "remove_peer":
		return n.removePeer(payload)
	case "set_credit_limit":
		return n.setCreditLimit(payload)
	case "set_demurrage":
		return n.setDemurrage(payload)
	case "payment":
		return n.payment(payload)
	case "status":
		return n.trustlineStatus()
	}
	return nil, ErrBadMethod
}

func (n *Node) addPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules,omitempty"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if peer := n.trustlines[peerID]; peer != nil && peer.State.Status == link.Connected {
		return nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if lk := n.links.Get(peerID); lk != nil {
		if lk.State.IntentClose {
			return nil, fmt.Errorf("link to %s is closing", peerID)
		}
	} else {
		if err := n.AddLink(peerID); err != nil {
			return nil, err
		}
	}
	// Request trustline + chosen modules on this link.
	lk := n.links.Get(peerID)
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	lk.State.Modules["trustline"] = &link.ConnectionState{IntentConnect: true}
	for _, name := range p.Modules {
		if n.moduleMap[name] != nil {
			lk.State.Modules[name] = &link.ConnectionState{IntentConnect: true}
		}
	}
	peer := n.trustlines[peerID]
	if peer == nil {
		peer = &Trustline{}
		n.trustlines[peerID] = peer
	}
	peer.State.IntentConnect = true
	return Enc("ok"), nil
}

func (n *Node) acceptPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules,omitempty"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if err := n.AcceptLink(peerID); err != nil {
		return nil, err
	}
	// Request trustline + chosen modules on this link.
	lk := n.links.Get(peerID)
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	if lk.State.Modules["trustline"] == nil {
		lk.State.Modules["trustline"] = &link.ConnectionState{}
	}
	lk.State.Modules["trustline"].IntentConnect = true
	for _, name := range p.Modules {
		if n.moduleMap[name] != nil {
			if lk.State.Modules[name] == nil {
				lk.State.Modules[name] = &link.ConnectionState{}
			}
			lk.State.Modules[name].IntentConnect = true
		}
	}
	// Accept peer too — pre-set for mutual detection when trustline.connect arrives.
	peer := n.trustlines[peerID]
	if peer == nil {
		peer = &Trustline{}
		n.trustlines[peerID] = peer
	}
	peer.State.IntentConnect = true
	n.Kick(peerID)
	return Enc("ok"), nil
}

func (n *Node) rejectPeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	if err := n.RejectLink(strings.ToLower(p.Peer)); err != nil {
		return nil, err
	}
	return Enc("ok"), nil
}

func (n *Node) removePeer(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := n.trustlines[peerID]
	if peer == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	if peer.State.Status != link.Connected {
		// Already disconnected — clean up local peer data.
		delete(n.trustlines, peerID)
		return Enc("ok"), nil
	}
	peer.State.IntentClose = true
	n.Kick(peerID)
	return Enc("ok"), nil
}

func (n *Node) setCreditLimit(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := n.trustlines[peerID]
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
	n.Kick(peerID)
	return Enc("ok"), nil
}

func (n *Node) setDemurrage(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := n.trustlines[peerID]
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
	n.Kick(peerID)
	return Enc("ok"), nil
}

func (n *Node) payment(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := n.trustlines[peerID]
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
	n.Kick(peerID)
	return Enc("ok"), nil
}

func (n *Node) trustlineStatus() (json.RawMessage, error) {
	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range n.trustlines {
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
		if l := n.links.Get(id); l != nil {
			info["link_state"] = l.State
		}
		peers[id] = info
	}

	// Show links without a peer yet (waiting or incoming).
	for id, l := range n.links.Map() {
		if n.trustlines[id] == nil {
			peers[id] = map[string]any{
				"status":     string(l.State.Status),
				"link_state": l.State,
			}
		}
	}

	return Enc(map[string]any{
		"peers":       peers,
		"invite_mode": n.links.InviteMode,
	}), nil
}
