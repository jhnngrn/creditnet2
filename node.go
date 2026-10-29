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
	CreditLimit   int64 `json:"credit_limit"`
	CreditLimitIn int64 `json:"credit_limit_in"`
	Balance       int64 `json:"balance"`
	Demurrage     int64 `json:"demurrage"`      // my rate (promille/year)
	DemurrageIn   int64 `json:"demurrage_in"`   // peer's rate
	DemurrageTime int64 `json:"demurrage_time"` // last materialized (secs)
}

// ---------------------------------------------------------------------------
// Node — the app-level node, holding trustlines and providing bandwidth
// ---------------------------------------------------------------------------

// Node is the central app-level entity. It implements link.Module for
// trustline protocol messages, and provides the Trustlines map and
// Bandwidth function that other modules depend on.
// ClosedBalance records the outstanding balance when a trustline was closed.
type ClosedBalance struct {
	Peer    string `json:"peer"`
	Balance int64  `json:"balance"`
	Time    int64  `json:"time"`
}

type Node struct {
	Trustlines     map[string]*Trustline `json:"trustlines"`
	ClosedBalances []ClosedBalance       `json:"closed_balances,omitempty"`
	Receipts       []Receipt             `json:"receipts,omitempty"`
	Queue          mimesis.SyncObject    `json:"queue"`
	name           string
	nodeID         string
	links          *mimesis.Node
	reservers      []Reserver
}

// NewNode creates a Node from persisted state.
func NewNode(name, nodeID string, links *mimesis.Node, api mimesis.ModuleAPI, state json.RawMessage) *Node {
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
	// Create or reattach the module's single SyncObject queue.
	if h.Queue.QID == 0 {
		h.Queue = api.NewSyncObject()
	} else {
		api.Reattach(&h.Queue)
	}
	return h
}

// outboxHas reports whether an outbox entry exists for this module and peer.
func (h *Node) outboxHas(peer string) bool {
	return h.Queue.HasPending(peer)
}

// AddReceipt logs a completed payment.
func (h *Node) AddReceipt(peer string, amount int64, ok bool) {
	if len(h.Receipts) >= MaxReceipts {
		h.Receipts = h.Receipts[1:]
	}
	h.Receipts = append(h.Receipts, Receipt{
		Peer:      peer,
		Amount:    amount,
		Status:    ok,
		Timestamp: time.Now().Unix(),
	})
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth.
func (h *Node) RegisterReserver(r Reserver) {
	h.reservers = append(h.reservers, r)
}

// GetTrustline returns the trustline data for a connected peer, or nil.
// The link must be bilaterally connected — waiting or incoming links
// are excluded so that account operations cannot create pending state
// on links that will never sync.
func (h *Node) GetTrustline(peer string) *Trustline {
	lk := h.links.Get(peer)
	if lk == nil || lk.State.Root != "trustline" || lk.State.Status != mimesis.Connected {
		return nil
	}
	return h.Trustlines[peer]
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

// OnConnect is called by the framework when link.connect completes bilaterally.
func (h *Node) OnConnect(peer string) {
	if h.Trustlines[peer] == nil {
		h.Trustlines[peer] = &Trustline{}
	}
}

// OnClose is called by the framework when link.close completes bilaterally.
// The link is about to be removed. Archive balance and clean up.
func (h *Node) OnClose(peer string) {
	p := h.Trustlines[peer]
	if p != nil && p.Balance != 0 {
		h.ClosedBalances = append(h.ClosedBalances, ClosedBalance{
			Peer:    peer,
			Balance: p.Balance,
			Time:    time.Now().Unix(),
		})
	}
	delete(h.Trustlines, peer)
}

// ForceRemove handles unilateral link removal. Same cleanup as OnClose.
func (h *Node) ForceRemove(peer string) {
	h.OnClose(peer)
}

// Busy reports whether this module has active bilateral state with the peer.
// The creditnet app itself is never busy — child modules hold their own Busy,
// and trustline state is archived unconditionally in OnClose.
func (h *Node) Busy(peer string) bool {
	return false
}

// ---------------------------------------------------------------------------
// link.Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Node) HandleRequest(peerID, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "set_credit_limit":
		return h.onSetCreditLimit(peerID, payload)
	case "set_demurrage":
		return h.onSetDemurrage(peerID, payload, ts)
	case "payment":
		return h.onPayment(peerID, payload, ts)
	}
	return false, ErrBadMethod
}

func (h *Node) onSetCreditLimit(peerID string, payload json.RawMessage) (bool, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, err
	}
	peer := h.Trustlines[peerID]
	if peer == nil {
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
		return false, err
	}
	peer := h.Trustlines[peerID]
	if peer == nil {
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
		return false, err
	}
	if p.Amount <= 0 {
		return false, nil
	}
	peer := h.Trustlines[peerID]
	if peer == nil {
		return false, fmt.Errorf("no trustline")
	}
	if p.Amount > h.Bandwidth(peerID, IN) {
		return false, nil
	}
	peer.AddBalance(p.Amount, ts)
	h.AddReceipt(peerID, p.Amount, true)
	return true, nil
}

// ---------------------------------------------------------------------------
// link.Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Node) HandleResponse(peerID, method string, payload json.RawMessage, ok bool, ts int64) {
	peer := h.Trustlines[peerID]
	if peer == nil {
		return
	}
	switch method {
	case "set_credit_limit":
		if ok {
			var req struct {
				Amount int64 `json:"amount"`
			}
			json.Unmarshal(payload, &req)
			peer.CreditLimit = req.Amount
		}
	case "set_demurrage":
		if ok {
			var req struct {
				Rate int64 `json:"rate"`
			}
			json.Unmarshal(payload, &req)
			peer.MaterializeDemurrage(ts)
			peer.Demurrage = req.Rate
		}
	case "payment":
		var req struct {
			Amount int64 `json:"amount"`
		}
		json.Unmarshal(payload, &req)
		if ok {
			peer.AddBalance(-req.Amount, ts)
		}
		h.AddReceipt(peerID, -req.Amount, ok)
	}
}

// ---------------------------------------------------------------------------
// link.Module — Status
// ---------------------------------------------------------------------------

func (h *Node) Status() json.RawMessage {
	now := time.Now().Unix()
	peers := make(map[string]any)
	for id, p := range h.Trustlines {
		info := map[string]any{
			"credit_limit":      p.CreditLimit,
			"credit_limit_in":   p.CreditLimitIn,
			"balance":           p.Balance,
			"effective_balance": p.EffectiveBalance(now),
			"demurrage":         p.Demurrage,
			"demurrage_in":      p.DemurrageIn,
		}
		if h.outboxHas(id) {
			info["pending"] = true
		}
		peers[id] = info
	}
	return Enc(map[string]any{"peers": peers, "receipts": h.Receipts})
}

// ---------------------------------------------------------------------------
// link.Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Node) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
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
	if lk := h.links.Get(peerID); lk != nil {
		return nil, fmt.Errorf("peer %s already exists", peerID)
	}
	if peerID == h.nodeID {
		return nil, fmt.Errorf("cannot link to self")
	}
	if _, err := h.links.Connect(peerID, "trustline"); err != nil {
		return nil, err
	}
	// Request chosen child modules on this link.
	if len(p.Modules) > 0 {
		h.links.ConnectModules(peerID, p.Modules)
	}
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
	lk := h.links.Get(peerID)
	if lk == nil {
		return nil, fmt.Errorf("no peer %s", peerID)
	}
	h.links.Close(peerID)
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
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Amount < 0 {
		return nil, fmt.Errorf("credit limit must be non-negative")
	}
	h.Queue.Send(peerID, "set_credit_limit", Enc(map[string]int64{"amount": p.Amount}))
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
		return nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	h.Queue.Send(peerID, "set_demurrage", Enc(map[string]int64{"rate": p.Rate}))
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
	peer := h.GetTrustline(peerID)
	if peer == nil {
		return nil, fmt.Errorf("peer %s has no trustline", peerID)
	}
	if p.Amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	if p.Amount > h.Bandwidth(peerID, OUT) {
		return nil, fmt.Errorf("insufficient credit")
	}
	h.Queue.Send(peerID, "payment", Enc(map[string]int64{"amount": p.Amount}))
	return Enc("ok"), nil
}
