// Package demurrage implements a payment module with balance decay.
// It is a full replacement for the barter module: it handles single-hop
// payments (like barter) but materializes demurrage decay before every
// balance change. It implements creditnet.Barter so triad/loops can
// run on top of it without knowing the difference.
package demurrage

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"creditnet"
	"github.com/jhnngrn/mimesis"
)

const (
	IN  = creditnet.IN
	OUT = creditnet.OUT
)

// ---------------------------------------------------------------------------
// Per-peer demurrage state (owned by this module, NOT on Trustline)
// ---------------------------------------------------------------------------

// PeerDemurrage tracks demurrage rates and materialization timestamps
// for a single peer. This state lives here, not on the Trustline.
type PeerDemurrage struct {
	Rate     int64 `json:"rate"`      // our rate — applied to peer's debt to us
	RateIn   int64 `json:"rate_in"`   // peer's rate — applied to our debt to them
	LastTime int64 `json:"last_time"` // last materialization timestamp
}

// ---------------------------------------------------------------------------
// Pending operations
// ---------------------------------------------------------------------------

// PendingPayment holds per-peer payment state.
type PendingPayment struct {
	Amount int64          `json:"amount"`
	Queue  creditnet.Queue `json:"queue"`
}

// PendingDemurrage holds per-peer demurrage-change state.
type PendingDemurrage struct {
	Rate  int64          `json:"rate"`
	Queue creditnet.Queue `json:"queue"`
}

// ---------------------------------------------------------------------------
// Demurrage module
// ---------------------------------------------------------------------------

// Demurrage handles single-hop payments with balance decay.
// Implements creditnet.Barter interface.
type Demurrage struct {
	PendingPay map[string]*PendingPayment  `json:"pending_pay,omitempty"`
	PendingDem map[string]*PendingDemurrage `json:"pending_dem,omitempty"`
	Rates      map[string]*PeerDemurrage   `json:"rates,omitempty"`

	name       string
	nodeID     string
	node       *mimesis.Node
	trustlines map[string]*creditnet.Trustline
	reservers  []creditnet.Reserver
}

// NewDemurrage matches the mimesis.ModuleFactory signature.
func NewDemurrage(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["trustline"].(*creditnet.Node)
	h := &Demurrage{
		PendingPay: map[string]*PendingPayment{},
		PendingDem: map[string]*PendingDemurrage{},
		Rates:      map[string]*PeerDemurrage{},
		name:       name,
		nodeID:     nodeID,
		node:       node,
		trustlines: tl.Trustlines,
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("demurrage %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.PendingPay == nil {
		h.PendingPay = map[string]*PendingPayment{}
	}
	if h.PendingDem == nil {
		h.PendingDem = map[string]*PendingDemurrage{}
	}
	if h.Rates == nil {
		h.Rates = map[string]*PeerDemurrage{}
	}
	return h
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth.
func (h *Demurrage) RegisterReserver(r creditnet.Reserver) {
	h.reservers = append(h.reservers, r)
}

// ---------------------------------------------------------------------------
// Demurrage math
// ---------------------------------------------------------------------------

// mulDiv computes a * b / c without overflow for typical credit amounts.
func mulDiv(a, b, c int64) int64 {
	if c == 0 {
		return 0
	}
	return a * b / c
}

// materialize applies demurrage decay to the balance for a peer.
// Must be called before any balance change.
func (h *Demurrage) materialize(peer string, ts int64) {
	tl := h.trustlines[peer]
	if tl == nil {
		return
	}
	r := h.Rates[peer]
	if r == nil {
		return
	}
	if r.LastTime == 0 || ts <= r.LastTime {
		r.LastTime = ts
		return
	}
	dt := ts - r.LastTime
	r.LastTime = ts
	bal := tl.Balance
	if bal == 0 {
		return
	}
	// Positive balance = peer owes us → apply our rate (r.Rate).
	// Negative balance = we owe peer → apply their rate (r.RateIn).
	if bal > 0 && r.Rate > 0 {
		decay := mulDiv(bal, r.Rate*dt, 1000000)
		tl.Balance -= decay
	} else if bal < 0 && r.RateIn > 0 {
		decay := mulDiv(-bal, r.RateIn*dt, 1000000)
		tl.Balance += decay // balance is negative, decay makes it less negative
	}
}

// effectiveBalance returns balance after applying demurrage decay
// without modifying state. Used for bandwidth calculations.
func (h *Demurrage) effectiveBalance(peer string, ts int64) int64 {
	tl := h.trustlines[peer]
	if tl == nil {
		return 0
	}
	bal := tl.Balance
	r := h.Rates[peer]
	if r == nil || r.LastTime == 0 || ts <= r.LastTime {
		return bal
	}
	dt := ts - r.LastTime
	if bal > 0 && r.Rate > 0 {
		decay := mulDiv(bal, r.Rate*dt, 1000000)
		bal -= decay
	} else if bal < 0 && r.RateIn > 0 {
		decay := mulDiv(-bal, r.RateIn*dt, 1000000)
		bal += decay
	}
	return bal
}

// getOrCreateRate returns the PeerDemurrage for a peer, creating if needed.
func (h *Demurrage) getOrCreateRate(peer string) *PeerDemurrage {
	r := h.Rates[peer]
	if r == nil {
		r = &PeerDemurrage{}
		h.Rates[peer] = r
	}
	return r
}

// ---------------------------------------------------------------------------
// creditnet.Barter interface
// ---------------------------------------------------------------------------

// PendingBalance returns balance minus reservations.
func (h *Demurrage) PendingBalance(peer string, side int) int64 {
	tl := h.trustlines[peer]
	if tl == nil {
		return 0
	}
	var debt int64
	if side == OUT {
		debt = tl.Balance
	} else {
		debt = -tl.Balance
	}
	for _, r := range h.reservers {
		debt -= r.Reserved(peer, side)
	}
	return debt
}

// Bandwidth returns available capacity for a peer on a given side.
func (h *Demurrage) Bandwidth(peer string, side int) int64 {
	tl := h.trustlines[peer]
	if tl == nil {
		return 0
	}
	var limit int64
	if side == OUT {
		limit = tl.CreditLimitIn
	} else {
		limit = tl.CreditLimit
	}
	total := limit + h.PendingBalance(peer, side)
	if total < 0 {
		return 0
	}
	return total
}

// AdjustBalance directly adjusts the balance for a peer.
// Materializes demurrage before adjusting.
func (h *Demurrage) AdjustBalance(peer string, delta int64, ts int64) {
	tl := h.trustlines[peer]
	if tl == nil {
		return
	}
	h.materialize(peer, ts)
	tl.Balance += delta
}

// Pay validates and queues a payment to a peer.
func (h *Demurrage) Pay(peer string, amount int64, ts int64) (bool, error) {
	if amount <= 0 {
		return false, fmt.Errorf("amount must be positive")
	}
	tl := h.trustlines[peer]
	if tl == nil {
		return false, fmt.Errorf("peer %s has no trustline", peer)
	}
	if !h.node.ModuleAvailable(peer, h.name) {
		return false, fmt.Errorf("demurrage not connected with %s", peer)
	}
	if h.PendingPay[peer] != nil {
		return false, fmt.Errorf("payment already pending")
	}
	if amount > h.Bandwidth(peer, OUT) {
		return false, fmt.Errorf("insufficient credit")
	}
	pp := &PendingPayment{Amount: amount}
	pp.Queue.Enqueue("payment")
	h.PendingPay[peer] = pp
	return true, nil
}

// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "payment":
		return h.reqPayment(peer, payload, ts)
	case "set_demurrage":
		return h.reqSetDemurrage(peer, payload, ts)
	}
	return false, nil, mimesis.ErrBadMethod
}

func (h *Demurrage) reqPayment(peerID string, payload json.RawMessage, ts int64) (bool, []string, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil, err
	}
	if p.Amount <= 0 {
		return false, nil, nil
	}
	peer := h.trustlines[peerID]
	if peer == nil {
		return false, nil, fmt.Errorf("no trustline")
	}
	if p.Amount > h.Bandwidth(peerID, IN) {
		return false, nil, nil
	}
	h.materialize(peerID, ts)
	peer.Balance += p.Amount
	return true, nil, nil
}

func (h *Demurrage) reqSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, []string, error) {
	var p struct {
		Rate int64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil, err
	}
	if p.Rate < 0 {
		return false, nil, nil
	}
	if !h.node.ModuleAvailable(peerID, "trustline") {
		return false, nil, fmt.Errorf("peer not connected")
	}
	h.materialize(peerID, ts)
	r := h.getOrCreateRate(peerID)
	r.RateIn = p.Rate
	if r.LastTime == 0 {
		r.LastTime = ts
	}
	return true, nil, nil
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "payment":
		return h.respPayment(peer, ok, ts)
	case "set_demurrage":
		h.respSetDemurrage(peer, ok, ts)
	}
	return nil
}

func (h *Demurrage) respPayment(peer string, ok bool, ts int64) []string {
	pp := h.PendingPay[peer]
	if pp == nil {
		return nil
	}
	tl := h.trustlines[peer]
	if tl == nil {
		return nil
	}
	if ok {
		h.materialize(peer, ts)
		tl.Balance -= pp.Amount
	}
	delete(h.PendingPay, peer)
	return nil
}

func (h *Demurrage) respSetDemurrage(peer string, ok bool, ts int64) {
	pd := h.PendingDem[peer]
	if pd == nil {
		return
	}
	if ok {
		h.materialize(peer, ts)
		r := h.getOrCreateRate(peer)
		r.Rate = pd.Rate
		if r.LastTime == 0 {
			r.LastTime = ts
		}
	}
	delete(h.PendingDem, peer)
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Demurrage) NextTx(peer string) (string, json.RawMessage, int64) {
	// Payments first.
	if pp := h.PendingPay[peer]; pp != nil && !pp.Queue.Empty() {
		ts := pp.Queue.HeadTime()
		return "payment", creditnet.Enc(map[string]int64{"amount": pp.Amount}), ts
	}
	// Then demurrage rate changes.
	if pd := h.PendingDem[peer]; pd != nil && !pd.Queue.Empty() {
		ts := pd.Queue.HeadTime()
		return "set_demurrage", creditnet.Enc(map[string]int64{"rate": pd.Rate}), ts
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "payment":
		return h.accountPayment(payload)
	case "set_demurrage":
		return h.accountSetDemurrage(payload)
	}
	return nil, nil, mimesis.ErrBadMethod
}

func (h *Demurrage) accountPayment(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer   string `json:"peer"`
		Amount int64  `json:"amount"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	ok, err := h.Pay(peerID, p.Amount, 0)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("payment failed")
	}
	return creditnet.Enc("ok"), []string{peerID}, nil
}

func (h *Demurrage) accountSetDemurrage(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	if !h.node.ModuleAvailable(peerID, h.name) {
		return nil, nil, fmt.Errorf("demurrage not connected with %s", peerID)
	}
	if p.Rate < 0 {
		return nil, nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if h.PendingDem[peerID] != nil {
		return nil, nil, fmt.Errorf("demurrage change already pending")
	}
	pd := &PendingDemurrage{Rate: p.Rate}
	pd.Queue.Enqueue("set_demurrage")
	h.PendingDem[peerID] = pd
	return creditnet.Enc("ok"), []string{peerID}, nil
}

// ---------------------------------------------------------------------------
// Module — Status / Busy / lifecycle
// ---------------------------------------------------------------------------

func (h *Demurrage) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{
		"pending_payments": len(h.PendingPay),
		"pending_changes":  len(h.PendingDem),
	})
}

func (h *Demurrage) Busy(peer string) bool {
	return h.PendingPay[peer] != nil || h.PendingDem[peer] != nil
}

func (h *Demurrage) OnConnect(peer string) {}
func (h *Demurrage) OnClose(peer string)   {}
