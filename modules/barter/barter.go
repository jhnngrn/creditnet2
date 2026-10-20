package barter

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

// PendingPayment holds per-peer payment state.
type PendingPayment struct {
	Amount int64          `json:"amount"`
	Queue  creditnet.Queue `json:"queue"`
}

// Barter handles single-hop direct payments between peers.
// Implements creditnet.Barter interface.
type Barter struct {
	Pending    map[string]*PendingPayment `json:"pending,omitempty"`
	name       string
	nodeID     string
	node       *mimesis.Node
	trustlines map[string]*creditnet.Trustline
	reservers  []creditnet.Reserver
}

// NewBarter matches the mimesis.ModuleFactory signature.
func NewBarter(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["creditnet"].(*creditnet.Node)
	h := &Barter{
		Pending:    map[string]*PendingPayment{},
		name:       name,
		nodeID:     nodeID,
		node:       node,
		trustlines: tl.Trustlines,
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("barter %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.Pending == nil {
		h.Pending = map[string]*PendingPayment{}
	}
	return h
}

// RegisterReserver adds a Reserver whose reservations are subtracted
// from available bandwidth.
func (h *Barter) RegisterReserver(r creditnet.Reserver) {
	h.reservers = append(h.reservers, r)
}

// ---------------------------------------------------------------------------
// creditnet.Barter interface
// ---------------------------------------------------------------------------

// PendingBalance returns balance minus reservations.
func (h *Barter) PendingBalance(peer string, side int) int64 {
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
func (h *Barter) Bandwidth(peer string, side int) int64 {
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
// Used by settlement protocols (triad, loops) for non-payment balance changes.
func (h *Barter) AdjustBalance(peer string, delta int64, ts int64) {
	tl := h.trustlines[peer]
	if tl == nil {
		return
	}
	tl.Balance += delta
}

// Pay validates and queues a payment to a peer.
func (h *Barter) Pay(peer string, amount int64, ts int64) (bool, error) {
	if amount <= 0 {
		return false, fmt.Errorf("amount must be positive")
	}
	tl := h.trustlines[peer]
	if tl == nil {
		return false, fmt.Errorf("peer %s has no trustline", peer)
	}
	if !h.node.ModuleAvailable(peer, h.name) {
		return false, fmt.Errorf("barter not connected with %s", peer)
	}
	if h.Pending[peer] != nil {
		return false, fmt.Errorf("payment already pending")
	}
	if amount > h.Bandwidth(peer, OUT) {
		return false, fmt.Errorf("insufficient credit")
	}
	pp := &PendingPayment{Amount: amount}
	pp.Queue.Enqueue("payment")
	h.Pending[peer] = pp
	return true, nil
}

// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Barter) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "payment":
		return h.reqPayment(peer, payload, ts)
	}
	return false, nil, mimesis.ErrBadMethod
}

func (h *Barter) reqPayment(peerID string, payload json.RawMessage, ts int64) (bool, []string, error) {
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
	peer.Balance += p.Amount
	return true, nil, nil
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Barter) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	if method != "payment" {
		return nil
	}
	pp := h.Pending[peer]
	if pp == nil {
		return nil
	}
	tl := h.trustlines[peer]
	if tl == nil {
		return nil
	}
	if ok {
		tl.Balance -= pp.Amount
	}
	delete(h.Pending, peer)
	return nil
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Barter) NextTx(peer string) (string, json.RawMessage, int64) {
	pp := h.Pending[peer]
	if pp == nil || pp.Queue.Empty() {
		return "", nil, 0
	}
	ts := pp.Queue.HeadTime()
	return "payment", creditnet.Enc(map[string]int64{"amount": pp.Amount}), ts
}

// ---------------------------------------------------------------------------
// Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Barter) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "payment":
		return h.payment(payload)
	}
	return nil, nil, mimesis.ErrBadMethod
}

func (h *Barter) payment(payload json.RawMessage) (json.RawMessage, []string, error) {
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

// ---------------------------------------------------------------------------
// Module — Status / Busy
// ---------------------------------------------------------------------------

func (h *Barter) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"pending_payments": len(h.Pending)})
}

func (h *Barter) Busy(peer string) bool {
	return h.Pending[peer] != nil
}

func (h *Barter) OnConnect(peer string) {}
func (h *Barter) OnClose(peer string)   {}
