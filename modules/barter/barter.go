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
type Barter struct {
	Pending    map[string]*PendingPayment `json:"pending,omitempty"`
	name       string
	nodeID     string
	node       *mimesis.Node
	trustlines map[string]*creditnet.Trustline
	bandwidth  creditnet.BandwidthFunc
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
		bandwidth:  tl.Bandwidth,
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
	if p.Amount > h.bandwidth(peerID, IN) {
		return false, nil, nil
	}
	peer.AddBalance(p.Amount, ts)
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
		tl.AddBalance(-pp.Amount, ts)
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
	tl := h.trustlines[peerID]
	if tl == nil {
		return nil, nil, fmt.Errorf("peer %s has no trustline", peerID)
	}
	if !h.node.ModuleAvailable(peerID, h.name) {
		return nil, nil, fmt.Errorf("barter not connected with %s", peerID)
	}
	if h.Pending[peerID] != nil {
		return nil, nil, fmt.Errorf("payment already pending")
	}
	if p.Amount <= 0 {
		return nil, nil, fmt.Errorf("amount must be positive")
	}
	if p.Amount > h.bandwidth(peerID, OUT) {
		return nil, nil, fmt.Errorf("insufficient credit")
	}
	pp := &PendingPayment{Amount: p.Amount}
	pp.Queue.Enqueue("payment")
	h.Pending[peerID] = pp
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
