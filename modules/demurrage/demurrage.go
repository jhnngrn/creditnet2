package demurrage

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"creditnet"
	"github.com/jhnngrn/mimesis"
)

// PendingDemurrage holds per-peer demurrage-change state.
type PendingDemurrage struct {
	Rate  int64          `json:"rate"`
	Queue creditnet.Queue `json:"queue"`
}

// Demurrage handles bilateral demurrage-rate negotiation.
type Demurrage struct {
	Pending    map[string]*PendingDemurrage `json:"pending,omitempty"`
	name       string
	nodeID     string
	node       *mimesis.Node
	trustlines map[string]*creditnet.Trustline
}

// NewDemurrage matches the mimesis.ModuleFactory signature.
func NewDemurrage(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["creditnet"].(*creditnet.Node)
	h := &Demurrage{
		Pending:    map[string]*PendingDemurrage{},
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
	if h.Pending == nil {
		h.Pending = map[string]*PendingDemurrage{}
	}
	return h
}

// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "set_demurrage":
		return h.reqSetDemurrage(peer, payload, ts)
	}
	return false, nil, mimesis.ErrBadMethod
}

func (h *Demurrage) reqSetDemurrage(peerID string, payload json.RawMessage, ts int64) (bool, []string, error) {
	var p struct {
		Rate int64 `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, nil, err
	}
	peer := h.getOrCreateTrustline(peerID)
	if peer == nil {
		return false, nil, fmt.Errorf("peer not connected")
	}
	if p.Rate < 0 {
		return false, nil, nil
	}
	peer.MaterializeDemurrage(ts)
	peer.DemurrageIn = p.Rate
	return true, nil, nil
}

// getOrCreateTrustline returns the trustline for a connected peer,
// creating it lazily if the link is connected but no trustline exists yet.
func (h *Demurrage) getOrCreateTrustline(peer string) *creditnet.Trustline {
	lk := h.node.Get(peer)
	if lk == nil || lk.State.Root != "creditnet" || lk.State.Status != mimesis.Connected {
		return nil
	}
	if h.trustlines[peer] == nil {
		h.trustlines[peer] = &creditnet.Trustline{}
	}
	return h.trustlines[peer]
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	if method != "set_demurrage" {
		return nil
	}
	pd := h.Pending[peer]
	if pd == nil {
		return nil
	}
	tl := h.trustlines[peer]
	if tl == nil {
		return nil
	}
	if ok {
		tl.MaterializeDemurrage(ts)
		tl.Demurrage = pd.Rate
	}
	delete(h.Pending, peer)
	return nil
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Demurrage) NextTx(peer string) (string, json.RawMessage, int64) {
	pd := h.Pending[peer]
	if pd == nil || pd.Queue.Empty() {
		return "", nil, 0
	}
	ts := pd.Queue.HeadTime()
	return "set_demurrage", creditnet.Enc(map[string]int64{"rate": pd.Rate}), ts
}

// ---------------------------------------------------------------------------
// Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Demurrage) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "set_demurrage":
		return h.setDemurrage(payload)
	}
	return nil, nil, mimesis.ErrBadMethod
}

func (h *Demurrage) setDemurrage(payload json.RawMessage) (json.RawMessage, []string, error) {
	var p struct {
		Peer string `json:"peer"`
		Rate int64  `json:"rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, fmt.Errorf("bad request")
	}
	peerID := strings.ToLower(p.Peer)
	peer := h.getOrCreateTrustline(peerID)
	if peer == nil {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}
	if p.Rate < 0 {
		return nil, nil, fmt.Errorf("demurrage rate must be non-negative")
	}
	if !h.node.ModuleAvailable(peerID, h.name) {
		return nil, nil, fmt.Errorf("demurrage not connected with %s", peerID)
	}
	if h.Pending[peerID] != nil {
		return nil, nil, fmt.Errorf("demurrage change already pending")
	}
	pd := &PendingDemurrage{Rate: p.Rate}
	pd.Queue.Enqueue("set_demurrage")
	h.Pending[peerID] = pd
	return creditnet.Enc("ok"), []string{peerID}, nil
}

// ---------------------------------------------------------------------------
// Module — Status / Busy
// ---------------------------------------------------------------------------

func (h *Demurrage) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"pending_changes": len(h.Pending)})
}

func (h *Demurrage) Busy(peer string) bool {
	return h.Pending[peer] != nil
}

func (h *Demurrage) OnConnect(peer string) {}
func (h *Demurrage) OnClose(peer string)   {}
