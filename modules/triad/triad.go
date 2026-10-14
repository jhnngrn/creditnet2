package triad

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"creditnet"
	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Triad — three-party novation to eliminate circular debt
// ---------------------------------------------------------------------------

const (
	MaxNovPerLink  = 3
	NovateInterval = 5 * time.Minute
)

// Novation tracks a single three-party novation in progress.
type Novation struct {
	Amount  int64    `json:"amount"`
	Role    string   `json:"role"` // "intermediary", "debtor", "creditor"
	InPeer  string   `json:"in_peer,omitempty"`
	OutPeer string   `json:"out_peer,omitempty"`
	Queue   []string `json:"queue,omitempty"`
}

func (n *Novation) pendingPeer() string {
	if creditnet.QueueHead(n.Queue) == "rollback" {
		return n.InPeer
	}
	return n.OutPeer
}

type novCommitMsg struct {
	NovationID   string `json:"novation_id"`
	Debtor       string `json:"debtor"`
	Intermediary string `json:"intermediary"`
	Creditor     string `json:"creditor"`
	Amount       int64  `json:"amount"`
}

type novFinalizeMsg struct {
	NovationID string `json:"novation_id"`
}

// Handler implements link.Module and creditnet.Reserver for novation.
type Handler struct {
	Novations  map[string]*Novation `json:"novations,omitempty"`
	nodeID     string
	schedule   creditnet.ScheduleFunc
	trustlines map[string]*creditnet.Trustline
	bandwidth  creditnet.BandwidthFunc
}

// NewHandler matches the ModuleFactory signature.
func NewHandler(nodeID string, schedule creditnet.ScheduleFunc, trustlines map[string]*creditnet.Trustline, bandwidth creditnet.BandwidthFunc, state json.RawMessage) link.Module {
	h := &Handler{
		Novations:  map[string]*Novation{},
		nodeID:     nodeID,
		schedule:   schedule,
		trustlines: trustlines,
		bandwidth:  bandwidth,
	}
	if state != nil {
		json.Unmarshal(state, h)
	}
	if h.Novations == nil {
		h.Novations = map[string]*Novation{}
	}
	h.scheduleNovate()
	return h
}

func (h *Handler) Name() string { return "triad" }

// ---------------------------------------------------------------------------
// Reserver — reports capacity reserved by in-flight novations
// ---------------------------------------------------------------------------

func (h *Handler) Reserved(peer string) (in, out int64) {
	for _, nov := range h.Novations {
		if nov.InPeer == peer {
			in += nov.Amount
		}
		if nov.OutPeer == peer {
			out += nov.Amount
		}
	}
	return
}


// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Handler) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "commit":
		return h.reqCommit(peer, payload)
	case "finalize":
		return h.reqFinalize(peer, payload, ts)
	case "rollback":
		return h.reqRollback(peer, payload)
	}
	return false, nil, creditnet.ErrBadMethod
}

func (h *Handler) reqCommit(peer string, raw json.RawMessage) (bool, []string, error) {
	p := h.trustlines[peer]
	if p == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Amount <= 0 {
		return false, nil, nil
	}

	switch h.nodeID {
	case m.Debtor:
		// Commit from intermediary — I am debtor, forwarding to creditor.
		if peer != m.Intermediary {
			return false, nil, nil
		}
		// Check I owe intermediary enough: -eff - inReservations >= amount.
		bwIn, _ := h.bandwidth(peer)
		if bwIn-p.CreditLimit < m.Amount {
			return false, nil, nil
		}
		if h.Novations[m.NovationID] != nil || !h.isConnected(m.Creditor) {
			return false, nil, nil
		}
		if h.novLinkCount(peer) >= MaxNovPerLink || h.novLinkCount(m.Creditor) >= MaxNovPerLink {
			return false, nil, nil
		}
		h.addNovation(m.NovationID, &Novation{
			Amount: m.Amount, Role: "debtor",
			InPeer: peer, OutPeer: m.Creditor,
			Queue: []string{"commit"},
		})
		return true, []string{m.Creditor}, nil

	case m.Creditor:
		// Commit from debtor — I am creditor, forwarding to intermediary.
		if peer != m.Debtor {
			return false, nil, nil
		}
		// Check incoming bandwidth from debtor (room for finalize balance increase).
		bwIn, _ := h.bandwidth(peer)
		if bwIn < m.Amount {
			return false, nil, nil
		}
		// Check link to intermediary and outgoing debt to intermediary.
		iPeer := h.trustlines[m.Intermediary]
		if iPeer == nil {
			return false, nil, nil
		}
		_, bwOut := h.bandwidth(m.Intermediary)
		if bwOut-iPeer.CreditLimitIn < m.Amount {
			return false, nil, nil
		}
		if h.Novations[m.NovationID] != nil {
			return false, nil, nil
		}
		if h.novLinkCount(peer) >= MaxNovPerLink || h.novLinkCount(m.Intermediary) >= MaxNovPerLink {
			return false, nil, nil
		}
		h.addNovation(m.NovationID, &Novation{
			Amount: m.Amount, Role: "creditor",
			InPeer: peer, OutPeer: m.Intermediary,
			Queue: []string{"commit"},
		})
		return true, []string{m.Intermediary}, nil

	case m.Intermediary:
		// Commit confirmation from creditor — all three committed, start finalize.
		nov := h.Novations[m.NovationID]
		if nov == nil || nov.Role != "intermediary" {
			return false, nil, nil
		}
		if nov.InPeer != peer {
			return false, nil, nil
		}
		creditnet.Enqueue(&nov.Queue, "finalize")
		return true, []string{nov.OutPeer}, nil
	}
	return false, nil, nil
}

func (h *Handler) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	p := h.trustlines[peer]
	if p == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
	var m novFinalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return false, nil, fmt.Errorf("novation %s not found", m.NovationID)
	}
	if nov.InPeer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	p.AddBalance(nov.Amount, ts)
	h.unrefPeer(nov.InPeer)
	nov.InPeer = ""
	var kicks []string
	if nov.OutPeer != "" {
		creditnet.Enqueue(&nov.Queue, "finalize")
		kicks = []string{nov.OutPeer}
	}
	if nov.OutPeer == "" {
		h.removeNovation(m.NovationID)
	}
	return true, kicks, nil
}

func (h *Handler) reqRollback(peer string, raw json.RawMessage) (bool, []string, error) {
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return false, nil, fmt.Errorf("unknown novation %s", m.NovationID)
	}
	if nov.OutPeer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	if nov.Role == "intermediary" {
		h.removeNovation(m.NovationID)
		return true, nil, nil
	}
	creditnet.Enqueue(&nov.Queue, "rollback")
	return true, []string{nov.InPeer}, nil
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Handler) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "commit":
		return h.respCommit(payload, ok)
	case "finalize":
		h.respFinalize(peer, payload, ts)
	case "rollback":
		h.respRollback(payload)
	}
	return nil
}

func (h *Handler) respCommit(raw json.RawMessage, ok bool) []string {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return nil
	}
	creditnet.Dequeue(&nov.Queue)
	if !ok {
		if nov.Role == "intermediary" {
			h.removeNovation(m.NovationID)
			return nil
		}
		creditnet.Enqueue(&nov.Queue, "rollback")
		return []string{nov.InPeer}
	}
	return nil
}

func (h *Handler) respFinalize(peer string, raw json.RawMessage, ts int64) {
	var m novFinalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return
	}
	creditnet.Dequeue(&nov.Queue)
	if p := h.trustlines[peer]; p != nil {
		p.AddBalance(-nov.Amount, ts)
	}
	h.unrefPeer(nov.OutPeer)
	nov.OutPeer = ""
	if nov.InPeer == "" {
		h.removeNovation(m.NovationID)
	}
}

func (h *Handler) respRollback(raw json.RawMessage) {
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	h.removeNovation(m.NovationID)
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Handler) NextTx(peer string) *link.QueuedMsg {
	for id, nov := range h.Novations {
		if len(nov.Queue) > 0 && nov.pendingPeer() == peer {
			return h.buildMsg(id, nov)
		}
	}
	return nil
}

func (h *Handler) buildMsg(id string, nov *Novation) *link.QueuedMsg {
	switch creditnet.QueueHead(nov.Queue) {
	case "commit":
		var d, i, c string
		switch nov.Role {
		case "intermediary":
			d, i, c = nov.OutPeer, h.nodeID, nov.InPeer
		case "debtor":
			d, i, c = h.nodeID, nov.InPeer, nov.OutPeer
		case "creditor":
			d, i, c = nov.InPeer, nov.OutPeer, h.nodeID
		}
		return &link.QueuedMsg{
			Method:  "commit",
			Payload: creditnet.Enc(novCommitMsg{NovationID: id, Debtor: d, Intermediary: i, Creditor: c, Amount: nov.Amount}),
		}
	case "finalize":
		return &link.QueuedMsg{
			Method:  "finalize",
			Payload: creditnet.Enc(novFinalizeMsg{NovationID: id}),
		}
	case "rollback":
		return &link.QueuedMsg{
			Method:  "rollback",
			Payload: creditnet.Enc(map[string]string{"novation_id": id}),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Handler) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "status":
		return creditnet.Enc(map[string]any{"novations": len(h.Novations)}), nil
	}
	return nil, creditnet.ErrBadMethod
}

// ---------------------------------------------------------------------------
// Background novation scheduling
// ---------------------------------------------------------------------------

func (h *Handler) scheduleNovate() {
	h.schedule(NovateInterval, h.novateAndReschedule)
}

func (h *Handler) novateAndReschedule() []string {
	kicks := h.tryNovate()
	h.scheduleNovate()
	return kicks
}

// tryNovate scans for circular debt and initiates novations as intermediary.
func (h *Handler) tryNovate() []string {
	var kicks []string
	peers := h.connectedPeers()
	for _, debtor := range peers {
		dPeer := h.trustlines[debtor]
		if dPeer == nil {
			continue
		}
		// avlD = eff(debtor) - outReservations: how much debtor owes us, unreserved.
		_, dOut := h.bandwidth(debtor)
		avlD := dOut - dPeer.CreditLimitIn
		if avlD <= 0 || h.novLinkCount(debtor) >= MaxNovPerLink {
			continue
		}
		for _, creditor := range peers {
			if creditor == debtor {
				continue
			}
			cPeer := h.trustlines[creditor]
			if cPeer == nil {
				continue
			}
			// avlC = -eff(creditor) - inReservations: how much we owe creditor, unreserved.
			cIn, _ := h.bandwidth(creditor)
			avlC := cIn - cPeer.CreditLimit
			if avlC <= 0 || h.novLinkCount(creditor) >= MaxNovPerLink {
				continue
			}
			amount := avlD
			if avlC < amount {
				amount = avlC
			}
			if amount <= 0 {
				continue
			}
			novID := randomNovationID()
			h.addNovation(novID, &Novation{
				Amount:  amount,
				Role:    "intermediary",
				InPeer:  creditor,
				OutPeer: debtor,
				Queue:   []string{"commit"},
			})
			kicks = append(kicks, debtor)
			avlD -= amount
			if avlD <= 0 {
				break
			}
		}
	}
	return kicks
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Handler) isConnected(peer string) bool {
	t := h.trustlines[peer]
	return t != nil && t.State.Status == link.Connected && !t.State.IntentClose
}

// addNovation stores a novation and refs both peer trustlines.
func (h *Handler) addNovation(id string, nov *Novation) {
	h.Novations[id] = nov
	if t := h.trustlines[nov.InPeer]; t != nil {
		t.State.Ref()
	}
	if t := h.trustlines[nov.OutPeer]; t != nil {
		t.State.Ref()
	}
}

// removeNovation unrefs remaining peer trustlines and deletes the novation.
func (h *Handler) removeNovation(id string) {
	nov := h.Novations[id]
	if nov == nil {
		return
	}
	if nov.InPeer != "" {
		if t := h.trustlines[nov.InPeer]; t != nil {
			t.State.Unref()
		}
	}
	if nov.OutPeer != "" {
		if t := h.trustlines[nov.OutPeer]; t != nil {
			t.State.Unref()
		}
	}
	delete(h.Novations, id)
}

// unrefPeer unrefs a single peer trustline for a novation side being cleared.
func (h *Handler) unrefPeer(peer string) {
	if t := h.trustlines[peer]; t != nil {
		t.State.Unref()
	}
}

func (h *Handler) connectedPeers() []string {
	out := make([]string, 0, len(h.trustlines))
	for id := range h.trustlines {
		if h.isConnected(id) {
			out = append(out, id)
		}
	}
	return out
}

func (h *Handler) novLinkCount(peer string) int {
	c := 0
	for _, n := range h.Novations {
		if n.InPeer == peer || n.OutPeer == peer {
			c++
		}
	}
	return c
}

func randomNovationID() string {
	nonce := make([]byte, 32)
	rand.Read(nonce)
	return hex.EncodeToString(nonce)
}
