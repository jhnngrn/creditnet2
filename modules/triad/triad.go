package triad

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"creditnet"
	"github.com/jhnngrn/mimesis"
)

// ---------------------------------------------------------------------------
// Triad — three-party novation to eliminate circular debt
// ---------------------------------------------------------------------------

const (
	MaxNovations = 9
	NovateInterval = 5 * time.Minute

	IN  = creditnet.IN
	OUT = creditnet.OUT
)

// Novation tracks a single three-party novation in progress.
// Each side has its own SyncObject — a 1:1 relationship with the peer.
type Novation struct {
	Amount    int64                `json:"amount"`
	Side      [2]mimesis.SyncObject `json:"side"`
	Initiator bool                 `json:"initiator,omitempty"` // true if we initiated via tryNovate
}

type novCommitMsg struct {
	NovationID string `json:"novation_id"`
	Amount     int64  `json:"amount"`
	Next       string `json:"next,omitempty"`
}

type novFinalizeMsg struct {
	NovationID string `json:"novation_id"`
}

// Triad implements the link Module interface and creditnet.Reserver for novation.
type Triad struct {
	Novations      map[string]*Novation `json:"novations,omitempty"`
	name           string
	nodeID         string
	node           *mimesis.Node
	api            mimesis.ModuleAPI
	trustlines     map[string]*creditnet.Trustline
	bandwidth      creditnet.BandwidthFunc
	pendingBalance creditnet.PendingBalanceFunc
}

// NewTriad matches the mimesis.ModuleFactory signature.
func NewTriad(name, nodeID string, node *mimesis.Node, api mimesis.ModuleAPI, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["trustline"].(*creditnet.Node)
	h := &Triad{
		Novations:      map[string]*Novation{},
		name:           name,
		nodeID:         nodeID,
		node:           node,
		api:            api,
		trustlines:     tl.Trustlines,
		bandwidth:      tl.Bandwidth,
		pendingBalance: tl.PendingBalance,
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("triad %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.Novations == nil {
		h.Novations = map[string]*Novation{}
	}
	// Reattach SyncObjects to the API after deserialization.
	for _, nov := range h.Novations {
		for s := 0; s < 2; s++ {
			if nov.Side[s].ID != 0 {
				h.api.Reattach(&nov.Side[s])
			}
		}
	}
	h.scheduleNovate()
	return h
}

// ---------------------------------------------------------------------------
// Reserver — reports capacity reserved by in-flight novations
// ---------------------------------------------------------------------------

func (h *Triad) Reserved(peer string, side int) int64 {
	var total int64
	for _, nov := range h.Novations {
		if nov.Side[side].Peer == peer {
			total += nov.Amount
		}
	}
	return total
}

// ---------------------------------------------------------------------------
// Module — lifecycle
// ---------------------------------------------------------------------------

func (h *Triad) OnConnect(peer string) {}
func (h *Triad) OnClose(peer string)   {}
func (h *Triad) ForceRemove(peer string) {
	for id, nov := range h.Novations {
		if nov.Side[OUT].Peer == peer {
			// OUT peer removed — safe to rollback: we haven't finalized
			// with OUT, so no balance was changed on that side. Send
			// rollback upstream to reverse any finalize we received.
			h.removeSide(nov, OUT)
			if nov.Side[IN].Peer != "" {
				nov.Side[IN].Send("rollback", creditnet.Enc(map[string]string{"novation_id": id}))
			} else {
				delete(h.Novations, id)
			}
			continue
		}
		if nov.Side[IN].Peer == peer {
			// IN peer removed — unsafe to auto-rollback because finalize
			// may already have propagated downstream. Clear the reference;
			// the operator can use forceRollback/forceFinalize to resolve.
			h.removeSide(nov, IN)
			if nov.Side[OUT].Peer == "" {
				delete(h.Novations, id)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Module — HandleRequest / HandleResponse
// ---------------------------------------------------------------------------

func (h *Triad) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "commit":
		return h.reqCommit(peer, payload)
	case "finalize":
		return h.reqFinalize(peer, payload, ts)
	case "rollback":
		return h.reqRollback(peer, payload)
	}
	return false, mimesis.ErrBadMethod
}

func (h *Triad) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "commit":
		h.respCommit(peer, payload, ok)
	case "finalize":
		h.respFinalize(peer, payload, ts)
	case "rollback":
		h.respRollback(payload)
	}
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

func (h *Triad) reqCommit(peer string, raw json.RawMessage) (bool, error) {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, fmt.Errorf("bad payload")
	}
	if m.Amount <= 0 {
		return false, nil
	}

	// Confirmation back to initiator — novation already exists.
	if nov := h.Novations[m.NovationID]; nov != nil {
		if nov.Side[IN].Peer != "" {
			return false, fmt.Errorf("novation %s already confirmed", m.NovationID)
		}
		if h.pendingBalance(peer, IN) < nov.Amount {
			return false, nil
		}
		nov.Side[IN] = h.api.NewSyncObject(peer)
		nov.Side[OUT].Send("finalize", creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID}))
		return true, nil
	}

	// New commit — verify connectivity and bandwidth.
	if m.Next == "" || !h.isConnected(m.Next) {
		return false, nil
	}
	if len(h.Novations) >= MaxNovations {
		return false, nil
	}
	bwIn := h.bandwidth(peer, IN)
	bwOut := h.bandwidth(m.Next, OUT)
	if bwIn < m.Amount || bwOut < m.Amount {
		return false, nil
	}

	nov := &Novation{Amount: m.Amount}
	nov.Side[IN] = h.api.NewSyncObject(peer)
	nov.Side[OUT] = h.api.NewSyncObject(m.Next)
	h.Novations[m.NovationID] = nov
	nov.Side[OUT].Send("commit", creditnet.Enc(novCommitMsg{
		NovationID: m.NovationID,
		Amount:     m.Amount,
		Next:       peer,
	}))
	return true, nil
}

func (h *Triad) respCommit(peer string, raw json.RawMessage, ok bool) {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		log.Fatalf("triad %s: respCommit for unknown novation %s", h.nodeID, m.NovationID)
	}
	if !ok {
		if nov.Side[IN].Peer == "" {
			h.closeNovation(nov)
			delete(h.Novations, m.NovationID)
			return
		}
		nov.Side[IN].Send("rollback", creditnet.Enc(map[string]string{"novation_id": m.NovationID}))
		return
	}
	// Side[OUT] already has the right peer from NewSyncObject at creation.
}

// ---------------------------------------------------------------------------
// finalize
// ---------------------------------------------------------------------------

func (h *Triad) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, error) {
	p := h.trustlines[peer]
	if p == nil {
		return false, fmt.Errorf("trustline not connected")
	}
	var m novFinalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return false, fmt.Errorf("novation %s not found", m.NovationID)
	}
	if nov.Side[IN].Peer != peer {
		return false, fmt.Errorf("wrong peer")
	}
	p.AddBalance(nov.Amount, ts)
	h.removeSide(nov, IN)
	if nov.Side[OUT].Peer != "" {
		nov.Side[OUT].Send("finalize", creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID}))
	}
	if nov.Side[OUT].Peer == "" {
		delete(h.Novations, m.NovationID)
	}
	return true, nil
}

func (h *Triad) respFinalize(peer string, raw json.RawMessage, ts int64) {
	var m novFinalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		log.Fatalf("triad %s: respFinalize for unknown novation %s", h.nodeID, m.NovationID)
	}
	p := h.trustlines[peer]
	if p == nil {
		log.Fatalf("triad %s: respFinalize for unknown trustline %s", h.nodeID, peer)
	}
	p.AddBalance(-nov.Amount, ts)
	h.removeSide(nov, OUT)
	if nov.Side[IN].Peer == "" {
		delete(h.Novations, m.NovationID)
	}
}

// ---------------------------------------------------------------------------
// rollback
// ---------------------------------------------------------------------------

func (h *Triad) reqRollback(peer string, raw json.RawMessage) (bool, error) {
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return false, fmt.Errorf("unknown novation %s", m.NovationID)
	}
	if nov.Side[OUT].Peer != peer {
		return false, fmt.Errorf("wrong peer")
	}
	h.removeSide(nov, OUT)
	if nov.Side[IN].Peer == "" {
		delete(h.Novations, m.NovationID)
		return true, nil
	}
	nov.Side[IN].Send("rollback", creditnet.Enc(map[string]string{"novation_id": m.NovationID}))
	return true, nil
}

func (h *Triad) respRollback(raw json.RawMessage) {
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	if nov := h.Novations[m.NovationID]; nov != nil {
		h.closeNovation(nov)
	}
	delete(h.Novations, m.NovationID)
}

// ---------------------------------------------------------------------------
// Module — HandleAccount / Status
// ---------------------------------------------------------------------------

func (h *Triad) Status() json.RawMessage {
	novs := make(map[string]any, len(h.Novations))
	for id, nov := range h.Novations {
		novs[id] = map[string]any{
			"amount": nov.Amount,
			"in":     nov.Side[IN].Peer,
			"out":    nov.Side[OUT].Peer,
		}
	}
	return creditnet.Enc(map[string]any{"novations": novs})
}


func (h *Triad) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	var side int
	var msg string
	switch method {
	case "forceRollback":
		side, msg = IN, "rollback"
	case "forceFinalize":
		side, msg = OUT, "finalize"
	default:
		return nil, mimesis.ErrBadMethod
	}
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(payload, &m) != nil {
		return nil, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return nil, fmt.Errorf("unknown novation %s", m.NovationID)
	}
	if nov.Side[side].Peer == "" {
		return nil, fmt.Errorf("no peer on %s side", method)
	}
	// The opposite side must be fully cleared — no peer reference and
	// no queued/in-flight messages — otherwise forceRollback could
	// conflict with a finalize already in flight (or vice versa).
	other := OUT
	if side == OUT {
		other = IN
	}
	if nov.Side[other].Peer != "" {
		return nil, fmt.Errorf("%s: opposite side still has peer %s", method, nov.Side[other].Peer)
	}
	if nov.Side[0].HasPending() || nov.Side[1].HasPending() {
		return nil, fmt.Errorf("%s: messages still queued for novation", method)
	}
	var msgPayload json.RawMessage
	if msg == "finalize" {
		msgPayload = creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID})
	} else {
		msgPayload = creditnet.Enc(map[string]string{"novation_id": m.NovationID})
	}
	nov.Side[side].Send(msg, msgPayload)
	return nil, nil
}

// ---------------------------------------------------------------------------
// Background novation scheduling
// ---------------------------------------------------------------------------

func (h *Triad) scheduleNovate() {
	h.api.Schedule(NovateInterval, h.novateAndReschedule)
}

func (h *Triad) novateAndReschedule() {
	h.tryNovate()
	h.scheduleNovate()
}

// tryNovate scans for circular debt and initiates novations as intermediary.
func (h *Triad) tryNovate() {
	peers := h.connectedPeers()
	for _, debtor := range peers {
		if len(h.Novations) >= MaxNovations {
			return
		}
		avlD := h.pendingBalance(debtor, OUT)
		if avlD <= 0 {
			continue
		}
		for _, creditor := range peers {
			if creditor == debtor {
				continue
			}
			avlC := h.pendingBalance(creditor, IN)
			if avlC <= 0 {
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
			nov := &Novation{
				Initiator: true,
				Amount:    amount,
			}
			// OUT side = debtor (we send commit downstream).
			// IN side left empty — filled when the confirmation comes back.
			nov.Side[OUT] = h.api.NewSyncObject(debtor)
			h.Novations[novID] = nov
			nov.Side[OUT].Send("commit", creditnet.Enc(novCommitMsg{
				NovationID: novID,
				Amount:     amount,
				Next:       creditor,
			}))
			avlD -= amount
			if avlD <= 0 {
				break
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Triad) isConnected(peer string) bool {
	return h.node.ModuleAvailable(peer, h.name)
}

// closeNovation closes both sides' SyncObjects.
func (h *Triad) closeNovation(nov *Novation) {
	for s := 0; s < 2; s++ {
		if nov.Side[s].ID != 0 {
			nov.Side[s].Close()
		}
	}
}

// removeSide closes a novation side's SyncObject and zeroes it.
func (h *Triad) removeSide(nov *Novation, side int) {
	if nov.Side[side].Peer != "" {
		nov.Side[side].Close()
		nov.Side[side] = mimesis.SyncObject{}
	}
}

func (h *Triad) connectedPeers() []string {
	out := make([]string, 0, len(h.trustlines))
	for id := range h.trustlines {
		if h.isConnected(id) {
			out = append(out, id)
		}
	}
	return out
}

func randomNovationID() string {
	nonce := make([]byte, 32)
	rand.Read(nonce)
	return hex.EncodeToString(nonce)
}
