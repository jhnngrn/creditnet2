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
	MaxNovPerLink  = 3
	NovateInterval = 5 * time.Minute

	IN  = creditnet.IN
	OUT = creditnet.OUT
)

// NovationSide holds per-side state (IN or OUT).
type NovationSide struct {
	Peer string `json:"peer,omitempty"`
}

// Novation tracks a single three-party novation in progress.
type Novation struct {
	Amount int64           `json:"amount"`
	Side   [2]NovationSide `json:"side"`
}

// OutboxEntry is a single queued message keyed by clearing ID (novation ID).
type OutboxEntry struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Time    int64           `json:"time,omitempty"`
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
	Novations      map[string]*Novation                `json:"novations,omitempty"`
	Outbox         map[string]map[string]*OutboxEntry   `json:"outbox,omitempty"` // peer -> novationID -> entry
	name           string
	nodeID         string
	node           *mimesis.Node
	schedule       mimesis.ScheduleFunc
	trustlines     map[string]*creditnet.Trustline
	bandwidth      creditnet.BandwidthFunc
	pendingBalance creditnet.PendingBalanceFunc
}

// NewTriad matches the mimesis.ModuleFactory signature.
func NewTriad(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["trustline"].(*creditnet.Node)
	h := &Triad{
		Novations:      map[string]*Novation{},
		Outbox:         map[string]map[string]*OutboxEntry{},
		name:           name,
		nodeID:         nodeID,
		node:           node,
		schedule:       schedule,
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
	if h.Outbox == nil {
		h.Outbox = map[string]map[string]*OutboxEntry{}
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
	delete(h.Outbox, peer)
	for id, nov := range h.Novations {
		for s := 0; s < 2; s++ {
			if nov.Side[s].Peer == peer {
				nov.Side[s] = NovationSide{}
			}
		}
		if nov.Side[IN].Peer == "" && nov.Side[OUT].Peer == "" {
			h.removeNovation(id)
		}
	}
}

// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Triad) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	case "commit":
		return h.reqCommit(peer, payload)
	case "finalize":
		return h.reqFinalize(peer, payload, ts)
	case "rollback":
		return h.reqRollback(peer, payload)
	}
	return false, nil, mimesis.ErrBadMethod
}

func (h *Triad) reqCommit(peer string, raw json.RawMessage) (bool, []string, error) {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Amount <= 0 {
		return false, nil, nil
	}

	// Confirmation back to initiator — novation already exists.
	if nov := h.Novations[m.NovationID]; nov != nil {
		if nov.Side[IN].Peer != "" {
			return false, nil, nil // already confirmed
		}
		if nov.Side[OUT].Peer == peer {
			return false, nil, nil // can't confirm own novation
		}
		if h.pendingBalance(peer, IN) < nov.Amount {
			return false, nil, nil
		}
		nov.Side[IN].Peer = peer
		h.outboxPut(nov.Side[OUT].Peer, m.NovationID, "finalize", creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID}))
		return true, []string{nov.Side[OUT].Peer}, nil
	}

	// New commit — verify connectivity, bandwidth, and debt direction.
	if !h.isConnected(peer) || m.Next == "" || !h.isConnected(m.Next) {
		return false, nil, nil
	}
	if h.novLinkCount(peer) >= MaxNovPerLink || h.novLinkCount(m.Next) >= MaxNovPerLink {
		return false, nil, nil
	}
	bwIn := h.bandwidth(peer, IN)
	bwOut := h.bandwidth(m.Next, OUT)
	if bwIn < m.Amount || bwOut < m.Amount {
		return false, nil, nil
	}
	// At least one side must have debt to settle.
	if h.pendingBalance(peer, IN) <= 0 && h.pendingBalance(m.Next, OUT) <= 0 {
		return false, nil, nil
	}

	nov := &Novation{
		Amount: m.Amount,
		Side: [2]NovationSide{
			{Peer: peer},
			{Peer: m.Next},
		},
	}
	h.addNovation(m.NovationID, nov)
	h.outboxPut(m.Next, m.NovationID, "commit", creditnet.Enc(novCommitMsg{
		NovationID: m.NovationID,
		Amount:     m.Amount,
		Next:       peer,
	}))
	return true, []string{m.Next}, nil
}

func (h *Triad) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
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
	if nov.Side[IN].Peer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	p.AddBalance(nov.Amount, ts)
	nov.Side[IN] = NovationSide{}
	var kicks []string
	if nov.Side[OUT].Peer != "" {
		h.outboxPut(nov.Side[OUT].Peer, m.NovationID, "finalize", creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID}))
		kicks = []string{nov.Side[OUT].Peer}
	}
	if nov.Side[OUT].Peer == "" {
		h.removeNovation(m.NovationID)
	}
	return true, kicks, nil
}

func (h *Triad) reqRollback(peer string, raw json.RawMessage) (bool, []string, error) {
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
	if nov.Side[OUT].Peer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	if nov.Side[IN].Peer == "" {
		h.removeNovation(m.NovationID)
		return true, nil, nil
	}
	h.outboxPut(nov.Side[IN].Peer, m.NovationID, "rollback", creditnet.Enc(map[string]string{"novation_id": m.NovationID}))
	return true, []string{nov.Side[IN].Peer}, nil
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Triad) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "commit":
		return h.respCommit(peer, payload, ok)
	case "finalize":
		h.respFinalize(peer, payload, ts)
	case "rollback":
		h.respRollback(payload)
	}
	return nil
}

func (h *Triad) respCommit(peer string, raw json.RawMessage, ok bool) []string {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return nil
	}
	h.outboxRemove(peer, m.NovationID)
	if !ok {
		if nov.Side[IN].Peer == "" {
			h.removeNovation(m.NovationID)
			return nil
		}
		h.outboxPut(nov.Side[IN].Peer, m.NovationID, "rollback", creditnet.Enc(map[string]string{"novation_id": m.NovationID}))
		return []string{nov.Side[IN].Peer}
	}
	return nil
}

func (h *Triad) respFinalize(peer string, raw json.RawMessage, ts int64) {
	var m novFinalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return
	}
	h.outboxRemove(peer, m.NovationID)
	if p := h.trustlines[peer]; p != nil {
		p.AddBalance(-nov.Amount, ts)
	}
	nov.Side[OUT] = NovationSide{}
	if nov.Side[IN].Peer == "" {
		h.removeNovation(m.NovationID)
	}
}

func (h *Triad) respRollback(raw json.RawMessage) {
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

func (h *Triad) NextTx(peer string) (string, json.RawMessage, int64) {
	m := h.Outbox[peer]
	if len(m) == 0 {
		return "", nil, 0
	}
	var best *OutboxEntry
	for _, e := range m {
		if best == nil || (e.Time > 0 && (best.Time == 0 || e.Time < best.Time)) {
			best = e
		}
	}
	if best == nil {
		return "", nil, 0
	}
	return best.Method, best.Payload, best.Time
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

// Busy reports whether this module has active work with the given peer.
func (h *Triad) Busy(peer string) bool {
	if len(h.Outbox[peer]) > 0 {
		return true
	}
	for _, nov := range h.Novations {
		for s := 0; s < 2; s++ {
			if nov.Side[s].Peer == peer {
				return true
			}
		}
	}
	return false
}

func (h *Triad) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	var side int
	var msg string
	switch method {
	case "forceRollback":
		side, msg = IN, "rollback"
	case "forceFinalize":
		side, msg = OUT, "finalize"
	default:
		return nil, nil, mimesis.ErrBadMethod
	}
	var m struct {
		NovationID string `json:"novation_id"`
	}
	if json.Unmarshal(payload, &m) != nil {
		return nil, nil, fmt.Errorf("bad payload")
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return nil, nil, fmt.Errorf("unknown novation %s", m.NovationID)
	}
	if nov.Side[side].Peer == "" {
		return nil, nil, fmt.Errorf("no peer on %s side", method)
	}
	var msgPayload json.RawMessage
	if msg == "finalize" {
		msgPayload = creditnet.Enc(novFinalizeMsg{NovationID: m.NovationID})
	} else {
		msgPayload = creditnet.Enc(map[string]string{"novation_id": m.NovationID})
	}
	h.outboxPut(nov.Side[side].Peer, m.NovationID, msg, msgPayload)
	return nil, []string{nov.Side[side].Peer}, nil
}

// ---------------------------------------------------------------------------
// Background novation scheduling
// ---------------------------------------------------------------------------

func (h *Triad) scheduleNovate() {
	h.schedule(NovateInterval, h.novateAndReschedule)
}

func (h *Triad) novateAndReschedule() []string {
	kicks := h.tryNovate()
	h.scheduleNovate()
	return kicks
}

// tryNovate scans for circular debt and initiates novations as intermediary.
func (h *Triad) tryNovate() []string {
	var kicks []string
	peers := h.connectedPeers()
	for _, debtor := range peers {
		avlD := h.pendingBalance(debtor, OUT)
		if avlD <= 0 || h.novLinkCount(debtor) >= MaxNovPerLink {
			continue
		}
		for _, creditor := range peers {
			if creditor == debtor {
				continue
			}
			avlC := h.pendingBalance(creditor, IN)
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
			nov := &Novation{
				Amount: amount,
				Side: [2]NovationSide{
					{},
					{Peer: debtor},
				},
			}
			h.addNovation(novID, nov)
			h.outboxPut(debtor, novID, "commit", creditnet.Enc(novCommitMsg{
				NovationID: novID,
				Amount:     amount,
				Next:       creditor,
			}))
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

func (h *Triad) isConnected(peer string) bool {
	return h.node.ModuleAvailable(peer, h.name)
}

// addNovation stores a novation.
func (h *Triad) addNovation(id string, nov *Novation) {
	h.Novations[id] = nov
}

// removeNovation deletes the novation and its outbox entries.
func (h *Triad) removeNovation(id string) {
	if nov := h.Novations[id]; nov != nil {
		for s := 0; s < 2; s++ {
			if peer := nov.Side[s].Peer; peer != "" {
				h.outboxRemove(peer, id)
			}
		}
	}
	delete(h.Novations, id)
}

func (h *Triad) outboxPut(peer, novID, method string, payload json.RawMessage) {
	m := h.Outbox[peer]
	if m == nil {
		m = map[string]*OutboxEntry{}
		h.Outbox[peer] = m
	}
	m[novID] = &OutboxEntry{Method: method, Payload: payload, Time: time.Now().Unix()}
}

func (h *Triad) outboxRemove(peer, novID string) {
	if m := h.Outbox[peer]; m != nil {
		delete(m, novID)
		if len(m) == 0 {
			delete(h.Outbox, peer)
		}
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

func (h *Triad) novLinkCount(peer string) int {
	c := 0
	for _, n := range h.Novations {
		for s := 0; s < 2; s++ {
			if n.Side[s].Peer == peer {
				c++
			}
		}
	}
	return c
}

func randomNovationID() string {
	nonce := make([]byte, 32)
	rand.Read(nonce)
	return hex.EncodeToString(nonce)
}
