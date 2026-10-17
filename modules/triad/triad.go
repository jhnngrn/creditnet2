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

// Novation tracks a single three-party novation in progress.
type Novation struct {
	Amount  int64    `json:"amount"`
	Role    string   `json:"role"` // "intermediary", "debtor", "creditor"
	InPeer  string   `json:"in_peer,omitempty"`
	OutPeer string   `json:"out_peer,omitempty"`
	Queue creditnet.Queue `json:"queue"`
}

func (n *Novation) pendingPeer() string {
	if n.Queue.Head() == "rollback" {
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

// Triad implements the link Module interface and creditnet.Reserver for novation.
type Triad struct {
	Novations  map[string]*Novation `json:"novations,omitempty"`
	name       string
	nodeID     string
	node       *mimesis.Node
	schedule   mimesis.ScheduleFunc
	trustlines map[string]*creditnet.Trustline
	bandwidth  creditnet.BandwidthFunc
}

// NewTriad matches the mimesis.ModuleFactory signature.
func NewTriad(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	tl := node.ModuleMap()["trustline"].(*creditnet.Node)
	h := &Triad{
		Novations:  map[string]*Novation{},
		name:       name,
		nodeID:     nodeID,
		node:       node,
		schedule:   schedule,
		trustlines: tl.Trustlines,
		bandwidth:  tl.Bandwidth,
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("triad %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.Novations == nil {
		h.Novations = map[string]*Novation{}
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
		if side == IN && nov.InPeer == peer {
			total += nov.Amount
		}
		if side == OUT && nov.OutPeer == peer {
			total += nov.Amount
		}
	}
	return total
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
		bwIn := h.bandwidth(peer, IN)
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
			Queue: creditnet.Queue{Entries: []creditnet.QueueEntry{{"commit", 0}}},
		})
		return true, []string{m.Creditor}, nil

	case m.Creditor:
		// Commit from debtor — I am creditor, forwarding to intermediary.
		if peer != m.Debtor {
			return false, nil, nil
		}
		bwIn := h.bandwidth(peer, IN)
		if bwIn < m.Amount {
			return false, nil, nil
		}
		iPeer := h.trustlines[m.Intermediary]
		if iPeer == nil {
			return false, nil, nil
		}
		bwOut := h.bandwidth(m.Intermediary, OUT)
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
			Queue: creditnet.Queue{Entries: []creditnet.QueueEntry{{"commit", 0}}},
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
		nov.Queue.Enqueue("finalize", 0)
		return true, []string{nov.OutPeer}, nil
	}
	return false, nil, nil
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
	if nov.InPeer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	p.AddBalance(nov.Amount, ts)
	h.node.UnrefModule(nov.InPeer, h.name)
	nov.InPeer = ""
	var kicks []string
	if nov.OutPeer != "" {
		nov.Queue.Enqueue("finalize", 0)
		kicks = []string{nov.OutPeer}
	}
	if nov.OutPeer == "" {
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
	if nov.OutPeer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	if nov.Role == "intermediary" {
		h.removeNovation(m.NovationID)
		return true, nil, nil
	}
	nov.Queue.Enqueue("rollback", 0)
	return true, []string{nov.InPeer}, nil
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Triad) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
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

func (h *Triad) respCommit(raw json.RawMessage, ok bool) []string {
	var m novCommitMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	nov := h.Novations[m.NovationID]
	if nov == nil {
		return nil
	}
	nov.Queue.Dequeue()
	if !ok {
		if nov.Role == "intermediary" {
			h.removeNovation(m.NovationID)
			return nil
		}
		nov.Queue.Enqueue("rollback", 0)
		return []string{nov.InPeer}
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
	nov.Queue.Dequeue()
	if p := h.trustlines[peer]; p != nil {
		p.AddBalance(-nov.Amount, ts)
	}
	h.node.UnrefModule(nov.OutPeer, h.name)
	nov.OutPeer = ""
	if nov.InPeer == "" {
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
	var bestID string
	var bestNov *Novation
	var bestTime int64
	for id, nov := range h.Novations {
		if !nov.Queue.Empty() && nov.pendingPeer() == peer {
			t := nov.Queue.HeadTime()
			if bestNov == nil || (t > 0 && (bestTime == 0 || t < bestTime)) {
				bestID, bestNov, bestTime = id, nov, t
			}
		}
	}
	if bestNov == nil {
		return "", nil, 0
	}
	return h.buildMsg(bestID, bestNov)
}

func (h *Triad) buildMsg(id string, nov *Novation) (string, json.RawMessage, int64) {
	switch nov.Queue.Head() {
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
		return "commit", creditnet.Enc(novCommitMsg{NovationID: id, Debtor: d, Intermediary: i, Creditor: c, Amount: nov.Amount}), nov.Queue.HeadTime()
	case "finalize":
		return "finalize", creditnet.Enc(novFinalizeMsg{NovationID: id}), nov.Queue.HeadTime()
	case "rollback":
		return "rollback", creditnet.Enc(map[string]string{"novation_id": id}), nov.Queue.HeadTime()
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Module — HandleAccount / Status
// ---------------------------------------------------------------------------

func (h *Triad) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"novations": len(h.Novations)})
}

func (h *Triad) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "status":
		return creditnet.Enc(map[string]any{"novations": len(h.Novations)}), nil, nil
	}
	return nil, nil, mimesis.ErrBadMethod
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
		dPeer := h.trustlines[debtor]
		if dPeer == nil {
			continue
		}
		avlD := h.bandwidth(debtor, OUT) - dPeer.CreditLimitIn
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
			avlC := h.bandwidth(creditor, IN) - cPeer.CreditLimit
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
				Queue:   creditnet.Queue{Entries: []creditnet.QueueEntry{{"commit", 0}}},
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

func (h *Triad) isConnected(peer string) bool {
	t := h.trustlines[peer]
	return t != nil && t.Connected && !t.IntentClose
}

// addNovation stores a novation and refs the involved peers.
func (h *Triad) addNovation(id string, nov *Novation) {
	h.Novations[id] = nov
	if nov.InPeer != "" {
		h.node.RefModule(nov.InPeer, h.name)
	}
	if nov.OutPeer != "" {
		h.node.RefModule(nov.OutPeer, h.name)
	}
}

// removeNovation deletes the novation and unrefs the involved peers.
func (h *Triad) removeNovation(id string) {
	if nov := h.Novations[id]; nov != nil {
		if nov.InPeer != "" {
			h.node.UnrefModule(nov.InPeer, h.name)
		}
		if nov.OutPeer != "" {
			h.node.UnrefModule(nov.OutPeer, h.name)
		}
	}
	delete(h.Novations, id)
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
