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
	Peer  string         `json:"peer,omitempty"`
	Queue creditnet.Queue `json:"queue"`
}

// Novation tracks a single three-party novation in progress.
type Novation struct {
	Amount    int64           `json:"amount"`
	Initiator bool            `json:"initiator,omitempty"`
	Side      [2]NovationSide `json:"side"`
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
	Novations  map[string]*Novation `json:"novations,omitempty"`
	name       string
	nodeID     string
	node       *mimesis.Node
	schedule   mimesis.ScheduleFunc
	barter     creditnet.Barter
}

// NewTriad matches the mimesis.ModuleFactory signature.
func NewTriad(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	h := &Triad{
		Novations: map[string]*Novation{},
		name:      name,
		nodeID:    nodeID,
		node:      node,
		schedule:  schedule,
	}
	// Find the Barter implementor in the module map.
	for _, mod := range node.ModuleMap() {
		if b, ok := mod.(creditnet.Barter); ok {
			h.barter = b
			break
		}
	}
	if h.barter == nil {
		log.Fatalf("triad %s: no Barter module found", nodeID)
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("triad %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.Novations == nil {
		h.Novations = map[string]*Novation{}
	}
	// Register as reserver so bandwidth accounts for in-flight novations.
	h.barter.RegisterReserver(h)
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
		if nov.Side[IN].Peer != peer {
			return false, nil, nil
		}
		nov.Side[OUT].Queue.Enqueue("finalize")
		return true, []string{nov.Side[OUT].Peer}, nil
	}

	// New commit — verify connectivity, bandwidth, and debt direction.
	if !h.isConnected(peer) || m.Next == "" || !h.isConnected(m.Next) {
		return false, nil, nil
	}
	if h.novLinkCount(peer) >= MaxNovPerLink || h.novLinkCount(m.Next) >= MaxNovPerLink {
		return false, nil, nil
	}
	bwIn := h.barter.Bandwidth(peer, IN)
	bwOut := h.barter.Bandwidth(m.Next, OUT)
	if bwIn < m.Amount || bwOut < m.Amount {
		return false, nil, nil
	}
	// At least one side must have debt to settle.
	if h.barter.PendingBalance(peer, IN) <= 0 && h.barter.PendingBalance(m.Next, OUT) <= 0 {
		return false, nil, nil
	}

	nov := &Novation{
		Amount: m.Amount,
		Side: [2]NovationSide{
			{Peer: peer},
			{Peer: m.Next},
		},
	}
	nov.Side[OUT].Queue.Enqueue("commit")
	h.addNovation(m.NovationID, nov)
	return true, []string{m.Next}, nil
}

func (h *Triad) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
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
	h.barter.AdjustBalance(peer, nov.Amount, ts)
	nov.Side[IN] = NovationSide{}
	var kicks []string
	if nov.Side[OUT].Peer != "" {
		nov.Side[OUT].Queue.Enqueue("finalize")
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
	if nov.Initiator {
		h.removeNovation(m.NovationID)
		return true, nil, nil
	}
	nov.Side[IN].Queue.Enqueue("rollback")
	return true, []string{nov.Side[IN].Peer}, nil
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
	nov.Side[OUT].Queue.Dequeue()
	if !ok {
		if nov.Initiator {
			h.removeNovation(m.NovationID)
			return nil
		}
		nov.Side[IN].Queue.Enqueue("rollback")
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
	nov.Side[OUT].Queue.Dequeue()
	h.barter.AdjustBalance(peer, -nov.Amount, ts)
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
	var bestID string
	var bestNov *Novation
	var bestSide int
	var bestTime int64
	for id, nov := range h.Novations {
		for s := 0; s < 2; s++ {
			if nov.Side[s].Peer == peer && !nov.Side[s].Queue.Empty() {
				t := nov.Side[s].Queue.HeadTime()
				if bestNov == nil || (t > 0 && (bestTime == 0 || t < bestTime)) {
					bestID, bestNov, bestSide, bestTime = id, nov, s, t
				}
			}
		}
	}
	if bestNov == nil {
		return "", nil, 0
	}
	return h.buildMsg(bestID, bestNov, bestSide)
}

func (h *Triad) buildMsg(id string, nov *Novation, side int) (string, json.RawMessage, int64) {
	ts := nov.Side[side].Queue.HeadTime()
	other := 1 - side
	switch nov.Side[side].Queue.Head() {
	case "commit":
		return "commit", creditnet.Enc(novCommitMsg{
			NovationID: id,
			Amount:     nov.Amount,
			Next:       nov.Side[other].Peer,
		}), ts
	case "finalize":
		return "finalize", creditnet.Enc(novFinalizeMsg{NovationID: id}), ts
	case "rollback":
		return "rollback", creditnet.Enc(map[string]string{"novation_id": id}), ts
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Module — HandleAccount / Status
// ---------------------------------------------------------------------------

func (h *Triad) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"novations": len(h.Novations)})
}

// Busy reports whether this module has active work with the given peer.
func (h *Triad) Busy(peer string) bool {
	for _, nov := range h.Novations {
		for s := 0; s < 2; s++ {
			if nov.Side[s].Peer == peer {
				return true
			}
		}
	}
	return false
}

func (h *Triad) OnConnect(peer string) {}
func (h *Triad) OnClose(peer string)   {}

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
	nov.Side[side].Queue.Enqueue(msg)
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
		avlD := h.barter.PendingBalance(debtor, OUT)
		if avlD <= 0 || h.novLinkCount(debtor) >= MaxNovPerLink {
			continue
		}
		for _, creditor := range peers {
			if creditor == debtor {
				continue
			}
			avlC := h.barter.PendingBalance(creditor, IN)
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
				Amount:    amount,
				Initiator: true,
				Side: [2]NovationSide{
					{Peer: creditor},
					{Peer: debtor},
				},
			}
			nov.Side[OUT].Queue.Enqueue("commit")
			h.addNovation(novID, nov)
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

// removeNovation deletes the novation.
func (h *Triad) removeNovation(id string) {
	delete(h.Novations, id)
}

func (h *Triad) connectedPeers() []string {
	// Use the creditnet node's trustlines to find peers.
	tl := h.node.ModuleMap()["creditnet"].(*creditnet.Node)
	out := make([]string, 0, len(tl.Trustlines))
	for id := range tl.Trustlines {
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
