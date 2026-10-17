package triad

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"creditnet"
	"github.com/jhnngrn/mimesis"
)

// ---------------------------------------------------------------------------
// Triad — single-hop clearing with chunked hash-chain settlement
//
// Hash chain: seed → h(seed,0) → h(seed,1) → ... → h(seed,N)
// Chunk at step k:
//   ID         = hex(chainHash(seed, k))
//   finalizePre = chainHash(seed, k-1)   →  hashEq: h(pre) == ID
//   cancelPre   = chainHash(seed, k-2)   →  doubleHashEq: h(h(pre)) == ID
//
// Finalize preimage revealed at finalize = next chunk's cancel preimage.
// ---------------------------------------------------------------------------

const (
	MaxPerLink     = 3
	NovateInterval = 5 * time.Minute
	TickRate       = 1000 // units per second
	ChunkSize      = int64(1000000)
	Nchunks        = 1024

	IN  = creditnet.IN
	OUT = creditnet.OUT
)

// ClearingSide holds per-side state for a clearing (IN or OUT).
type ClearingSide struct {
	Peer  string   `json:"peer,omitempty"`
	Time  int64    `json:"time,omitempty"`
	Queue []string `json:"queue,omitempty"`
}

// Clearing tracks a single in-flight chunk in a three-party clearing.
// Side[IN] is where commit/finalize arrives, Side[OUT] is where
// it forwards. Seed != nil identifies the initiator.
type Clearing struct {
	Amount int64          `json:"amount"`
	Side   [2]ClearingSide `json:"side"`

	// Hash chain
	Preimage  []byte `json:"preimage,omitempty"`   // finalize preimage to forward
	CancelPre []byte `json:"cancel_pre,omitempty"` // cancel preimage to forward
	Final     bool   `json:"final,omitempty"`       // last chunk in chain
	Seed      []byte `json:"seed,omitempty"`        // initiator only
	Step      int    `json:"step,omitempty"`         // initiator only, counts down
}

type commitMsg struct {
	ClearingID string `json:"clearing_id"`
	Amount     int64  `json:"amount"`
	Next       string `json:"next,omitempty"` // first hop only: who to forward to
}

type finalizeMsg struct {
	ClearingID string `json:"clearing_id"`
	Preimage   string `json:"preimage"`
	Final      bool   `json:"final,omitempty"`
}

type cancelMsg struct {
	ClearingID string `json:"clearing_id"`
	Preimage   string `json:"preimage"`
}

// Triad implements the mimesis Module interface for single-hop clearing.
type Triad struct {
	Clearings  map[string]*Clearing `json:"clearings,omitempty"`
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
		Clearings:  map[string]*Clearing{},
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
	if h.Clearings == nil {
		h.Clearings = map[string]*Clearing{}
	}
	h.scheduleNovate()
	// Re-schedule cleanup timers for persisted clearings.
	for id, cl := range h.Clearings {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer != "" && cl.Side[s].Time > 0 {
				h.scheduleCleanup(id, s, cl.Side[s].Time)
			}
		}
	}
	return h
}

// ---------------------------------------------------------------------------
// Reserver — reports capacity reserved by in-flight clearings
// ---------------------------------------------------------------------------

func (h *Triad) Reserved(peer string, side int) int64 {
	var total int64
	for _, cl := range h.Clearings {
		if cl.Side[side].Peer == peer {
			total += cl.Amount
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
		return h.reqCommit(peer, payload, ts)
	case "finalize":
		return h.reqFinalize(peer, payload, ts)
	case "cancel":
		return h.reqCancel(peer, payload, ts)
	case "rollback":
		return h.reqRollback(peer, payload)
	case "cleanup":
		return h.reqCleanup(peer, payload, ts)
	}
	return false, nil, mimesis.ErrBadMethod
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

func (h *Triad) reqCommit(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	if h.trustlines[peer] == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
	var m commitMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Amount <= 0 {
		return false, nil, nil
	}

	// Loop closure: initiator receives commit back.
	if cl := h.Clearings[m.ClearingID]; cl != nil {
		if cl.Seed == nil || cl.Side[IN].Peer != "" {
			return false, nil, nil
		}
		if h.bandwidth(peer, IN) < m.Amount {
			return false, nil, nil
		}
		cl.Side[IN].Peer = peer
		cl.Side[IN].Time = ts
		h.node.RefModule(peer, h.name)
		creditnet.Enqueue(&cl.Side[OUT].Queue, "finalize")
		return true, []string{cl.Side[OUT].Peer}, nil
	}

	// First hop: Next tells us who to forward to.
	outPeer := m.Next
	if outPeer == "" || outPeer == h.nodeID || !h.isConnected(outPeer) {
		return false, nil, nil
	}

	if h.bandwidth(peer, IN) < m.Amount {
		return false, nil, nil
	}
	if h.linkCount(peer) >= MaxPerLink || h.linkCount(outPeer) >= MaxPerLink {
		return false, nil, nil
	}

	h.addClearing(m.ClearingID, &Clearing{
		Amount: m.Amount,
		Side: [2]ClearingSide{
			{Peer: peer, Time: ts},
			{Peer: outPeer, Queue: []string{"commit"}},
		},
	})
	return true, []string{outPeer}, nil
}

func (h *Triad) respCommit(raw json.RawMessage, ok bool, ts int64) []string {
	var m commitMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return nil
	}
	creditnet.Dequeue(&cl.Side[OUT].Queue)
	if !ok {
		if cl.Seed != nil {
			// Initiator: commit rejected, remove.
			h.removeClearing(m.ClearingID)
			return nil
		}
		creditnet.Enqueue(&cl.Side[IN].Queue, "rollback")
		return []string{cl.Side[IN].Peer}
	}
	// OUT side committed — schedule cleanup for both sides.
	cl.Side[OUT].Time = ts
	for s := 0; s < 2; s++ {
		if cl.Side[s].Peer != "" && cl.Side[s].Time > 0 {
			h.scheduleCleanup(m.ClearingID, s, cl.Side[s].Time)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Finalize
// ---------------------------------------------------------------------------

func (h *Triad) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	p := h.trustlines[peer]
	if p == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
	var m finalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return false, nil, fmt.Errorf("finalize: unknown clearing %s", m.ClearingID)
	}
	if cl.Side[IN].Peer != peer {
		return false, nil, fmt.Errorf("finalize: wrong peer")
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !hashEq(pre, m.ClearingID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	// Settle IN side.
	p.AddBalance(cl.Amount, ts)

	// Continuation: create next chunk if not final.
	forwardFinal := m.Final
	if cl.Seed == nil && !m.Final {
		avl := h.bandwidth(peer, IN)
		avl += cl.Amount // account for about-to-be-freed reservation
		if avl < cl.Amount {
			forwardFinal = true
		}
	}

	var kicks []string
	if !m.Final {
		nextID := hex.EncodeToString(pre)
		if cl.Seed != nil && cl.Side[OUT].Peer != "" {
			// Initiator: create next chunk clearing.
			nextStep := cl.Step - 1
			avlIn := h.bandwidth(peer, IN)
			avlOut := h.bandwidth(cl.Side[OUT].Peer, OUT)
			isFinal := nextStep <= 2 || avlIn < 2*cl.Amount || avlOut < 2*cl.Amount
			nextCl := &Clearing{
				Amount: cl.Amount,
				Side: [2]ClearingSide{
					{Peer: peer},
					{Peer: cl.Side[OUT].Peer},
				},
				Final: isFinal,
				Seed:  cl.Seed,
				Step:  nextStep,
			}
			if nextStep >= 1 {
				nextCl.Preimage = chainHash(cl.Seed, nextStep-1)
			}
			if nextStep >= 2 {
				nextCl.CancelPre = chainHash(cl.Seed, nextStep-2)
			}
			h.Clearings[nextID] = nextCl
			h.node.RefModule(peer, h.name)
			h.node.RefModule(cl.Side[OUT].Peer, h.name)
			creditnet.Enqueue(&nextCl.Side[OUT].Queue, "finalize")
			kicks = append(kicks, cl.Side[OUT].Peer)
		} else if cl.Seed == nil && !forwardFinal {
			// Non-initiator: create next chunk with IN side only.
			nextCl := h.Clearings[nextID]
			if nextCl == nil {
				nextCl = &Clearing{Amount: cl.Amount}
				h.Clearings[nextID] = nextCl
			}
			if nextCl.Side[IN].Peer == "" {
				nextCl.Side[IN].Peer = peer
				h.node.RefModule(peer, h.name)
			}
		}
	}

	// Forward finalize to OUT.
	h.node.UnrefModule(peer, h.name)
	cl.Side[IN] = ClearingSide{}
	if cl.Side[OUT].Peer != "" && len(cl.Side[OUT].Queue) == 0 {
		creditnet.Enqueue(&cl.Side[OUT].Queue, "finalize")
		cl.Preimage = pre
		cl.Final = forwardFinal
		kicks = append(kicks, cl.Side[OUT].Peer)
	}
	if cl.Side[IN].Peer == "" && cl.Side[OUT].Peer == "" {
		h.removeClearing(m.ClearingID)
	}
	return true, kicks, nil
}

func (h *Triad) respFinalize(peer string, raw json.RawMessage, ts int64) {
	var m finalizeMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return
	}
	creditnet.Dequeue(&cl.Side[OUT].Queue)
	if p := h.trustlines[peer]; p != nil {
		p.AddBalance(-cl.Amount, ts)
	}

	// Non-initiator continuation: set up OUT side of next chunk.
	if cl.Seed == nil && cl.Preimage != nil && !cl.Final {
		nextID := hex.EncodeToString(cl.Preimage)
		nextCl := h.Clearings[nextID]
		if nextCl == nil {
			nextCl = &Clearing{Amount: cl.Amount}
			h.Clearings[nextID] = nextCl
		}
		if nextCl.Side[OUT].Peer == "" {
			nextCl.Side[OUT].Peer = peer
			h.node.RefModule(peer, h.name)
		}
	}

	h.node.UnrefModule(cl.Side[OUT].Peer, h.name)
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer == "" {
		h.removeClearing(m.ClearingID)
	}
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

func (h *Triad) reqCancel(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	if h.trustlines[peer] == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
	var m cancelMsg
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return false, nil, nil
	}
	if cl.Side[IN].Peer != peer {
		return false, nil, nil
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !doubleHashEq(pre, m.ClearingID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	// Settle ticked portion on IN side.
	ticked := tickedAmount(cl.Amount, cl.Side[IN].Time, ts)
	if ticked > 0 {
		if p := h.trustlines[peer]; p != nil {
			p.AddBalance(ticked, ts)
		}
	}

	h.node.UnrefModule(peer, h.name)
	cl.Side[IN] = ClearingSide{}
	if cl.Side[OUT].Peer != "" {
		creditnet.Enqueue(&cl.Side[OUT].Queue, "cancel")
		cl.CancelPre = pre
		return true, []string{cl.Side[OUT].Peer}, nil
	}
	h.removeClearing(m.ClearingID)
	return true, nil, nil
}

func (h *Triad) respCancel(peer string, raw json.RawMessage, ts int64) {
	var m cancelMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return
	}
	creditnet.Dequeue(&cl.Side[OUT].Queue)
	// Settle ticked portion on OUT side.
	ticked := tickedAmount(cl.Amount, cl.Side[OUT].Time, ts)
	if ticked > 0 {
		if p := h.trustlines[peer]; p != nil {
			p.AddBalance(-ticked, ts)
		}
	}
	h.node.UnrefModule(cl.Side[OUT].Peer, h.name)
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer == "" {
		h.removeClearing(m.ClearingID)
	}
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

func (h *Triad) reqRollback(peer string, raw json.RawMessage) (bool, []string, error) {
	var m struct {
		ClearingID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return false, nil, fmt.Errorf("unknown clearing %s", m.ClearingID)
	}
	if cl.Side[OUT].Peer != peer {
		return false, nil, fmt.Errorf("wrong peer")
	}
	if cl.Seed != nil {
		// Initiator: rollback terminates here.
		h.removeClearing(m.ClearingID)
		return true, nil, nil
	}
	creditnet.Enqueue(&cl.Side[IN].Queue, "rollback")
	return true, []string{cl.Side[IN].Peer}, nil
}

// ---------------------------------------------------------------------------
// Cleanup — bilateral settlement after full tick
// ---------------------------------------------------------------------------

func (h *Triad) reqCleanup(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ClearingID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return false, nil, fmt.Errorf("cleanup: unknown clearing %s", m.ClearingID)
	}

	// Find which side this peer is on.
	var side int
	if cl.Side[IN].Peer == peer {
		side = IN
	} else if cl.Side[OUT].Peer == peer {
		side = OUT
	} else {
		return false, nil, fmt.Errorf("cleanup: wrong peer")
	}

	// Verify tick has expired on this side.
	if tickedAmount(cl.Amount, cl.Side[side].Time, ts) < cl.Amount {
		return false, nil, nil // tick not expired yet
	}

	// Settle full amount.
	if p := h.trustlines[peer]; p != nil {
		if side == IN {
			p.AddBalance(cl.Amount, ts)
		} else {
			p.AddBalance(-cl.Amount, ts)
		}
	}
	h.node.UnrefModule(peer, h.name)
	cl.Side[side] = ClearingSide{}
	if cl.Side[IN].Peer == "" && cl.Side[OUT].Peer == "" {
		delete(h.Clearings, m.ClearingID)
	}
	return true, nil, nil
}

func (h *Triad) respCleanup(peer string, raw json.RawMessage, ts int64) {
	var m struct {
		ClearingID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ClearingID]
	if cl == nil {
		return
	}

	// Find which side we sent cleanup to.
	var side int
	if cl.Side[OUT].Peer == peer {
		side = OUT
	} else if cl.Side[IN].Peer == peer {
		side = IN
	} else {
		return
	}
	creditnet.Dequeue(&cl.Side[side].Queue)

	// Our side settles (mirror of reqCleanup).
	if p := h.trustlines[peer]; p != nil {
		if side == OUT {
			p.AddBalance(-cl.Amount, ts)
		} else {
			p.AddBalance(cl.Amount, ts)
		}
	}
	h.node.UnrefModule(peer, h.name)
	cl.Side[side] = ClearingSide{}
	if cl.Side[IN].Peer == "" && cl.Side[OUT].Peer == "" {
		delete(h.Clearings, m.ClearingID)
	}
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Triad) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "commit":
		return h.respCommit(payload, ok, ts)
	case "finalize":
		h.respFinalize(peer, payload, ts)
	case "cancel":
		h.respCancel(peer, payload, ts)
	case "rollback":
		h.respRollback(payload)
	case "cleanup":
		h.respCleanup(peer, payload, ts)
	}
	return nil
}

func (h *Triad) respRollback(raw json.RawMessage) {
	var m struct {
		ClearingID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	h.removeClearing(m.ClearingID)
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Triad) NextTx(peer string) (string, json.RawMessage, int64) {
	for id, cl := range h.Clearings {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer && creditnet.QueueHead(cl.Side[s].Queue) != "" {
				return h.buildMsg(id, cl, s)
			}
		}
	}
	return "", nil, 0
}

func (h *Triad) buildMsg(id string, cl *Clearing, side int) (string, json.RawMessage, int64) {
	switch creditnet.QueueHead(cl.Side[side].Queue) {
	case "commit":
		return "commit", creditnet.Enc(commitMsg{
			ClearingID: id,
			Amount:     cl.Amount,
			Next:       cl.Side[IN].Peer, // receiver should forward back to my InPeer
		}), 0
	case "finalize":
		return "finalize", creditnet.Enc(finalizeMsg{
			ClearingID: id,
			Preimage:   hex.EncodeToString(cl.Preimage),
			Final:      cl.Final,
		}), 0
	case "cancel":
		return "cancel", creditnet.Enc(cancelMsg{
			ClearingID: id,
			Preimage:   hex.EncodeToString(cl.CancelPre),
		}), 0
	case "rollback":
		return "rollback", creditnet.Enc(map[string]string{"clearing_id": id}), 0
	case "cleanup":
		return "cleanup", creditnet.Enc(map[string]string{"clearing_id": id}), 0
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Module — HandleAccount / Status
// ---------------------------------------------------------------------------

func (h *Triad) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"clearings": len(h.Clearings)})
}

func (h *Triad) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "status":
		return creditnet.Enc(map[string]any{"clearings": len(h.Clearings)}), nil, nil
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

// tryNovate scans for triangles and initiates chunked clearing.
func (h *Triad) tryNovate() []string {
	var kicks []string
	peers := h.connectedPeers()
	for _, debtor := range peers {
		dPeer := h.trustlines[debtor]
		if dPeer == nil {
			continue
		}
		dOut := h.bandwidth(debtor, OUT)
		avlD := dOut - dPeer.CreditLimitIn
		if avlD < ChunkSize || h.linkCount(debtor) >= MaxPerLink {
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
			cIn := h.bandwidth(creditor, IN)
			avlC := cIn - cPeer.CreditLimit
			if avlC < ChunkSize || h.linkCount(creditor) >= MaxPerLink {
				continue
			}

			// Generate hash chain and start first chunk.
			seed := make([]byte, 32)
			rand.Read(seed)
			step := Nchunks
			clID := hex.EncodeToString(chainHash(seed, step))

			cl := &Clearing{
				Amount: ChunkSize,
				Side: [2]ClearingSide{
					{Peer: creditor},
					{Peer: debtor, Queue: []string{"commit"}},
				},
				Seed:  seed,
				Step:  step,
				Final: step <= 2,
			}
			if step >= 1 {
				cl.Preimage = chainHash(seed, step-1)
			}
			if step >= 2 {
				cl.CancelPre = chainHash(seed, step-2)
			}

			h.addClearing(clID, cl)
			kicks = append(kicks, debtor)
			break // one triangle per creditor scan
		}
	}
	return kicks
}

// scheduleCleanup schedules bilateral cleanup for one side after its tick expires.
func (h *Triad) scheduleCleanup(id string, side int, commitTime int64) {
	cl := h.Clearings[id]
	if cl == nil || commitTime == 0 {
		return
	}
	elapsed := time.Now().Unix() - commitTime
	remaining := cl.Amount/int64(TickRate) - elapsed
	if remaining < 0 {
		remaining = 0
	}
	h.schedule(time.Duration(remaining+1)*time.Second, func() []string {
		cl := h.Clearings[id]
		if cl == nil {
			return nil
		}
		peer := cl.Side[side].Peer
		if peer == "" {
			return nil
		}
		creditnet.Enqueue(&cl.Side[side].Queue, "cleanup")
		return []string{peer}
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Triad) isConnected(peer string) bool {
	t := h.trustlines[peer]
	return t != nil && t.Connected && !t.IntentClose
}

func (h *Triad) addClearing(id string, cl *Clearing) {
	h.Clearings[id] = cl
	for s := 0; s < 2; s++ {
		if cl.Side[s].Peer != "" {
			h.node.RefModule(cl.Side[s].Peer, h.name)
		}
	}
}

func (h *Triad) removeClearing(id string) {
	if cl := h.Clearings[id]; cl != nil {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer != "" {
				h.node.UnrefModule(cl.Side[s].Peer, h.name)
			}
		}
	}
	delete(h.Clearings, id)
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

func (h *Triad) linkCount(peer string) int {
	c := 0
	for _, cl := range h.Clearings {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer {
				c++
			}
		}
	}
	return c
}

// tickedAmount returns how much of amount has settled based on elapsed time.
func tickedAmount(amount, commitTime, now int64) int64 {
	if commitTime == 0 {
		return 0
	}
	elapsed := now - commitTime
	if elapsed < 0 {
		return 0
	}
	ticked := (elapsed + 1) * int64(TickRate)
	if ticked > amount {
		ticked = amount
	}
	return ticked
}

// ---------------------------------------------------------------------------
// Hash chain
// ---------------------------------------------------------------------------

func chainHash(data []byte, n int) []byte {
	h := make([]byte, len(data))
	copy(h, data)
	for i := 0; i < n; i++ {
		s := sha256.Sum256(h)
		h = s[:]
	}
	return h
}

func hashEq(pre []byte, id string) bool {
	h := sha256.Sum256(pre)
	return hex.EncodeToString(h[:]) == id
}

func doubleHashEq(pre []byte, id string) bool {
	h1 := sha256.Sum256(pre)
	h2 := sha256.Sum256(h1[:])
	return hex.EncodeToString(h2[:]) == id
}
