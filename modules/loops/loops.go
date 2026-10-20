// Package loops implements loop clearing with bidirectional probing,
// hash-chain 2PC settlement, and sync-based transport.
//
// Hash chain: seed → h1 → h2 → ... → hN
// Chunk k: id = h(N-k+1). Finalize peels 1: reveal h(N-k).
// Cancel peels 2: reveal h(N-k-1).
// Intermediary seeing finalize preimage cannot cancel (needs one layer deeper).
// By the time that layer leaks (next finalize), the chunk is already settled.
//
// Unlike creditnet1 which used fire-and-forget messages for path search,
// everything here is sync-based: search, found, and path cleanup
// all go through the bilateral sync protocol.
package loops

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

const (
	MaxPerLink      = 3
	MaxDepth        = 4
	PathTTL         = 5 * time.Minute
	SearchInterval  = 10 * time.Minute
	CancelTimeout   = 5 * time.Minute
	CleanupRetry    = 5 * time.Minute
	TickRate        = 1000
	PenaltyRate     = 10 // promille
	ChunkSize       = int64(1000000)
	Nchunks         = 1024
	GasBase         = 16
	RefractoryDelay = 1 * time.Second
)

// ---------------------------------------------------------------------------
// Types — Path (search phase, synced)
// ---------------------------------------------------------------------------

// Path tracks search state. Both sides are synced: cleanup is explicit.
//
// Peer[0] = dir-0 sender (debtor chain, toward initiator).
// Peer[1] = dir-1 sender (creditor chain).
// At initiator both start empty and are set when searches bounce back.
type Path struct {
	Amount  int64     `json:"amount"`
	Penalty int64     `json:"penalty"`
	Gas     uint64    `json:"gas"`
	Depth   uint8     `json:"depth"`
	Hops    uint8     `json:"hops"`
	Peer    [2]string `json:"peer"`
	Seed    []byte    `json:"seed,omitempty"`
	Queue   creditnet.Queue  `json:"queue"`
	Expiry  int64     `json:"expiry"`
}

func (p *Path) pendingPeer() string {
	switch p.Queue.Head() {
	case "found":
		return p.Peer[0]
	case "expire":
		if p.Peer[1] != "" {
			return p.Peer[1]
		}
		return p.Peer[0]
	}
	return ""
}

// SearchProbe is an outgoing search probe queued for a specific peer.
// Probes implement flood search: one probe per target peer, each sent
// independently through that peer's sync channel.
type SearchProbe struct {
	SearchID  string `json:"id"`
	Direction uint8  `json:"dir"`
	Amount    int64  `json:"amount"`
	Penalty   int64  `json:"penalty"`
	Gas       uint64 `json:"gas"`
	Depth     uint8  `json:"depth"`
	Hops      uint8  `json:"hops"`
}

// ---------------------------------------------------------------------------
// Types — Clearing (settlement phase, synced)
// ---------------------------------------------------------------------------

type Clearing struct {
	Amount      int64          `json:"amount"`
	PenaltyRate int64          `json:"penalty_rate"`
	Side        [2]ClearingSide `json:"side"`
	Preimage    []byte         `json:"preimage,omitempty"`
	CancelPre   []byte         `json:"cancel_pre,omitempty"`
	Final       bool           `json:"final,omitempty"`
	Seed        []byte         `json:"seed,omitempty"` // initiator only
	Step        int            `json:"step,omitempty"` // initiator only
}

type ClearingSide struct {
	Peer    string   `json:"peer,omitempty"`
	Time    int64    `json:"time,omitempty"`
	Penalty int64    `json:"penalty,omitempty"`
	Queue   creditnet.Queue `json:"queue"`
}

const (
	IN  = creditnet.IN
	OUT = creditnet.OUT
)

// ---------------------------------------------------------------------------
// Loops
// ---------------------------------------------------------------------------

type Loops struct {
	Paths     map[string]*Path          `json:"paths,omitempty"`
	Clearings map[string]*Clearing      `json:"clearings,omitempty"`
	Probes    map[string][]*SearchProbe `json:"probes,omitempty"` // peer → outgoing search probes

	name     string
	nodeID   string
	node     *mimesis.Node
	schedule mimesis.ScheduleFunc
	barter   creditnet.Barter
}

func NewLoops(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
	h := &Loops{
		Paths:     map[string]*Path{},
		Clearings: map[string]*Clearing{},
		Probes:    map[string][]*SearchProbe{},
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
		log.Fatalf("loops %s: no Barter module found", nodeID)
	}
	if state != nil {
		if err := json.Unmarshal(state, h); err != nil {
			log.Fatalf("loops %s: corrupt state: %v", nodeID, err)
		}
	}
	if h.Paths == nil {
		h.Paths = map[string]*Path{}
	}
	if h.Clearings == nil {
		h.Clearings = map[string]*Clearing{}
	}
	if h.Probes == nil {
		h.Probes = map[string][]*SearchProbe{}
	}
	// Register as reserver so bandwidth accounts for in-flight clearings.
	h.barter.RegisterReserver(h)
	h.scheduleSearch()
	// Re-schedule path expiry timers.
	now := time.Now().Unix()
	for id, p := range h.Paths {
		remaining := time.Duration(p.Expiry-now) * time.Second
		if remaining <= 0 {
			remaining = 0
		}
		pid := id
		h.schedule(remaining, func() []string {
			return h.expirePath(pid)
		})
	}
	// Re-schedule cancel timers for persisted clearings.
	for id, cl := range h.Clearings {
		if cl.Seed != nil && cl.CancelPre != nil &&
			cl.Side[IN].Peer == "" && cl.Side[OUT].Peer != "" {
			h.scheduleCancel(id)
		}
	}
	return h
}

// ---------------------------------------------------------------------------
// Reserver
// ---------------------------------------------------------------------------

func (h *Loops) Reserved(peer string, side int) int64 {
	var total int64
	for _, cl := range h.Clearings {
		if cl.Side[side].Peer == peer {
			total += cl.Amount + cl.Side[side].Penalty
		}
	}
	return total
}


// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Loops) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	// Path phase
	case "search":
		return h.reqSearch(peer, payload, ts)
	case "found":
		return h.reqFound(peer, payload)
	case "expire":
		return h.reqExpire(peer, payload, ts)
	// Clearing phase
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
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Loops) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "search":
		return h.respSearch(peer, payload, ok, ts)
	case "found":
		h.respFound(payload, ok)
	case "expire":
		h.respExpire(payload)
	case "commit":
		return h.respCommit(payload, ok, ts)
	case "finalize":
		return h.respFinalize(peer, payload, ok, ts)
	case "cancel":
		h.respCancel(peer, payload, ok, ts)
	case "rollback":
		h.respRollback(payload)
	case "cleanup":
		h.respCleanup(peer, payload, ok, ts)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Loops) NextTx(peer string) (string, json.RawMessage, int64) {
	// Probes first — flood search outgoing.
	if probes := h.Probes[peer]; len(probes) > 0 {
		probe := probes[0]
		return "search", creditnet.Enc(searchMsg{
			ID: probe.SearchID, Amount: probe.Amount, Penalty: probe.Penalty,
			Depth: probe.Depth, Gas: probe.Gas, Direction: probe.Direction, Hops: probe.Hops,
		}), 0
	}
	// Then paths — found/expire.
	for id, p := range h.Paths {
		if !p.Queue.Empty() && p.pendingPeer() == peer {
			return h.buildPathMsg(id, p)
		}
	}
	// Then clearings.
	for id, cl := range h.Clearings {
		for side := 0; side < 2; side++ {
			if cl.Side[side].Peer == peer && !cl.Side[side].Queue.Empty() {
				return h.buildClearingMsg(id, cl, side)
			}
		}
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Module — HandleAccount
// ---------------------------------------------------------------------------

func (h *Loops) Status() json.RawMessage {
	return creditnet.Enc(map[string]any{"clearings": len(h.Clearings), "paths": len(h.Paths)})
}

func (h *Loops) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "status":
		return creditnet.Enc(map[string]any{
			"paths":     len(h.Paths),
			"clearings": len(h.Clearings),
		}), nil, nil
	}
	return nil, nil, mimesis.ErrBadMethod
}

// ---------------------------------------------------------------------------
// Path — search request/response
// ---------------------------------------------------------------------------

type searchMsg struct {
	ID        string `json:"clearing_id"`
	Amount    int64  `json:"amount"`
	Penalty   int64  `json:"penalty"`
	Depth     uint8  `json:"depth"`
	Gas       uint64 `json:"gas"`
	Direction uint8  `json:"direction"` // 0=debtors, 1=creditors
	Hops      uint8  `json:"hops"`
}

func (h *Loops) reqSearch(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m searchMsg
	if json.Unmarshal(raw, &m) != nil || m.Amount <= 0 {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Direction > 1 {
		return false, nil, nil
	}
	dir := m.Direction

	p := h.Paths[m.ID]
	if p != nil {
		// Already have this path — check if loop closure.
		if p.Peer[dir] != "" {
			return false, nil, nil // duplicate
		}
		if p.Peer[1-dir] == peer {
			return false, nil, nil // same peer both dirs
		}
		p.Peer[dir] = peer
		if m.Amount < p.Amount {
			p.Amount = m.Amount
		}
		if m.Gas > 0 {
			h.barter.AdjustBalance(peer, int64(m.Gas), ts) // accept gas at closure
		}
		if p.Peer[0] != "" && p.Peer[1] != "" {
			// Both sides connected — found!
			p.Queue.Enqueue("found")
			return true, []string{p.Peer[0]}, nil
		}
		return true, nil, nil
	}

	// New path — accept and flood forward.
	if m.Gas == 0 {
		return false, nil, nil
	}
	if m.Penalty*2000 < m.Amount*PenaltyRate {
		return false, nil, nil
	}
	if h.linkSlots(peer, false) >= MaxPerLink {
		return false, nil, nil
	}

	p = &Path{Amount: m.Amount, Penalty: m.Penalty, Hops: m.Hops + 1, Depth: m.Depth, Gas: m.Gas - 1, Expiry: ts + int64(PathTTL.Seconds())}
	p.Peer[dir] = peer
	h.Paths[m.ID] = p
	h.barter.AdjustBalance(peer, int64(m.Gas), ts) // receive full gas from sender

	kicks := h.floodForward(m.ID, p, dir, peer)
	h.schedulePathExpiry(m.ID)
	return true, kicks, nil
}

func (h *Loops) respSearch(peer string, raw json.RawMessage, ok bool, ts int64) []string {
	var m searchMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	// Consume the probe.
	probes := h.Probes[peer]
	if len(probes) > 0 {
		h.Probes[peer] = probes[1:]
		if len(h.Probes[peer]) == 0 {
			delete(h.Probes, peer)
		}
	}
	if ok {
		h.barter.AdjustBalance(peer, -int64(m.Gas), ts) // sender pays gas
	}
	return nil
}

// floodForward creates probes for all eligible peers in the given direction,
// dividing gas evenly among them (matching old creditnet's floodSearch).
func (h *Loops) floodForward(searchID string, p *Path, dir uint8, exclude string) []string {
	var eligible []string
	tl := h.node.ModuleMap()["creditnet"].(*creditnet.Node)
	for peerID := range tl.Trustlines {
		if peerID == exclude || !h.isConnected(peerID) {
			continue
		}
		if h.linkSlots(peerID, false) >= MaxPerLink {
			continue
		}
		if dir == 0 {
			if h.barter.Bandwidth(peerID, OUT) > 0 {
				eligible = append(eligible, peerID)
			}
		} else {
			if h.barter.Bandwidth(peerID, IN) > 0 {
				eligible = append(eligible, peerID)
			}
		}
	}
	if len(eligible) == 0 || p.Gas == 0 {
		return nil
	}
	gas := p.Gas
	share := gas / uint64(len(eligible))
	if share < 1 {
		share = 1
	}
	var kicks []string
	for _, peerID := range eligible {
		if gas < 1 {
			break
		}
		g := share
		if g > gas {
			g = gas
		}
		h.addProbe(peerID, &SearchProbe{
			SearchID:  searchID,
			Direction: dir,
			Amount:    p.Amount,
			Penalty:   p.Penalty,
			Gas:       g,
			Depth:     p.Depth,
			Hops:      p.Hops,
		})
		kicks = append(kicks, peerID)
		gas -= g
	}
	return kicks
}

func (h *Loops) addProbe(peer string, probe *SearchProbe) {
	h.Probes[peer] = append(h.Probes[peer], probe)
}

// ---------------------------------------------------------------------------
// Path — found request/response
// ---------------------------------------------------------------------------

func (h *Loops) reqFound(peer string, raw json.RawMessage) (bool, []string, error) {
	var m struct {
		ID   string `json:"clearing_id"`
		Hops uint8  `json:"hops"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	p := h.Paths[m.ID]
	if p == nil {
		return false, nil, nil
	}
	if p.Peer[0] == peer {
		return false, nil, nil // reject from the direction we'd forward to
	}
	p.Hops = m.Hops + 1

	if p.Seed != nil {
		// Initiator — start loop settlement.
		return true, h.startLoop(m.ID), nil
	}
	// Intermediate — forward found toward initiator (Peer[0]).
	if p.Peer[0] != "" {
		p.Queue.Enqueue("found")
		return true, []string{p.Peer[0]}, nil
	}
	return true, nil, nil
}

func (h *Loops) respFound(raw json.RawMessage, ok bool) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	p := h.Paths[m.ID]
	if p == nil {
		return
	}
	p.Queue.Dequeue()
}



// ---------------------------------------------------------------------------
// Path — cleanup
// ---------------------------------------------------------------------------

func (h *Loops) reqExpire(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	p := h.Paths[m.ID]
	if p == nil {
		return true, nil, nil // already gone
	}
	if ts < p.Expiry {
		return false, nil, nil
	}
	// Accept cleanup from either side.
	if p.Peer[0] != peer && p.Peer[1] != peer {
		return false, nil, nil
	}
	// Forward cleanup to the other side.
	var kicks []string
	if p.Peer[0] == peer {
		p.Peer[0] = ""
		if p.Peer[1] != "" {
			p.Queue.Enqueue("expire")
			kicks = []string{p.Peer[1]}
		}
	} else {
		p.Peer[1] = ""
		if p.Peer[0] != "" {
			p.Queue.Enqueue("expire")
			kicks = []string{p.Peer[0]}
		}
	}
	if p.Peer[0] == "" && p.Peer[1] == "" {
		delete(h.Paths, m.ID)
	}
	return true, kicks, nil
}

func (h *Loops) respExpire(raw json.RawMessage) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	p := h.Paths[m.ID]
	if p == nil {
		return
	}
	p.Queue.Dequeue()
	// Remove the side we cleaned up.
	head := p.Queue.Head()
	if head == "" {
		// Cleanup was the last thing — now remove our side too.
		delete(h.Paths, m.ID)
	}
}

func (h *Loops) expirePath(id string) []string {
	p := h.Paths[id]
	if p == nil {
		return nil
	}
	// Send cleanup — Peer[1] first if set, else Peer[0].
	var kicks []string
	if p.Peer[1] != "" {
		p.Queue.Enqueue("expire")
		kicks = append(kicks, p.Peer[1])
	} else if p.Peer[0] != "" {
		p.Queue.Enqueue("expire")
		kicks = append(kicks, p.Peer[0])
	}
	if p.Peer[0] == "" && p.Peer[1] == "" {
		delete(h.Paths, id)
	}
	return kicks
}

func (h *Loops) schedulePathExpiry(id string) {
	h.schedule(PathTTL, func() []string {
		return h.expirePath(id)
	})
}

// ---------------------------------------------------------------------------
// Path — message builders
// ---------------------------------------------------------------------------

func (h *Loops) buildPathMsg(id string, p *Path) (string, json.RawMessage, int64) {
	switch p.Queue.Head() {
	case "found":
		return "found", creditnet.Enc(map[string]any{"clearing_id": id, "hops": p.Hops}), 0
	case "expire":
		return "expire", creditnet.Enc(map[string]string{"clearing_id": id}), 0
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Clearing — commit
// ---------------------------------------------------------------------------

func (h *Loops) startClearing(id string, amount int64, outPeer string, preimage, cancelPre, seed []byte, step int) {
	pen := amount * PenaltyRate / 1000
	h.Clearings[id] = &Clearing{
		Amount: amount, PenaltyRate: pen,
		Preimage: preimage, CancelPre: cancelPre,
		Final: step <= 2,
		Seed: seed, Step: step,
		Side: [2]ClearingSide{
			{}, // IN — filled when commit comes back
			{Peer: outPeer, Penalty: pen, Queue: creditnet.Queue{Entries: []creditnet.QueueEntry{{"commit", 0}}}},
		},
	}
}

func (h *Loops) reqCommit(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID      string `json:"clearing_id"`
		Amount  int64  `json:"amount"`
		Penalty int64  `json:"penalty"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Amount <= 0 {
		return false, nil, nil
	}

	// Loop closure: initiator receives commit back.
	if cl := h.Clearings[m.ID]; cl != nil {
		if cl.Side[IN].Peer != "" {
			return false, nil, nil
		}
		p := h.Paths[m.ID]
		var hops uint8
		if p != nil {
			hops = p.Hops
			delete(h.Paths, m.ID)
		}
		inPen := m.Penalty * int64(hops+1)
		if h.barter.Bandwidth(peer, IN) < m.Amount+inPen {
			return false, nil, nil
		}
		cl.Side[IN] = ClearingSide{Peer: peer, Time: ts, Penalty: inPen}
			h.scheduleCleanup(m.ID, IN)
		cl.Side[OUT].Queue.Enqueue("finalize")
		return true, []string{cl.Side[OUT].Peer}, nil
	}

	// Intermediate: look up path.
	p := h.Paths[m.ID]
	if p == nil {
		return false, nil, nil
	}
	hops := p.Hops
	outPeer := p.Peer[0] // toward initiator
	delete(h.Paths, m.ID)

	inPen := m.Penalty * int64(hops+1)
	if h.barter.Bandwidth(peer, IN) < m.Amount+inPen {
		return false, nil, nil
	}

	outPen := m.Penalty * int64(hops)
	cl := &Clearing{
		Amount: m.Amount, PenaltyRate: m.Penalty,
		Side: [2]ClearingSide{
			{Peer: peer, Time: ts, Penalty: inPen},
			{},
		},
	}
	h.Clearings[m.ID] = cl
	h.scheduleCleanup(m.ID, IN)

	if outPeer != "" {
		cl.Side[OUT] = ClearingSide{Peer: outPeer, Penalty: outPen, Queue: creditnet.Queue{Entries: []creditnet.QueueEntry{{"commit", 0}}}}
			return true, []string{outPeer}, nil
	}
	return true, nil, nil
}

func (h *Loops) respCommit(raw json.RawMessage, ok bool, ts int64) []string {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return nil
	}
	cl.Side[OUT].Queue.Dequeue()
	if ok {
		cl.Side[OUT].Time = ts
		h.scheduleCleanup(m.ID, OUT)
		if cl.Seed != nil && cl.CancelPre != nil {
			h.scheduleCancel(m.ID)
		}
		return nil
	}
	// Rejected — rollback IN side.
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer != "" {
		cl.Side[IN].Queue.Enqueue("rollback")
		return []string{cl.Side[IN].Peer}
	}
	h.removeClearing(m.ID)
	return nil
}

// ---------------------------------------------------------------------------
// Clearing — finalize
// ---------------------------------------------------------------------------

func (h *Loops) reqFinalize(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID       string `json:"clearing_id"`
		Preimage string `json:"preimage"`
		Final    bool   `json:"final"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return false, nil, fmt.Errorf("finalize: unknown clearing %s", m.ID)
	}
	if cl.Side[IN].Peer != peer {
		return false, nil, fmt.Errorf("finalize: wrong peer %s", peer)
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !hashEq(pre, m.ID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	// Settle IN side.
	penConsumed := penaltyTick(cl.Amount, cl.Side[IN].Penalty, cl.Side[IN].Time, ts)
	h.barter.AdjustBalance(peer, cl.Amount+penConsumed, ts)
	outPeer := cl.Side[OUT].Peer
	var kicks []string

	// Intermediary: check if there's enough IN debt for another chunk.
	// After settlement, compensate for this clearing's about-to-be-freed reservation.
	// If not enough, skip continuation and forward final=true.
	forwardFinal := m.Final
	if cl.Seed == nil && !m.Final {
		avl := h.barter.Bandwidth(peer, IN)
		avl += cl.Amount + cl.Side[IN].Penalty
		if avl < cl.Amount {
			forwardFinal = true
		}
	}

	// Continuation: create next chunk clearing.
	if !m.Final {
		nextID := hex.EncodeToString(pre)
		if cl.Seed != nil && outPeer != "" {
			// Initiator: create complete clearing and drive next chunk.
			// In sync transport reqFinalize always arrives before respFinalize,
			// so the initiator handles all continuation here.
			nextStep := cl.Step - 1
			avlIn := h.barter.Bandwidth(peer, IN)
			avlOut := h.barter.Bandwidth(outPeer, OUT)
			isFinal := nextStep <= 2 || avlIn < 2*cl.Amount || avlOut < 2*cl.Amount
			nextCl := &Clearing{
				Amount:      cl.Amount,
				PenaltyRate: cl.PenaltyRate,
				Seed:        cl.Seed,
				Step:        nextStep,
				Final:       isFinal,
				Side: [2]ClearingSide{
					{Peer: peer, Time: ts, Penalty: cl.Side[IN].Penalty},
					{Peer: outPeer, Penalty: cl.Side[OUT].Penalty},
				},
			}
			if nextStep >= 1 {
				nextCl.Preimage = chainHash(cl.Seed, nextStep-1)
			}
			if nextStep >= 2 {
				nextCl.CancelPre = chainHash(cl.Seed, nextStep-2)
			}
			h.Clearings[nextID] = nextCl
			h.scheduleCleanup(nextID, IN)
			h.scheduleCleanup(nextID, OUT)
			if nextCl.CancelPre != nil {
				h.scheduleCancel(nextID)
			}
			nextCl.Side[OUT].Queue.Enqueue("finalize")
			kicks = append(kicks, outPeer)
		} else if cl.Seed == nil && !forwardFinal {
			// Intermediary: create with IN side only.
			// OUT side set by respFinalize when OUT peer accepts.
			nextCl := h.Clearings[nextID]
			if nextCl == nil {
				nextCl = &Clearing{
					Amount: cl.Amount, PenaltyRate: cl.PenaltyRate,
				}
				h.Clearings[nextID] = nextCl
			}
			if nextCl.Side[IN].Peer == "" {
				nextCl.Side[IN] = ClearingSide{Peer: peer, Time: ts, Penalty: cl.Side[IN].Penalty}
				h.scheduleCleanup(nextID, IN)
			}
		}
	}

	// Forward finalize to OUT.
	cl.Side[IN] = ClearingSide{}
	if outPeer != "" && cl.Side[OUT].Queue.Empty() {
		cl.Side[OUT].Queue.Enqueue("finalize")
		cl.Preimage = pre
		cl.Final = forwardFinal
		kicks = []string{outPeer}
	}
	if cl.Side[IN].Peer == "" && cl.Side[OUT].Peer == "" {
		h.removeClearing(m.ID)
	}
	return !forwardFinal || m.Final, kicks, nil
}

func (h *Loops) respFinalize(peer string, raw json.RawMessage, ok bool, ts int64) []string {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return nil
	}
	outPeer := cl.Side[OUT].Peer
	cl.Side[OUT].Queue.Dequeue()
	if ok {
		penConsumed := penaltyTick(cl.Amount, cl.Side[OUT].Penalty, cl.Side[OUT].Time, ts)
		h.barter.AdjustBalance(peer, -(cl.Amount + penConsumed), ts)

		// Intermediary continuation: set up OUT half of next chunk.
		// Initiator continuation is handled entirely in reqFinalize
		// (which always arrives first in sync transport).
		if cl.Seed == nil && cl.Preimage != nil && !cl.Final {
			nextID := hex.EncodeToString(cl.Preimage)
			nextCl := h.Clearings[nextID]
			if nextCl == nil {
				nextCl = &Clearing{
					Amount: cl.Amount, PenaltyRate: cl.PenaltyRate,
				}
				h.Clearings[nextID] = nextCl
			}
			if nextCl.Side[OUT].Peer == "" {
				nextCl.Side[OUT] = ClearingSide{Peer: outPeer, Penalty: cl.Side[OUT].Penalty}
				h.scheduleCleanup(nextID, OUT)
			}
		}

		cl.Side[OUT] = ClearingSide{}
		if cl.Side[IN].Peer == "" {
			h.removeClearing(m.ID)
		}
		return nil
	}
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer != "" {
		cl.Side[IN].Queue.Enqueue("rollback")
	} else {
		h.removeClearing(m.ID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Clearing — cancel
// ---------------------------------------------------------------------------

func (h *Loops) reqCancel(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID       string `json:"clearing_id"`
		Preimage string `json:"preimage"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return false, nil, nil
	}
	if cl.Side[IN].Peer != peer {
		return false, nil, nil
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !doubleHashEq(pre, m.ID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	penConsumed := penaltyTick(cl.Amount, cl.Side[IN].Penalty, cl.Side[IN].Time, ts)
	if penConsumed > 0 {
		h.barter.AdjustBalance(peer, penConsumed, ts)
	}
	cl.Side[IN] = ClearingSide{}
	var kicks []string
	if cl.Side[OUT].Peer != "" {
		cl.Side[OUT].Queue.Enqueue("cancel")
		cl.CancelPre = pre
		kicks = []string{cl.Side[OUT].Peer}
	} else {
		h.removeClearing(m.ID)
	}
	return true, kicks, nil
}

func (h *Loops) respCancel(peer string, raw json.RawMessage, ok bool, ts int64) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return
	}
	cl.Side[OUT].Queue.Dequeue()
	if !ok {
		return
	}
	penConsumed := penaltyTick(cl.Amount, cl.Side[OUT].Penalty, cl.Side[OUT].Time, ts)
	if penConsumed > 0 {
		h.barter.AdjustBalance(peer, -penConsumed, ts)
	}
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer == "" {
		h.removeClearing(m.ID)
	}
}

// ---------------------------------------------------------------------------
// Clearing — rollback
// ---------------------------------------------------------------------------

func (h *Loops) reqRollback(peer string, raw json.RawMessage) (bool, []string, error) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return false, nil, fmt.Errorf("rollback: unknown clearing %s", m.ID)
	}
	if cl.Side[IN].Peer != peer {
		return false, nil, fmt.Errorf("rollback: wrong peer %s", peer)
	}

	cl.Side[IN] = ClearingSide{}
	if cl.Side[OUT].Peer != "" {
		cl.Side[OUT].Queue.Enqueue("rollback")
		return true, []string{cl.Side[OUT].Peer}, nil
	}
	h.removeClearing(m.ID)
	return true, nil, nil
}

func (h *Loops) respRollback(raw json.RawMessage) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return
	}
	cl.Side[OUT].Queue.Dequeue()
	cl.Side[OUT] = ClearingSide{}
	if cl.Side[IN].Peer == "" {
		h.removeClearing(m.ID)
	}
}

// ---------------------------------------------------------------------------
// Clearing — cleanup (timeout settlement)
// ---------------------------------------------------------------------------

func (h *Loops) reqCleanup(peer string, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return false, nil, nil
	}

	var settled bool
	var side int
	for s := 0; s < 2; s++ {
		if cl.Side[s].Peer != peer || cl.Side[s].Time == 0 {
			continue
		}
		elapsed := ts - cl.Side[s].Time
		if elapsed < 0 {
			continue
		}
		consumed := elapsed * int64(TickRate)
		if consumed < cl.Amount {
			continue
		}
		total := cl.Amount + cl.Side[s].Penalty
		if s == IN {
			h.barter.AdjustBalance(peer, total, ts)
		} else {
			h.barter.AdjustBalance(peer, -total, ts)
		}
		cl.Side[s] = ClearingSide{}
		settled = true
		side = s
	}
	if !settled {
		return false, nil, nil
	}

	var kicks []string
	other := 1 - side
	if cl.Side[other].Queue.Empty() && cl.Side[other].Peer != "" {
		cl.Side[other].Queue.Enqueue("cleanup")
		kicks = []string{cl.Side[other].Peer}
	}
	if cl.Side[0].Peer == "" && cl.Side[1].Peer == "" {
		h.removeClearing(m.ID)
	}
	return true, kicks, nil
}

func (h *Loops) respCleanup(peer string, raw json.RawMessage, ok bool, ts int64) {
	var m struct {
		ID string `json:"clearing_id"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	cl := h.Clearings[m.ID]
	if cl == nil {
		return
	}
	for s := 0; s < 2; s++ {
		if cl.Side[s].Peer == peer {
			cl.Side[s].Queue.Dequeue()
		}
	}
	if ok {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer {
				total := cl.Amount + cl.Side[s].Penalty
				if s == IN {
					h.barter.AdjustBalance(peer, total, ts)
				} else {
					h.barter.AdjustBalance(peer, -total, ts)
				}
				cl.Side[s] = ClearingSide{}
			}
		}
		if cl.Side[0].Peer == "" && cl.Side[1].Peer == "" {
			h.removeClearing(m.ID)
		}
	} else {
		h.schedule(CleanupRetry, func() []string {
			cl := h.Clearings[m.ID]
			if cl == nil {
				return nil
			}
			var kicks []string
			for s := 0; s < 2; s++ {
				if cl.Side[s].Peer == peer && cl.Side[s].Queue.Empty() {
					cl.Side[s].Queue.Enqueue("cleanup")
					kicks = append(kicks, peer)
				}
			}
			return kicks
		})
	}
}

// ---------------------------------------------------------------------------
// Clearing — message builders
// ---------------------------------------------------------------------------

func (h *Loops) buildClearingMsg(id string, cl *Clearing, side int) (string, json.RawMessage, int64) {
	switch cl.Side[side].Queue.Head() {
	case "commit":
		return "commit", creditnet.Enc(map[string]any{"clearing_id": id, "amount": cl.Amount, "penalty": cl.PenaltyRate}), 0
	case "finalize":
		m := map[string]any{"clearing_id": id}
		if cl.Preimage != nil {
			m["preimage"] = hex.EncodeToString(cl.Preimage)
		}
		if cl.Final {
			m["final"] = true
		}
		return "finalize", creditnet.Enc(m), 0
	case "cancel":
		m := map[string]any{"clearing_id": id}
		if cl.CancelPre != nil {
			m["preimage"] = hex.EncodeToString(cl.CancelPre)
		}
		return "cancel", creditnet.Enc(m), 0
	case "rollback":
		return "rollback", creditnet.Enc(map[string]string{"clearing_id": id}), 0
	case "cleanup":
		return "cleanup", creditnet.Enc(map[string]string{"clearing_id": id}), 0
	}
	return "", nil, 0
}

// ---------------------------------------------------------------------------
// Search scheduling
// ---------------------------------------------------------------------------

func (h *Loops) scheduleSearch() {
	h.schedule(SearchInterval, h.searchAndReschedule)
}

func (h *Loops) searchAndReschedule() []string {
	kicks := h.sendSearches()
	h.scheduleSearch()
	return kicks
}

func (h *Loops) sendSearches() []string {
	// Quick check: need at least one debtor and one creditor.
	tl := h.node.ModuleMap()["creditnet"].(*creditnet.Node)
	var hasDebtor, hasCreditor bool
	var amount int64
	for peerID := range tl.Trustlines {
		if !h.isConnected(peerID) || h.linkSlots(peerID, false) >= MaxPerLink {
			continue
		}
		bwOut := h.barter.Bandwidth(peerID, OUT)
		if bwOut > 0 {
			hasDebtor = true
			if amount == 0 || bwOut < amount {
				amount = bwOut
			}
		}
		if h.barter.Bandwidth(peerID, IN) > 0 {
			hasCreditor = true
		}
	}
	if !hasDebtor || !hasCreditor {
		return nil
	}

	seed := make([]byte, 32)
	rand.Read(seed)
	searchID := hex.EncodeToString(chainHash(seed, Nchunks))

	pen := amount * PenaltyRate / 1000
	gas := gasForDepth(0)
	p := &Path{
		Seed: seed, Amount: amount, Penalty: pen,
		Gas: gas, Depth: 1, Expiry: time.Now().Add(PathTTL).Unix(),
	}
	// Peer[0] and Peer[1] start EMPTY at initiator — set when searches bounce back.
	h.Paths[searchID] = p
	h.schedulePathExpiry(searchID)
	kicks := h.floodSearchInitiator(searchID, p)
	h.scheduleDepthIncrease(searchID)
	return kicks
}

func (h *Loops) floodSearchInitiator(searchID string, p *Path) []string {
	tl := h.node.ModuleMap()["creditnet"].(*creditnet.Node)
	var debtors, creditors []string
	for peerID := range tl.Trustlines {
		if !h.isConnected(peerID) || h.linkSlots(peerID, false) >= MaxPerLink {
			continue
		}
		bwOut := h.barter.Bandwidth(peerID, OUT)
		bwIn := h.barter.Bandwidth(peerID, IN)
		if bwOut > 0 {
			debtors = append(debtors, peerID)
		}
		if bwIn > 0 {
			creditors = append(creditors, peerID)
		}
	}
	if len(debtors) == 0 || len(creditors) == 0 {
		return nil
	}

	var kicks []string

	// Dir 0 — debtors.
	gasDir0 := p.Gas / 2
	if gasDir0 < 1 {
		gasDir0 = 1
	}
	share := gasDir0 / uint64(len(debtors))
	if share < 1 {
		share = 1
	}
	remaining := gasDir0
	for _, peerID := range debtors {
		if remaining < 1 {
			break
		}
		g := share
		if g > remaining {
			g = remaining
		}
		h.addProbe(peerID, &SearchProbe{
			SearchID: searchID, Direction: 0,
			Amount: p.Amount, Penalty: p.Penalty,
			Gas: g, Depth: p.Depth, Hops: 0,
		})
		kicks = append(kicks, peerID)
		remaining -= g
	}

	// Dir 1 — creditors.
	gasDir1 := p.Gas - gasDir0
	if gasDir1 < 1 {
		gasDir1 = 1
	}
	share = gasDir1 / uint64(len(creditors))
	if share < 1 {
		share = 1
	}
	remaining = gasDir1
	for _, peerID := range creditors {
		if remaining < 1 {
			break
		}
		g := share
		if g > remaining {
			g = remaining
		}
		h.addProbe(peerID, &SearchProbe{
			SearchID: searchID, Direction: 1,
			Amount: p.Amount, Penalty: p.Penalty,
			Gas: g, Depth: p.Depth, Hops: 0,
		})
		kicks = append(kicks, peerID)
		remaining -= g
	}

	return kicks
}

func (h *Loops) scheduleDepthIncrease(id string) {
	h.schedule(RefractoryDelay, func() []string {
		p := h.Paths[id]
		if p == nil || (p.Peer[0] != "" && p.Peer[1] != "") {
			return nil
		}
		if p.Depth >= MaxDepth {
			return nil
		}
		p.Gas += gasForDepth(p.Depth)
		p.Depth++
		kicks := h.floodSearchInitiator(id, p)
		h.scheduleDepthIncrease(id)
		return kicks
	})
}

func (h *Loops) startLoop(searchID string) []string {
	p := h.Paths[searchID]
	if p == nil || p.Peer[0] == "" {
		return nil
	}
	avl := h.barter.Bandwidth(p.Peer[0], OUT)
	if avl <= 0 {
		h.expirePath(searchID)
		return nil
	}
	amount := avl
	if p.Amount < amount {
		amount = p.Amount
	}
	if amount > ChunkSize {
		amount = ChunkSize
	}
	if amount == 0 {
		h.expirePath(searchID)
		return nil
	}
	step := Nchunks
	preimage := chainHash(p.Seed, step-1)
	cancelPre := chainHash(p.Seed, step-2)
	h.startClearing(searchID, amount, p.Peer[0], preimage, cancelPre, p.Seed, step)
	delete(h.Paths, searchID)
	return []string{p.Peer[0]}
}

// ---------------------------------------------------------------------------
// Cancel scheduling
// ---------------------------------------------------------------------------

func (h *Loops) scheduleCancel(id string) {
	h.schedule(CancelTimeout, func() []string {
		cl := h.Clearings[id]
		if cl == nil || cl.Seed == nil || cl.CancelPre == nil {
			return nil
		}
		if cl.Side[IN].Peer != "" {
			return nil // loop completed
		}
		if cl.Side[OUT].Peer == "" {
			return nil
		}
		cl.Side[OUT].Queue.Enqueue("cancel")
		return []string{cl.Side[OUT].Peer}
	})
}

// ---------------------------------------------------------------------------
// Cleanup scheduling
// ---------------------------------------------------------------------------

func (h *Loops) scheduleCleanup(id string, side int) {
	cl := h.Clearings[id]
	if cl == nil {
		return
	}
	t := cl.Side[side].Time
	if t == 0 {
		return
	}
	elapsed := time.Now().Unix() - t
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
		cl.Side[side].Queue.Enqueue("cleanup")
		return []string{peer}
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Loops) isConnected(peer string) bool {
	return h.node.ModuleAvailable(peer, h.name)
}

// Busy reports whether this module has active work with the given peer.
func (h *Loops) Busy(peer string) bool {
	for _, cl := range h.Clearings {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer {
				return true
			}
		}
	}
	return false
}

func (h *Loops) OnConnect(peer string) {}
func (h *Loops) OnClose(peer string)   {}

func (h *Loops) removeClearing(id string) {
	delete(h.Clearings, id)
}

// linkSlots counts operations on a link.
// reserved=false: all paths + all clearings (admission).
// reserved=true: clearings only (bandwidth).
func (h *Loops) linkSlots(peer string, reserved bool) int {
	count := 0
	for _, cl := range h.Clearings {
		if cl.Side[IN].Peer == peer || cl.Side[OUT].Peer == peer {
			count++
		}
	}
	if !reserved {
		for _, p := range h.Paths {
			if p.Peer[0] == peer || p.Peer[1] == peer {
				count++
			}
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Hash chain helpers
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

func penaltyTick(amount, penalty, commitTime, now int64) int64 {
	if penalty == 0 || commitTime == 0 {
		return 0
	}
	elapsed := now - commitTime
	if elapsed < 0 {
		elapsed = 0
	}
	tick := (elapsed + 1) * int64(TickRate)
	if tick > amount {
		tick = amount
	}
	return penalty * tick / amount
}

func gasForDepth(depth uint8) uint64 {
	g := uint64(1)
	for i := uint8(0); i <= depth; i++ {
		g *= GasBase
	}
	return g
}
