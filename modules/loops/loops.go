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
// everything here is sync-based: search, found, prepare, and path cleanup
// all go through the bilateral sync protocol.
package loops

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"creditnet"
	"creditnet/link"
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
	ChunkSize       = uint64(1000000)
	Nchunks         = 1024
	MaxAmount       = uint64(1<<63 - 1)
	GasBase         = 16
)

// ---------------------------------------------------------------------------
// Types — Path (search phase, synced)
// ---------------------------------------------------------------------------

// Path tracks search state. Both sides are synced: cleanup is explicit.
type Path struct {
	Amount  uint64 `json:"amount"`
	Penalty uint64 `json:"penalty"`
	Gas     uint64 `json:"gas"`
	Depth   uint8  `json:"depth"`
	Hops    uint8  `json:"hops"`
	Commit  bool   `json:"commit,omitempty"`   // prepare accepted
	InPeer  string `json:"in_peer,omitempty"`  // direction 0: who sent search to us
	OutPeer string `json:"out_peer,omitempty"` // direction 1: who sent search to us
	Seed    []byte `json:"seed,omitempty"`     // initiator only
	Queue   []string `json:"queue,omitempty"`
}

func (p *Path) pendingPeer() string {
	head := link.QueueHead(p.Queue)
	switch head {
	case "found", "prepare":
		return p.InPeer // propagate backwards
	default:
		return p.OutPeer // search, cleanup forward
	}
}

// ---------------------------------------------------------------------------
// Types — Clearing (settlement phase, synced)
// ---------------------------------------------------------------------------

type Clearing struct {
	Amount      uint64         `json:"amount"`
	PenaltyRate uint64         `json:"penalty_rate"`
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
	Penalty uint64   `json:"penalty,omitempty"`
	Queue   []string `json:"queue,omitempty"`
}

const (
	sideIN  = 0
	sideOUT = 1
)

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

type Handler struct {
	Paths     map[string]*Path     `json:"paths,omitempty"`
	Clearings map[string]*Clearing `json:"clearings,omitempty"`

	nodeID     string
	schedule   creditnet.ScheduleFunc
	trustlines map[string]*creditnet.Trustline
	bandwidth  creditnet.BandwidthFunc
}

func NewHandler(nodeID string, schedule creditnet.ScheduleFunc, trustlines map[string]*creditnet.Trustline, bandwidth creditnet.BandwidthFunc, state json.RawMessage) creditnet.Module {
	h := &Handler{
		Paths:      map[string]*Path{},
		Clearings:  map[string]*Clearing{},
		nodeID:     nodeID,
		schedule:   schedule,
		trustlines: trustlines,
		bandwidth:  bandwidth,
	}
	if state != nil {
		json.Unmarshal(state, h)
	}
	if h.Paths == nil {
		h.Paths = map[string]*Path{}
	}
	if h.Clearings == nil {
		h.Clearings = map[string]*Clearing{}
	}
	h.scheduleSearch()
	// Re-schedule cancel timers for persisted clearings.
	for id, cl := range h.Clearings {
		if cl.Seed != nil && cl.CancelPre != nil &&
			cl.Side[sideIN].Peer == "" && cl.Side[sideOUT].Peer != "" {
			h.scheduleCancel(id)
		}
	}
	return h
}

// ---------------------------------------------------------------------------
// Reserver
// ---------------------------------------------------------------------------

func (h *Handler) Reserved(peer string) (in, out int64) {
	for _, p := range h.Paths {
		if p.Commit {
			if p.InPeer == peer {
				in += int64(p.Amount)
			}
			if p.OutPeer == peer {
				out += int64(p.Amount)
			}
		}
	}
	for _, cl := range h.Clearings {
		if cl.Side[sideIN].Peer == peer {
			in += int64(cl.Amount + cl.Side[sideIN].Penalty)
		}
		if cl.Side[sideOUT].Peer == peer {
			out += int64(cl.Amount + cl.Side[sideOUT].Penalty)
		}
	}
	return
}

func (h *Handler) LinkDeleted(peer string) {
	for id, p := range h.Paths {
		if p.InPeer == peer || p.OutPeer == peer {
			delete(h.Paths, id)
		}
	}
	for id, cl := range h.Clearings {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer {
				cl.Side[s] = ClearingSide{}
			}
		}
		if cl.Side[0].Peer == "" && cl.Side[1].Peer == "" {
			delete(h.Clearings, id)
		}
	}
}

// ---------------------------------------------------------------------------
// Module — HandleRequest
// ---------------------------------------------------------------------------

func (h *Handler) HandleRequest(peer string, t *creditnet.Trustline, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	switch method {
	// Path phase
	case "search":
		return h.reqSearch(peer, payload)
	case "found":
		return h.reqFound(peer, payload)
	case "prepare":
		return h.reqPrepare(peer, payload)
	case "expire":
		return h.reqExpire(peer, payload)
	// Clearing phase
	case "commit":
		return h.reqCommit(peer, t, payload, ts)
	case "finalize":
		return h.reqFinalize(peer, t, payload, ts)
	case "cancel":
		return h.reqCancel(peer, t, payload, ts)
	case "rollback":
		return h.reqRollback(peer, payload)
	case "cleanup":
		return h.reqCleanup(peer, t, payload, ts)
	}
	return false, nil, creditnet.ErrBadMethod
}

// ---------------------------------------------------------------------------
// Module — HandleResponse
// ---------------------------------------------------------------------------

func (h *Handler) HandleResponse(peer string, t *creditnet.Trustline, method string, payload json.RawMessage, ok bool, ts int64) []string {
	switch method {
	case "search":
		return h.respSearch(payload, ok)
	case "found":
		h.respFound(payload, ok)
	case "prepare":
		h.respPrepare(payload, ok)
	case "expire":
		h.respExpire(payload)
	case "commit":
		return h.respCommit(payload, ok, ts)
	case "finalize":
		h.respFinalize(t, payload, ok, ts)
	case "cancel":
		h.respCancel(t, payload, ok, ts)
	case "rollback":
		h.respRollback(payload)
	case "cleanup":
		h.respCleanup(peer, t, payload, ok, ts)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Module — NextTx
// ---------------------------------------------------------------------------

func (h *Handler) NextTx(peer string) *link.QueuedMsg {
	// Paths first — search is lightweight.
	for id, p := range h.Paths {
		if len(p.Queue) > 0 && p.pendingPeer() == peer {
			return h.buildPathMsg(id, p)
		}
	}
	// Then clearings.
	for id, cl := range h.Clearings {
		for side := 0; side < 2; side++ {
			if cl.Side[side].Peer == peer && link.QueueHead(cl.Side[side].Queue) != "" {
				return h.buildClearingMsg(id, cl, side)
			}
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
		return creditnet.Enc(map[string]any{
			"paths":     len(h.Paths),
			"clearings": len(h.Clearings),
		}), nil
	}
	return nil, creditnet.ErrBadMethod
}

// ---------------------------------------------------------------------------
// Path — search request/response
// ---------------------------------------------------------------------------

type searchMsg struct {
	ID        string `json:"clearing_id"`
	Amount    uint64 `json:"amount"`
	Penalty   uint64 `json:"penalty"`
	Depth     uint8  `json:"depth"`
	Gas       uint64 `json:"gas"`
	Direction uint8  `json:"direction"` // 0=debtors, 1=creditors
	Hops      uint8  `json:"hops"`
}

func (h *Handler) reqSearch(peer string, raw json.RawMessage) (bool, []string, error) {
	var m searchMsg
	if json.Unmarshal(raw, &m) != nil || m.Amount == 0 {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Direction > 1 {
		return false, nil, nil
	}

	p := h.Paths[m.ID]
	if p != nil {
		// Already have this path — check if loop closure.
		dir := m.Direction
		if dir == 0 && p.InPeer == "" && p.OutPeer != peer {
			p.InPeer = peer
			if m.Amount < p.Amount {
				p.Amount = m.Amount
			}
			if p.OutPeer != "" {
				// Both sides connected — found!
				link.Enqueue(&p.Queue, "found")
				return true, []string{p.OutPeer}, nil
			}
			return true, nil, nil
		}
		if dir == 1 && p.OutPeer == "" && p.InPeer != peer {
			p.OutPeer = peer
			if m.Amount < p.Amount {
				p.Amount = m.Amount
			}
			if p.InPeer != "" {
				link.Enqueue(&p.Queue, "found")
				return true, []string{p.InPeer}, nil
			}
			return true, nil, nil
		}
		return false, nil, nil // duplicate or conflict
	}

	// New path — accept and forward.
	if m.Penalty*2000 < m.Amount*PenaltyRate {
		return false, nil, nil
	}
	if h.linkSlots(peer, false) >= MaxPerLink {
		return false, nil, nil
	}

	p = &Path{Amount: m.Amount, Penalty: m.Penalty, Hops: m.Hops + 1, Depth: m.Depth, Gas: m.Gas}
	if m.Direction == 0 {
		p.InPeer = peer
	} else {
		p.OutPeer = peer
	}
	h.Paths[m.ID] = p

	// Forward to peers with debt in the right direction.
	var kicks []string
	for peerID := range h.trustlines {
		if peerID == peer || !h.isConnected(peerID) {
			continue
		}
		if h.linkSlots(peerID, false) >= MaxPerLink {
			continue
		}
		_, bwOut := h.bandwidth(peerID)
		bwIn, _ := h.bandwidth(peerID)
		if m.Direction == 0 && bwOut > 0 {
			if p.OutPeer == "" {
				p.OutPeer = peerID
				link.Enqueue(&p.Queue, "search")
				kicks = append(kicks, peerID)
				break
			}
		}
		if m.Direction == 1 && bwIn > 0 {
			if p.InPeer == "" {
				p.InPeer = peerID
				link.Enqueue(&p.Queue, "search")
				kicks = append(kicks, peerID)
				break
			}
		}
	}

	h.schedulePathExpiry(m.ID)
	return true, kicks, nil
}

func (h *Handler) respSearch(raw json.RawMessage, ok bool) []string {
	var m searchMsg
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	p := h.Paths[m.ID]
	if p == nil {
		return nil
	}
	link.Dequeue(&p.Queue)
	if !ok {
		// Search rejected — try next peer or give up.
		if m.Direction == 0 {
			p.OutPeer = ""
		} else {
			p.InPeer = ""
		}
		// Try forwarding to another peer.
		for peerID := range h.trustlines {
			if !h.isConnected(peerID) || h.linkSlots(peerID, false) >= MaxPerLink {
				continue
			}
			_, bwOut := h.bandwidth(peerID)
			bwIn, _ := h.bandwidth(peerID)
			if m.Direction == 0 && bwOut > 0 && p.OutPeer == "" && peerID != p.InPeer {
				p.OutPeer = peerID
				link.Enqueue(&p.Queue, "search")
				return []string{peerID}
			}
			if m.Direction == 1 && bwIn > 0 && p.InPeer == "" && peerID != p.OutPeer {
				p.InPeer = peerID
				link.Enqueue(&p.Queue, "search")
				return []string{peerID}
			}
		}
		// No more peers to try.
		if p.InPeer == "" && p.OutPeer == "" {
			delete(h.Paths, m.ID)
		}
	}
	return nil
}

func (h *Handler) buildSearchPayload(id string, p *Path, dir uint8) json.RawMessage {
	return creditnet.Enc(searchMsg{
		ID: id, Amount: p.Amount, Penalty: p.Penalty,
		Depth: p.Depth, Gas: p.Gas, Direction: dir, Hops: p.Hops,
	})
}

// ---------------------------------------------------------------------------
// Path — found request/response
// ---------------------------------------------------------------------------

func (h *Handler) reqFound(peer string, raw json.RawMessage) (bool, []string, error) {
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
	if p.OutPeer != peer {
		return false, nil, nil
	}
	p.Hops = m.Hops + 1

	if p.Seed != nil {
		// Initiator — start loop settlement.
		return true, h.startLoop(m.ID), nil
	}
	// Intermediate — forward found to InPeer.
	if p.InPeer != "" {
		link.Enqueue(&p.Queue, "found")
		return true, []string{p.InPeer}, nil
	}
	return true, nil, nil
}

func (h *Handler) respFound(raw json.RawMessage, ok bool) {
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
	link.Dequeue(&p.Queue)
}

// ---------------------------------------------------------------------------
// Path — prepare request/response
// ---------------------------------------------------------------------------

func (h *Handler) reqPrepare(peer string, raw json.RawMessage) (bool, []string, error) {
	var m struct {
		ID     string `json:"clearing_id"`
		Amount uint64 `json:"amount"`
	}
	if json.Unmarshal(raw, &m) != nil || m.Amount == 0 {
		return false, nil, fmt.Errorf("bad payload")
	}
	p := h.Paths[m.ID]
	if p == nil {
		return false, nil, nil
	}
	if p.OutPeer != peer {
		return false, nil, nil
	}

	if p.Seed != nil {
		// Initiator — prepare came back. Start 2PC.
		if m.Amount > ChunkSize {
			m.Amount = ChunkSize
		}
		step := Nchunks
		preimage := chainHash(p.Seed, step-1)
		cancelPre := chainHash(p.Seed, step-2)
		h.startClearing(m.ID, m.Amount, p.InPeer, preimage, cancelPre, p.Seed, step)
		delete(h.Paths, m.ID)
		return true, []string{p.InPeer}, nil
	}

	// Intermediate — check bandwidth and forward.
	bwIn, _ := h.bandwidth(peer)
	if bwIn < int64(m.Amount) {
		return false, nil, nil
	}
	if p.InPeer == "" {
		return false, nil, nil
	}
	_, bwOut := h.bandwidth(p.InPeer)
	if bwOut < int64(m.Amount) {
		m.Amount = uint64(bwOut)
	}
	if m.Amount > ChunkSize {
		m.Amount = ChunkSize
	}
	if m.Amount == 0 {
		return false, nil, nil
	}
	p.Commit = true
	p.Amount = m.Amount
	link.Enqueue(&p.Queue, "prepare")
	return true, []string{p.InPeer}, nil
}

func (h *Handler) respPrepare(raw json.RawMessage, ok bool) {
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
	link.Dequeue(&p.Queue)
	if !ok {
		p.Commit = false
		// Could try with less amount, but for now just give up.
		h.expirePath(m.ID)
	}
}

// ---------------------------------------------------------------------------
// Path — cleanup
// ---------------------------------------------------------------------------

func (h *Handler) reqExpire(peer string, raw json.RawMessage) (bool, []string, error) {
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
	// Accept cleanup from either side.
	if p.InPeer != peer && p.OutPeer != peer {
		return false, nil, nil
	}
	// Forward cleanup to the other side.
	var kicks []string
	if p.InPeer == peer {
		p.InPeer = ""
		if p.OutPeer != "" {
			link.Enqueue(&p.Queue, "expire")
			kicks = []string{p.OutPeer}
		}
	} else {
		p.OutPeer = ""
		if p.InPeer != "" {
			link.Enqueue(&p.Queue, "expire")
			kicks = []string{p.InPeer}
		}
	}
	if p.InPeer == "" && p.OutPeer == "" {
		delete(h.Paths, m.ID)
	}
	return true, kicks, nil
}

func (h *Handler) respExpire(raw json.RawMessage) {
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
	link.Dequeue(&p.Queue)
	// Remove the side we cleaned up.
	head := link.QueueHead(p.Queue)
	if head == "" {
		// Cleanup was the last thing — now remove our side too.
		delete(h.Paths, m.ID)
	}
}

func (h *Handler) expirePath(id string) []string {
	p := h.Paths[id]
	if p == nil {
		return nil
	}
	// Send cleanup to both sides.
	var kicks []string
	if p.OutPeer != "" {
		link.Enqueue(&p.Queue, "expire")
		kicks = append(kicks, p.OutPeer)
	}
	if p.InPeer != "" && p.OutPeer == "" {
		link.Enqueue(&p.Queue, "expire")
		kicks = append(kicks, p.InPeer)
	}
	if p.InPeer == "" && p.OutPeer == "" {
		delete(h.Paths, id)
	}
	return kicks
}

func (h *Handler) schedulePathExpiry(id string) {
	h.schedule(PathTTL, func() []string {
		return h.expirePath(id)
	})
}

// ---------------------------------------------------------------------------
// Path — message builders
// ---------------------------------------------------------------------------

func (h *Handler) buildPathMsg(id string, p *Path) *link.QueuedMsg {
	switch link.QueueHead(p.Queue) {
	case "search":
		// Determine direction from which side we're sending to.
		var dir uint8
		if p.pendingPeer() == p.OutPeer {
			dir = 0 // searching debtors
		} else {
			dir = 1 // searching creditors
		}
		return &link.QueuedMsg{
			Method:  "search",
			Payload: h.buildSearchPayload(id, p, dir),
		}
	case "found":
		return &link.QueuedMsg{
			Method:  "found",
			Payload: creditnet.Enc(map[string]any{"clearing_id": id, "hops": p.Hops}),
		}
	case "prepare":
		return &link.QueuedMsg{
			Method:  "prepare",
			Payload: creditnet.Enc(map[string]any{"clearing_id": id, "amount": p.Amount}),
		}
	case "expire":
		return &link.QueuedMsg{
			Method:  "expire",
			Payload: creditnet.Enc(map[string]string{"clearing_id": id}),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Clearing — commit
// ---------------------------------------------------------------------------

func (h *Handler) startClearing(id string, amount uint64, outPeer string, preimage, cancelPre, seed []byte, step int) {
	pen := amount * PenaltyRate / 1000
	h.Clearings[id] = &Clearing{
		Amount: amount, PenaltyRate: pen,
		Preimage: preimage, CancelPre: cancelPre,
		Final: step <= 2,
		Seed: seed, Step: step,
		Side: [2]ClearingSide{
			{}, // IN — filled when commit comes back
			{Peer: outPeer, Penalty: pen, Queue: []string{"commit"}},
		},
	}
	h.refPeer(outPeer)
}

func (h *Handler) reqCommit(peer string, t *creditnet.Trustline, raw json.RawMessage, ts int64) (bool, []string, error) {
	var m struct {
		ID      string `json:"clearing_id"`
		Amount  uint64 `json:"amount"`
		Penalty uint64 `json:"penalty"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return false, nil, fmt.Errorf("bad payload")
	}
	if m.Amount == 0 || m.Amount > MaxAmount {
		return false, nil, nil
	}

	// Loop closure: initiator receives commit back.
	if cl := h.Clearings[m.ID]; cl != nil {
		if cl.Side[sideIN].Peer != "" {
			return false, nil, nil
		}
		p := h.Paths[m.ID]
		var hops uint8
		if p != nil {
			hops = p.Hops
			delete(h.Paths, m.ID)
		}
		inPen := m.Penalty * uint64(hops+1)
		total := int64(m.Amount + inPen)
		bwIn, _ := h.bandwidth(peer)
		if bwIn < total {
			return false, nil, nil
		}
		cl.Side[sideIN] = ClearingSide{Peer: peer, Time: ts, Penalty: inPen}
		h.refPeer(peer)
		h.scheduleCleanup(m.ID, sideIN)
		link.Enqueue(&cl.Side[sideOUT].Queue, "finalize")
		return true, []string{cl.Side[sideOUT].Peer}, nil
	}

	// Intermediate: look up path.
	p := h.Paths[m.ID]
	if p == nil {
		return false, nil, nil
	}
	hops := p.Hops
	outPeer := p.InPeer // path IN = clearing OUT (towards initiator)
	delete(h.Paths, m.ID)

	inPen := m.Penalty * uint64(hops+1)
	total := int64(m.Amount + inPen)
	bwIn, _ := h.bandwidth(peer)
	if bwIn < total {
		return false, nil, nil
	}

	outPen := m.Penalty * uint64(hops)
	cl := &Clearing{
		Amount: m.Amount, PenaltyRate: m.Penalty,
		Side: [2]ClearingSide{
			{Peer: peer, Time: ts, Penalty: inPen},
			{},
		},
	}
	h.Clearings[m.ID] = cl
	h.refPeer(peer)
	h.scheduleCleanup(m.ID, sideIN)

	if outPeer != "" {
		cl.Side[sideOUT] = ClearingSide{Peer: outPeer, Penalty: outPen, Queue: []string{"commit"}}
		h.refPeer(outPeer)
		return true, []string{outPeer}, nil
	}
	return true, nil, nil
}

func (h *Handler) respCommit(raw json.RawMessage, ok bool, ts int64) []string {
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
	link.Dequeue(&cl.Side[sideOUT].Queue)
	if ok {
		cl.Side[sideOUT].Time = ts
		h.scheduleCleanup(m.ID, sideOUT)
		if cl.Seed != nil && cl.CancelPre != nil {
			h.scheduleCancel(m.ID)
		}
		return nil
	}
	// Rejected — rollback IN side.
	h.unrefPeer(cl.Side[sideOUT].Peer)
	cl.Side[sideOUT] = ClearingSide{}
	if cl.Side[sideIN].Peer != "" {
		link.Enqueue(&cl.Side[sideIN].Queue, "rollback")
		return []string{cl.Side[sideIN].Peer}
	}
	h.removeClearing(m.ID)
	return nil
}

// ---------------------------------------------------------------------------
// Clearing — finalize
// ---------------------------------------------------------------------------

func (h *Handler) reqFinalize(peer string, t *creditnet.Trustline, raw json.RawMessage, ts int64) (bool, []string, error) {
	if t == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
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
		return false, nil, nil
	}
	if cl.Side[sideIN].Peer != peer {
		return false, nil, nil
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !hashEq(pre, m.ID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	// Settle IN side.
	penConsumed := penaltyTick(cl.Amount, cl.Side[sideIN].Penalty, cl.Side[sideIN].Time, ts)
	t.AddBalance(int64(cl.Amount+penConsumed), ts)
	outPeer := cl.Side[sideOUT].Peer

	// Continuation: create next chunk.
	if !m.Final && outPeer != "" {
		nextID := hex.EncodeToString(pre)
		if h.Clearings[nextID] == nil {
			nextCl := &Clearing{
				Amount: cl.Amount, PenaltyRate: cl.PenaltyRate,
				Side: [2]ClearingSide{
					{Peer: peer, Time: ts, Penalty: cl.Side[sideIN].Penalty},
					{Peer: outPeer, Penalty: cl.Side[sideOUT].Penalty},
				},
			}
			h.refPeer(peer)
			h.refPeer(outPeer)
			if cl.Seed != nil {
				nextStep := cl.Step - 1
				nextCl.Seed = cl.Seed
				nextCl.Step = nextStep
				if nextStep >= 1 {
					nextCl.Preimage = chainHash(cl.Seed, nextStep-1)
				}
				if nextStep >= 2 {
					nextCl.CancelPre = chainHash(cl.Seed, nextStep-2)
				}
				_, avl := h.bandwidth(outPeer)
				nextCl.Final = nextStep <= 2 || avl < int64(nextCl.Amount)
				link.Enqueue(&nextCl.Side[sideOUT].Queue, "finalize")
			}
			h.Clearings[nextID] = nextCl
			h.scheduleCleanup(nextID, sideIN)
		}
	}

	// Forward finalize to OUT.
	h.unrefPeer(peer)
	cl.Side[sideIN] = ClearingSide{}
	var kicks []string
	if outPeer != "" && len(cl.Side[sideOUT].Queue) == 0 {
		link.Enqueue(&cl.Side[sideOUT].Queue, "finalize")
		cl.Preimage = pre
		cl.Final = m.Final
		kicks = []string{outPeer}
	}
	if cl.Side[sideIN].Peer == "" && cl.Side[sideOUT].Peer == "" {
		h.removeClearing(m.ID)
	}
	return true, kicks, nil
}

func (h *Handler) respFinalize(t *creditnet.Trustline, raw json.RawMessage, ok bool, ts int64) {
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
	outPeer := cl.Side[sideOUT].Peer
	link.Dequeue(&cl.Side[sideOUT].Queue)
	if ok {
		penConsumed := penaltyTick(cl.Amount, cl.Side[sideOUT].Penalty, cl.Side[sideOUT].Time, ts)
		t.AddBalance(-int64(cl.Amount+penConsumed), ts)
		h.unrefPeer(outPeer)
		cl.Side[sideOUT] = ClearingSide{}
		if cl.Side[sideIN].Peer == "" {
			h.removeClearing(m.ID)
		}
	} else {
		h.unrefPeer(outPeer)
		cl.Side[sideOUT] = ClearingSide{}
		if cl.Side[sideIN].Peer != "" {
			link.Enqueue(&cl.Side[sideIN].Queue, "rollback")
		} else {
			h.removeClearing(m.ID)
		}
	}
}

// ---------------------------------------------------------------------------
// Clearing — cancel
// ---------------------------------------------------------------------------

func (h *Handler) reqCancel(peer string, t *creditnet.Trustline, raw json.RawMessage, ts int64) (bool, []string, error) {
	if t == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
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
	if cl.Side[sideIN].Peer != peer {
		return false, nil, nil
	}

	pre, _ := hex.DecodeString(m.Preimage)
	if len(pre) == 0 || !doubleHashEq(pre, m.ID) {
		return false, nil, fmt.Errorf("bad preimage")
	}

	penConsumed := penaltyTick(cl.Amount, cl.Side[sideIN].Penalty, cl.Side[sideIN].Time, ts)
	if penConsumed > 0 {
		t.AddBalance(int64(penConsumed), ts)
	}
	h.unrefPeer(peer)
	cl.Side[sideIN] = ClearingSide{}
	var kicks []string
	if cl.Side[sideOUT].Peer != "" {
		link.Enqueue(&cl.Side[sideOUT].Queue, "cancel")
		cl.CancelPre = pre
		kicks = []string{cl.Side[sideOUT].Peer}
	} else {
		h.removeClearing(m.ID)
	}
	return true, kicks, nil
}

func (h *Handler) respCancel(t *creditnet.Trustline, raw json.RawMessage, ok bool, ts int64) {
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
	link.Dequeue(&cl.Side[sideOUT].Queue)
	if !ok {
		return
	}
	penConsumed := penaltyTick(cl.Amount, cl.Side[sideOUT].Penalty, cl.Side[sideOUT].Time, ts)
	if penConsumed > 0 {
		t.AddBalance(-int64(penConsumed), ts)
	}
	h.unrefPeer(cl.Side[sideOUT].Peer)
	cl.Side[sideOUT] = ClearingSide{}
	if cl.Side[sideIN].Peer == "" {
		h.removeClearing(m.ID)
	}
}

// ---------------------------------------------------------------------------
// Clearing — rollback
// ---------------------------------------------------------------------------

func (h *Handler) reqRollback(peer string, raw json.RawMessage) (bool, []string, error) {
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
	if cl.Side[sideIN].Peer != peer {
		return false, nil, nil
	}

	h.unrefPeer(peer)
	cl.Side[sideIN] = ClearingSide{}
	if cl.Side[sideOUT].Peer != "" {
		link.Enqueue(&cl.Side[sideOUT].Queue, "rollback")
		return true, []string{cl.Side[sideOUT].Peer}, nil
	}
	h.removeClearing(m.ID)
	return true, nil, nil
}

func (h *Handler) respRollback(raw json.RawMessage) {
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
	link.Dequeue(&cl.Side[sideOUT].Queue)
	h.unrefPeer(cl.Side[sideOUT].Peer)
	cl.Side[sideOUT] = ClearingSide{}
	if cl.Side[sideIN].Peer == "" {
		h.removeClearing(m.ID)
	}
}

// ---------------------------------------------------------------------------
// Clearing — cleanup (timeout settlement)
// ---------------------------------------------------------------------------

func (h *Handler) reqCleanup(peer string, t *creditnet.Trustline, raw json.RawMessage, ts int64) (bool, []string, error) {
	if t == nil {
		return false, nil, fmt.Errorf("trustline not connected")
	}
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
		consumed := uint64(elapsed) * TickRate
		if consumed < cl.Amount {
			continue
		}
		total := int64(cl.Amount + cl.Side[s].Penalty)
		if s == sideIN {
			t.AddBalance(total, ts)
		} else {
			t.AddBalance(-total, ts)
		}
		h.unrefPeer(peer)
		cl.Side[s] = ClearingSide{}
		settled = true
		side = s
	}
	if !settled {
		return false, nil, nil
	}

	var kicks []string
	other := 1 - side
	if len(cl.Side[other].Queue) == 0 && cl.Side[other].Peer != "" {
		link.Enqueue(&cl.Side[other].Queue, "cleanup")
		kicks = []string{cl.Side[other].Peer}
	}
	if cl.Side[0].Peer == "" && cl.Side[1].Peer == "" {
		h.removeClearing(m.ID)
	}
	return true, kicks, nil
}

func (h *Handler) respCleanup(peer string, t *creditnet.Trustline, raw json.RawMessage, ok bool, ts int64) {
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
			link.Dequeue(&cl.Side[s].Queue)
		}
	}
	if ok {
		for s := 0; s < 2; s++ {
			if cl.Side[s].Peer == peer {
				total := int64(cl.Amount + cl.Side[s].Penalty)
				if s == sideIN {
					t.AddBalance(total, ts)
				} else {
					t.AddBalance(-total, ts)
				}
				h.unrefPeer(peer)
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
				if cl.Side[s].Peer == peer && len(cl.Side[s].Queue) == 0 {
					link.Enqueue(&cl.Side[s].Queue, "cleanup")
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

func (h *Handler) buildClearingMsg(id string, cl *Clearing, side int) *link.QueuedMsg {
	switch link.QueueHead(cl.Side[side].Queue) {
	case "commit":
		return &link.QueuedMsg{
			Method:  "commit",
			Payload: creditnet.Enc(map[string]any{"clearing_id": id, "amount": cl.Amount, "penalty": cl.PenaltyRate}),
		}
	case "finalize":
		m := map[string]any{"clearing_id": id}
		if cl.Preimage != nil {
			m["preimage"] = hex.EncodeToString(cl.Preimage)
		}
		if cl.Final {
			m["final"] = true
		}
		return &link.QueuedMsg{Method: "finalize", Payload: creditnet.Enc(m)}
	case "cancel":
		m := map[string]any{"clearing_id": id}
		if cl.CancelPre != nil {
			m["preimage"] = hex.EncodeToString(cl.CancelPre)
		}
		return &link.QueuedMsg{Method: "cancel", Payload: creditnet.Enc(m)}
	case "rollback":
		return &link.QueuedMsg{
			Method:  "rollback",
			Payload: creditnet.Enc(map[string]string{"clearing_id": id}),
		}
	case "cleanup":
		return &link.QueuedMsg{
			Method:  "cleanup",
			Payload: creditnet.Enc(map[string]string{"clearing_id": id}),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Search scheduling
// ---------------------------------------------------------------------------

func (h *Handler) scheduleSearch() {
	h.schedule(SearchInterval, h.searchAndReschedule)
}

func (h *Handler) searchAndReschedule() []string {
	kicks := h.sendSearches()
	h.scheduleSearch()
	return kicks
}

func (h *Handler) sendSearches() []string {
	var debtors, creditors []string
	for peerID := range h.trustlines {
		if !h.isConnected(peerID) || h.linkSlots(peerID, false) >= MaxPerLink {
			continue
		}
		_, bwOut := h.bandwidth(peerID)
		bwIn, _ := h.bandwidth(peerID)
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

	seed := make([]byte, 32)
	rand.Read(seed)
	searchID := hex.EncodeToString(chainHash(seed, Nchunks))

	var amount uint64
	for _, peer := range debtors {
		_, bwOut := h.bandwidth(peer)
		if amount == 0 || bwOut < int64(amount) {
			amount = uint64(bwOut)
		}
	}

	pen := amount * PenaltyRate / 1000
	p := &Path{
		Seed: seed, Amount: amount, Penalty: pen,
		Gas: gasForDepth(0), Depth: 1,
	}
	h.Paths[searchID] = p
	h.schedulePathExpiry(searchID)

	// Send search to first debtor (direction 0) and first creditor (direction 1).
	var kicks []string
	if len(debtors) > 0 {
		p.OutPeer = debtors[0]
		link.Enqueue(&p.Queue, "search")
		kicks = append(kicks, debtors[0])
	}
	if len(creditors) > 0 {
		p.InPeer = creditors[0]
		link.Enqueue(&p.Queue, "search")
		kicks = append(kicks, creditors[0])
	}
	return kicks
}

func (h *Handler) startLoop(searchID string) []string {
	p := h.Paths[searchID]
	if p == nil || p.InPeer == "" {
		return nil
	}
	_, avl := h.bandwidth(p.InPeer)
	if avl <= 0 {
		h.expirePath(searchID)
		return nil
	}
	amount := uint64(avl)
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
	p.Amount = amount
	link.Enqueue(&p.Queue, "prepare")
	return []string{p.InPeer}
}

// ---------------------------------------------------------------------------
// Cancel scheduling
// ---------------------------------------------------------------------------

func (h *Handler) scheduleCancel(id string) {
	h.schedule(CancelTimeout, func() []string {
		cl := h.Clearings[id]
		if cl == nil || cl.Seed == nil || cl.CancelPre == nil {
			return nil
		}
		if cl.Side[sideIN].Peer != "" {
			return nil // loop completed
		}
		if cl.Side[sideOUT].Peer == "" {
			return nil
		}
		link.Enqueue(&cl.Side[sideOUT].Queue, "cancel")
		return []string{cl.Side[sideOUT].Peer}
	})
}

// ---------------------------------------------------------------------------
// Cleanup scheduling
// ---------------------------------------------------------------------------

func (h *Handler) scheduleCleanup(id string, side int) {
	cl := h.Clearings[id]
	if cl == nil {
		return
	}
	t := cl.Side[side].Time
	if t == 0 {
		return
	}
	elapsed := time.Now().Unix() - t
	remaining := int64(cl.Amount/TickRate) - elapsed
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
		link.Enqueue(&cl.Side[side].Queue, "cleanup")
		return []string{peer}
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Handler) isConnected(peer string) bool {
	t := h.trustlines[peer]
	return t != nil && t.State.Status == link.Connected && !t.State.IntentClose
}

func (h *Handler) refPeer(peer string) {
	if t := h.trustlines[peer]; t != nil {
		t.State.Ref()
	}
}

func (h *Handler) unrefPeer(peer string) {
	if t := h.trustlines[peer]; t != nil {
		t.State.Unref()
	}
}

func (h *Handler) removeClearing(id string) {
	cl := h.Clearings[id]
	if cl == nil {
		return
	}
	for s := 0; s < 2; s++ {
		if cl.Side[s].Peer != "" {
			h.unrefPeer(cl.Side[s].Peer)
		}
	}
	delete(h.Clearings, id)
}

// linkSlots counts operations on a link.
// reserved=false: all paths + all clearings (admission).
// reserved=true: committed paths + all clearings (bandwidth).
func (h *Handler) linkSlots(peer string, reserved bool) int {
	count := 0
	for _, cl := range h.Clearings {
		if cl.Side[sideIN].Peer == peer || cl.Side[sideOUT].Peer == peer {
			count++
		}
	}
	for _, p := range h.Paths {
		if p.InPeer != peer && p.OutPeer != peer {
			continue
		}
		if reserved && !p.Commit {
			continue
		}
		count++
	}
	return count
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

func penaltyTick(amount, penalty uint64, commitTime, now int64) uint64 {
	if penalty == 0 || commitTime == 0 {
		return 0
	}
	elapsed := now - commitTime
	if elapsed < 0 {
		elapsed = 0
	}
	tick := uint64(elapsed+1) * TickRate
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
