package link

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Node — a peer in the network with bilateral links
// ---------------------------------------------------------------------------

type Node struct {
	mu           sync.Mutex
	id           string
	links        map[string]*Link
	inFlight     map[string]bool
	allowInvites bool

	transport Transport
	store     NodeStore
}

type nodeState struct {
	ID           string           `json:"id"`
	Links        map[string]*Link `json:"links"`
	AllowInvites *bool            `json:"allow_invites,omitempty"`
}

func NewNode(id string, raw json.RawMessage, transport Transport, store NodeStore) *Node {
	var st nodeState
	if raw != nil {
		json.Unmarshal(raw, &st)
	}
	if st.Links == nil {
		st.Links = map[string]*Link{}
	}
	invites := true
	if st.AllowInvites != nil {
		invites = *st.AllowInvites
	}
	return &Node{
		id:           id,
		links:        st.Links,
		inFlight:     map[string]bool{},
		allowInvites: invites,
		transport:    transport,
		store:        store,
	}
}

// ---------------------------------------------------------------------------
// Locking & persistence
// ---------------------------------------------------------------------------

func (n *Node) Lock()      { n.mu.Lock() }
func (n *Node) Unlock()    { n.mu.Unlock() }
func (n *Node) ID() string { return n.id }

// Commit persists the node state. Caller must hold Lock.
func (n *Node) Commit() error {
	ai := n.allowInvites
	st := nodeState{ID: n.id, Links: n.links, AllowInvites: &ai}
	if err := n.store.Save(n.id, st); err != nil {
		// Rollback from persisted state.
		if raw, lerr := n.store.Load(n.id); lerr == nil {
			var reloaded nodeState
			if json.Unmarshal(raw, &reloaded) == nil {
				if reloaded.Links != nil {
					n.links = reloaded.Links
				}
				if reloaded.AllowInvites != nil {
					n.allowInvites = *reloaded.AllowInvites
				}
			}
		}
		return err
	}
	return nil
}

func (n *Node) Kick(peer string) { go n.sendPending(peer) }

// ---------------------------------------------------------------------------
// Incoming sync
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect).
func (n *Node) QuickReject(from, method string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.links[from] == nil && method != "link.connect"
}

// HandleSync processes an incoming sync request. The caller is responsible
// for authentication (signature verification).
func (n *Node) HandleSync(env SyncRequest) (*SyncResponse, error) {
	n.mu.Lock()

	link := n.links[env.From]
	if link == nil {
		if env.Method != "link.connect" {
			n.mu.Unlock()
			return nil, ErrNoLink
		}
		if !n.allowInvites {
			n.mu.Unlock()
			return nil, ErrInvitesDisabled
		}
		if len(n.links) >= MaxLinks {
			n.mu.Unlock()
			return nil, ErrMaxLinks
		}
		link = NewLink(n.id, env.From)
		link.State = Requested
		n.links[env.From] = link
	}

	collision := link.Pending != nil

	// Piggybacked response.
	if env.Resp != nil && link.Pending != nil && env.Resp.Counter == link.Counter {
		n.handleCoreSyncResponse(env.From, link, link.Pending.Method, link.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		link.Counter++
		link.RespTimestamp = env.Resp.Timestamp
		link.Pending = nil
		if n.links[env.From] != nil {
			if msg := n.nextTx(env.From); msg != nil {
				link.Pending = msg
			}
		}
		if err := n.Commit(); err != nil {
			n.mu.Unlock()
			return nil, ErrPersistFailed
		}
		collision = false
	}

	// Counter checks.
	if env.Counter < link.Counter {
		if cr := link.CachedResponse; cr != nil && cr.Counter == env.Counter {
			resp := *cr
			n.mu.Unlock()
			return &resp, nil
		}
		n.mu.Unlock()
		return nil, ErrStaleCounter
	}
	if env.Counter > link.Counter {
		n.mu.Unlock()
		return nil, ErrCounterAhead
	}
	if collision && link.Dominant() {
		n.mu.Unlock()
		return nil, ErrCollision
	}

	ts := env.Timestamp
	if link.Dominant() {
		ts = NowSecs()
	}

	// Dispatch.
	if !coreSyncMethods[env.Method] {
		n.mu.Unlock()
		return nil, ErrBadMethod
	}
	ok, reqErr := n.handleCoreSyncRequest(env.From, link, env.Method, env.Payload, ts)
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}

	link.Counter++
	link.ReqTimestamp = ts
	if collision {
		link.Pending = nil
	}

	resp := SyncResponse{Counter: env.Counter, OK: ok, Timestamp: ts}
	link.CachedResponse = &resp
	if err := n.Commit(); err != nil {
		n.mu.Unlock()
		return nil, ErrPersistFailed
	}
	n.mu.Unlock()
	n.Kick(env.From)
	return &resp, nil
}

// ---------------------------------------------------------------------------
// Outgoing sync
// ---------------------------------------------------------------------------

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	link := n.links[peer]
	if link == nil || n.inFlight[peer] {
		n.mu.Unlock()
		return
	}
	promoted := false
	if link.Pending == nil {
		msg := n.nextTx(peer)
		if msg == nil {
			n.mu.Unlock()
			return
		}
		link.Pending = msg
		promoted = true
	}
	n.inFlight[peer] = true
	msg := link.Pending
	counter := link.Counter
	env := SyncRequest{
		From: n.id, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
		Timestamp: NowSecs(),
	}
	if cr := link.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		if err := n.Commit(); err != nil {
			delete(n.inFlight, peer)
			n.mu.Unlock()
			return
		}
	}
	n.mu.Unlock()

	status, body, err := n.transport.Send(PeerDomain(peer), env)

	n.mu.Lock()
	delete(n.inFlight, peer)
	if err != nil || status != http.StatusOK {
		if status == http.StatusBadRequest {
			log.Printf("[link] %s->%s %s: 400", n.id, peer, msg.Method)
		}
		link = n.links[peer]
		requeue := status == http.StatusConflict && link != nil && link.Pending == nil
		n.mu.Unlock()
		if requeue {
			n.Kick(peer)
		}
		return
	}
	var resp SyncResponse
	if json.Unmarshal(body, &resp) != nil {
		n.mu.Unlock()
		return
	}
	link = n.links[peer]
	if link == nil || link.Pending != msg || link.Counter != counter || resp.Counter != counter {
		n.mu.Unlock()
		return
	}

	n.handleCoreSyncResponse(peer, link, msg.Method, msg.Payload, resp.OK, resp.Timestamp)
	link.Counter++
	link.RespTimestamp = resp.Timestamp
	link.Pending = nil

	var kickPeer bool
	if n.links[peer] != nil {
		if next := n.nextTx(peer); next != nil {
			link.Pending = next
			kickPeer = true
		}
	}
	if err := n.Commit(); err != nil {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	if kickPeer {
		n.Kick(peer)
	}
}

// RetryPending kicks all peers that have pending messages or are in
// lifecycle states and aren't already in flight.
func (n *Node) RetryPending() {
	n.mu.Lock()
	var kicks []string
	for peer, l := range n.links {
		if n.inFlight[peer] {
			continue
		}
		if l.Pending != nil {
			kicks = append(kicks, peer)
			continue
		}
		switch l.State {
		case Connecting, Rejecting, Disconnecting:
			kicks = append(kicks, peer)
		}
	}
	n.mu.Unlock()
	for _, p := range kicks {
		n.Kick(p)
	}
}

// ---------------------------------------------------------------------------
// Account operations (caller must hold Lock)
// ---------------------------------------------------------------------------

func (n *Node) AddLink(peer string) error {
	if _, _, err := ParseIdentity(peer); err != nil {
		return err
	}
	peer = strings.ToLower(peer)
	if peer == n.id {
		return fmt.Errorf("cannot link to self")
	}
	if n.links[peer] != nil {
		return fmt.Errorf("link to %s already exists", peer)
	}
	if len(n.links) >= MaxLinks {
		return fmt.Errorf("max links reached")
	}
	n.links[peer] = NewLink(n.id, peer)
	n.links[peer].State = Connecting
	n.Kick(peer)
	return nil
}

func (n *Node) RemoveLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.State == Connected {
		link.State = Disconnecting
	} else {
		delete(n.links, peer)
	}
	n.Kick(peer)
	return nil
}

func (n *Node) AcceptLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.State != Requested {
		return fmt.Errorf("link to %s is not requested", peer)
	}
	link.State = Connecting
	n.Kick(peer)
	return nil
}

func (n *Node) RejectLink(peer string) error {
	peer = strings.ToLower(peer)
	link := n.links[peer]
	if link == nil {
		return fmt.Errorf("no link to %s", peer)
	}
	if link.State != Requested {
		return fmt.Errorf("link to %s is not requested", peer)
	}
	link.State = Rejecting
	n.Kick(peer)
	return nil
}

func (n *Node) SetAllowInvites(enabled bool) {
	n.allowInvites = enabled
}

func (n *Node) Status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.links {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	return Enc(map[string]any{
		"links":         links,
		"allow_invites": n.allowInvites,
	})
}
