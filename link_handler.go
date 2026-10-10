package creditnet

import (
	"encoding/json"
	"fmt"
	"strings"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// linkHandler — handles link.connect, link.close, link.module
// ---------------------------------------------------------------------------

type linkHandler struct {
	node *Node
}

func newLinkHandler(n *Node) *linkHandler {
	return &linkHandler{node: n}
}

func (h *linkHandler) HandleRequest(peer string, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	lk := h.node.links[peer]
	switch method {
	case "connect":
		if lk.State.Status == link.Waiting {
			lk.State.Status = link.Connected
			if link.QueueHead(lk.State.Queue) == "connect" {
				link.Dequeue(&lk.State.Queue) // mutual connect — no need to send own
			}
			return true, nil, nil
		}
		return false, nil, nil // Incoming → NACK (invite flow)

	case "close":
		h.node.DeleteLink(peer)
		return true, nil, nil

	case "module":
		return h.reqModule(peer, lk, payload)
	}
	return false, nil, ErrBadMethod
}

func (h *linkHandler) HandleResponse(peer string, method string, payload json.RawMessage, ok bool, ts int64) []string {
	lk := h.node.links[peer]
	switch method {
	case "connect":
		if ok {
			lk.State.Status = link.Connected
		}
		// !ok: stays Waiting, peer has incoming

	case "close":
		if ok {
			h.node.DeleteLink(peer)
			return nil
		}
		// !ok: refs > 0 on peer side

	case "module":
		h.respModule(lk, payload, ok)
	}
	link.Dequeue(&lk.State.Queue)
	return nil
}

func (h *linkHandler) NextTx(peer string) *link.QueuedMsg {
	lk := h.node.links[peer]
	method := link.QueueHead(lk.State.Queue)
	if method == "" {
		return nil
	}
	// Module proposal from queue (e.g. "module:trustline").
	if name, ok := strings.CutPrefix(method, "module:"); ok {
		return &link.QueuedMsg{
			Method:  "module",
			Payload: Enc(map[string]string{"name": name}),
		}
	}
	// Link queue (connect/close).
	return &link.QueuedMsg{Method: method}
}

func (h *linkHandler) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "set_allow_invites":
		var p struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		h.node.SetAllowInvites(p.Enabled)
		return Enc("ok"), nil
	case "accept_module":
		return h.acceptModule(payload)
	case "reject_module":
		return h.rejectModule(payload)
	case "status":
		return h.node.Status(), nil
	}
	return nil, ErrBadMethod
}

// ---------------------------------------------------------------------------
// Module negotiation
// ---------------------------------------------------------------------------

func (h *linkHandler) reqModule(peer string, lk *link.Link, payload json.RawMessage) (bool, []string, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, nil, fmt.Errorf("bad payload")
	}
	if _, supported := h.node.moduleMap[p.Name]; !supported {
		return false, nil, nil // we don't speak this module
	}
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]link.LinkStatus{}
	}
	// Check if we have a pending proposal in queue (mutual).
	if h.hasQueuedModule(lk, p.Name) {
		h.dequeueModule(lk, p.Name)
		lk.State.Modules[p.Name] = link.Connected
		return true, nil, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	lk.State.Modules[p.Name] = link.Incoming
	return false, nil, nil // NACK — not accepted yet
}

func (h *linkHandler) respModule(lk *link.Link, payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]link.LinkStatus{}
	}
	if ok {
		lk.State.Modules[p.Name] = link.Connected
	} else {
		lk.State.Modules[p.Name] = link.Waiting
	}
}

func (h *linkHandler) acceptModule(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Module string `json:"module"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := h.node.links[p.Peer]
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	if lk.State.Modules[p.Module] != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	delete(lk.State.Modules, p.Module)
	link.Enqueue(&lk.State.Queue, "module:"+p.Module)
	h.node.Kick(p.Peer)
	return Enc("ok"), nil
}

func (h *linkHandler) rejectModule(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Module string `json:"module"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := h.node.links[p.Peer]
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	if lk.State.Modules[p.Module] != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	delete(lk.State.Modules, p.Module)
	return Enc("ok"), nil
}

// hasQueuedModule checks if a module proposal is pending in the link queue.
func (h *linkHandler) hasQueuedModule(lk *link.Link, name string) bool {
	entry := "module:" + name
	for _, q := range lk.State.Queue {
		if q == entry {
			return true
		}
	}
	return false
}

// dequeueModule removes a module proposal from the link queue.
func (h *linkHandler) dequeueModule(lk *link.Link, name string) {
	entry := "module:" + name
	for i, q := range lk.State.Queue {
		if q == entry {
			lk.State.Queue = append(lk.State.Queue[:i], lk.State.Queue[i+1:]...)
			return
		}
	}
}
