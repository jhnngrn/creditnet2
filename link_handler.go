package creditnet

import (
	"encoding/json"
	"fmt"

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
	// Link queue (connect/close).
	if method := link.QueueHead(lk.State.Queue); method != "" {
		return &link.QueuedMsg{Method: method}
	}
	// Module proposals — only send for modules marked as waiting.
	if lk.State.Status == link.Connected {
		for name, status := range lk.Modules {
			if status == link.Waiting {
				return &link.QueuedMsg{
					Method:  "module",
					Payload: Enc(map[string]string{"name": name}),
				}
			}
		}
	}
	return nil
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
	if lk.Modules == nil {
		lk.Modules = map[string]link.LinkStatus{}
	}
	if lk.Modules[p.Name] == link.Waiting {
		// Mutual — both sides proposed. Connect immediately.
		lk.Modules[p.Name] = link.Connected
		return true, nil, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	lk.Modules[p.Name] = link.Incoming
	return false, nil, nil // NACK — not accepted yet
}

func (h *linkHandler) respModule(lk *link.Link, payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	if ok {
		lk.Modules[p.Name] = link.Connected
	}
	// !ok: stays Waiting, peer has incoming
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
	if lk.Modules[p.Module] != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	// Set to waiting and enqueue our own proposal — mutual connect on response.
	lk.Modules[p.Module] = link.Waiting
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
	if lk.Modules[p.Module] != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	delete(lk.Modules, p.Module)
	return Enc("ok"), nil
}
