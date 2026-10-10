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
		if lk.State.Status == link.Connecting || lk.State.Status == link.Waiting {
			lk.State.Status = link.Connected
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
		} else {
			lk.State.Status = link.Waiting
		}

	case "close":
		if ok {
			h.node.DeleteLink(peer)
			return nil
		}

	case "module":
		h.respModule(lk, payload, ok)
	}
	return nil
}

func (h *linkHandler) NextTx(peer string) *link.QueuedMsg {
	lk := h.node.links[peer]
	// Link lifecycle.
	switch lk.State.Status {
	case link.Connecting:
		return &link.QueuedMsg{Method: "connect"}
	case link.Disconnecting:
		return &link.QueuedMsg{Method: "close"}
	}
	// Module proposals (only when connected).
	if lk.State.Status == link.Connected {
		for name, status := range lk.State.Modules {
			if status == link.Connecting {
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
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]link.LinkStatus{}
	}
	// Mutual — we already want this module.
	if lk.State.Modules[p.Name] == link.Connecting || lk.State.Modules[p.Name] == link.Waiting {
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
	lk.State.Modules[p.Module] = link.Connecting
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
