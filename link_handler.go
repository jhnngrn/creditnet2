package creditnet

import (
	"encoding/json"
	"fmt"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// Link handling — connect, close, module negotiation (built into Node)
// ---------------------------------------------------------------------------

func (n *Node) handleLinkRequest(peer string, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	lk := n.links[peer]
	switch method {
	case "connect":
		if lk.State.IntentConnect || lk.State.Status == link.Waiting {
			lk.State.Status = link.Connected
			lk.State.IntentConnect = false
			return true, nil, nil
		}
		if n.inviteMode == InviteModeOpen {
			lk.State.Status = link.Connected
			return true, nil, nil
		}
		return false, nil, nil // Incoming → NACK (invite flow)

	case "close":
		n.DeleteLink(peer)
		return true, nil, nil

	case "module":
		return n.reqModule(peer, lk, payload)
	}
	return false, nil, ErrBadMethod
}

func (n *Node) handleLinkResponse(peer string, method string, payload json.RawMessage, ok bool, ts int64) []string {
	lk := n.links[peer]
	switch method {
	case "connect":
		if ok {
			lk.State.Status = link.Connected
		} else {
			lk.State.Status = link.Waiting
		}
		lk.State.IntentConnect = false

	case "close":
		if ok {
			n.DeleteLink(peer)
			return nil
		}
		lk.State.IntentClose = false

	case "module":
		n.respModule(lk, payload, ok)
	}
	return nil
}

func (n *Node) linkNextTx(peer string) *link.QueuedMsg {
	lk := n.links[peer]
	// Link lifecycle.
	if lk.State.IntentClose && lk.State.Refs == 0 {
		return &link.QueuedMsg{Method: "close"}
	}
	if lk.State.IntentConnect {
		return &link.QueuedMsg{Method: "connect"}
	}
	// Module proposals (only when connected).
	if lk.State.Status == link.Connected {
		for name, mod := range lk.State.Modules {
			if mod.IntentConnect {
				return &link.QueuedMsg{
					Method:  "module",
					Payload: Enc(map[string]string{"name": name}),
				}
			}
		}
	}
	return nil
}

func (n *Node) handleLinkAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "set_invite_mode":
		var p struct {
			Mode InviteMode `json:"mode"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case InviteModeClosed, InviteModeInvite, InviteModeOpen:
		default:
			return nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		n.SetInviteMode(p.Mode)
		return Enc("ok"), nil
	case "accept_module":
		return n.acceptModule(payload)
	case "reject_module":
		return n.rejectModule(payload)
	case "status":
		return n.Status(), nil
	}
	return nil, ErrBadMethod
}

// ---------------------------------------------------------------------------
// Module negotiation
// ---------------------------------------------------------------------------

func (n *Node) reqModule(peer string, lk *link.Link, payload json.RawMessage) (bool, []string, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, nil, fmt.Errorf("bad payload")
	}
	if !n.ModuleSupported(p.Name) {
		return false, nil, nil // we don't speak this module
	}
	if lk.State.Modules == nil {
		lk.State.Modules = map[string]*link.ConnectionState{}
	}
	mod := lk.State.Modules[p.Name]
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == link.Waiting) {
		mod.Status = link.Connected
		mod.IntentConnect = false
		return true, nil, nil
	}
	// Open mode — auto-accept supported modules.
	if n.inviteMode == InviteModeOpen {
		if mod == nil {
			mod = &link.ConnectionState{}
			lk.State.Modules[p.Name] = mod
		}
		mod.Status = link.Connected
		return true, nil, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	if mod == nil {
		mod = &link.ConnectionState{}
		lk.State.Modules[p.Name] = mod
	}
	mod.Status = link.Incoming
	return false, nil, nil // NACK — not accepted yet
}

func (n *Node) respModule(lk *link.Link, payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	mod := lk.State.Modules[p.Name]
	if mod == nil {
		return
	}
	if ok {
		mod.Status = link.Connected
	} else {
		mod.Status = link.Waiting
	}
	mod.IntentConnect = false
}

func (n *Node) acceptModule(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Module string `json:"module"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := n.links[p.Peer]
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	mod := lk.State.Modules[p.Module]
	if mod == nil || mod.Status != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	mod.IntentConnect = true
	n.Kick(p.Peer)
	return Enc("ok"), nil
}

func (n *Node) rejectModule(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Module string `json:"module"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := n.links[p.Peer]
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	mod := lk.State.Modules[p.Module]
	if mod == nil || mod.Status != link.Incoming {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	delete(lk.State.Modules, p.Module)
	return Enc("ok"), nil
}
