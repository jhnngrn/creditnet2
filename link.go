package creditnet

import (
	"encoding/json"
	"fmt"

	"creditnet/link"
)

// ---------------------------------------------------------------------------
// Link account operations (dispatch stays here, logic moved to link package)
// ---------------------------------------------------------------------------

func (n *Node) handleLinkAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
	switch method {
	case "set_invite_mode":
		var p struct {
			Mode link.InviteMode `json:"mode"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case link.InviteModeClosed, link.InviteModeInvite, link.InviteModeOpen:
		default:
			return nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		n.SetInviteMode(p.Mode)
		return Enc("ok"), nil
	case "set_modules":
		return n.setModules(payload)
	case "accept_module":
		return n.acceptModule(payload)
	case "reject_module":
		return n.rejectModule(payload)
	case "status":
		return n.Status(), nil
	}
	return nil, ErrBadMethod
}

func (n *Node) setModules(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := n.links.Get(p.Peer)
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	lk.SetModules(p.Modules, n.supported)
	n.Kick(p.Peer)
	return Enc("ok"), nil
}

func (n *Node) acceptModule(payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Peer   string `json:"peer"`
		Module string `json:"module"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad request")
	}
	lk := n.links.Get(p.Peer)
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	if err := lk.AcceptModule(p.Module); err != nil {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
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
	lk := n.links.Get(p.Peer)
	if lk == nil {
		return nil, fmt.Errorf("no link to %s", p.Peer)
	}
	if err := lk.RejectModule(p.Module); err != nil {
		return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
	}
	return Enc("ok"), nil
}
