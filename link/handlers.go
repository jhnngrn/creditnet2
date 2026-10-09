package link

import (
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// linkHandler — handles link.connect, link.close
// ---------------------------------------------------------------------------

type linkHandler struct {
	node *Node
}

// NewLinkHandler creates a Handler for the "link" namespace.
func NewLinkHandler(n *Node) Handler {
	return &linkHandler{node: n}
}

func (h *linkHandler) HandleRequest(peer string, lk *Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "connect":
		if lk.State.Status == Waiting {
			lk.State.Status = Connected
			if lk.State.Intent == IntentConnect {
				lk.State.Intent = "" // mutual connect — no need to send own
			}
			return true, nil
		}
		return false, nil // Incoming → NACK (invite flow)

	case "close":
		if lk.Refs > 0 {
			return false, nil
		}
		h.node.DeleteLink(peer)
		return true, nil
	}
	return false, ErrBadMethod
}

func (h *linkHandler) HandleResponse(peer string, lk *Link, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "connect":
		if ok {
			lk.State.Status = Connected
		}
		// !ok: stays Waiting, peer has incoming

	case "close":
		if ok {
			h.node.DeleteLink(peer)
		}
		// !ok: refs > 0 on peer side
	}
	lk.State.Intent = ""
}

func (h *linkHandler) NextTx(peer string, lk *Link) *QueuedMsg {
	switch lk.State.Intent {
	case IntentConnect:
		return &QueuedMsg{Method: "connect"}
	case IntentClose:
		return &QueuedMsg{Method: "close"}
	}
	return nil
}

func (h *linkHandler) ResetState(json.RawMessage) {}

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
	case "accept_link":
		var p struct {
			Peer string `json:"peer"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		if err := h.node.AcceptLink(p.Peer); err != nil {
			return nil, err
		}
		return Enc("ok"), nil
	case "reject_link":
		var p struct {
			Peer string `json:"peer"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("bad request")
		}
		if err := h.node.RejectLink(p.Peer); err != nil {
			return nil, err
		}
		return Enc("ok"), nil
	case "status":
		return h.node.Status(), nil
	}
	return nil, ErrBadMethod
}
