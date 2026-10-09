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
			if QueueHead(lk.State.Queue) == "connect" {
				Dequeue(&lk.State.Queue) // mutual connect — no need to send own
			}
			return true, nil
		}
		return false, nil // Incoming → NACK (invite flow)

	case "close":
		if lk.InUse {
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
	Dequeue(&lk.State.Queue)
}

func (h *linkHandler) NextTx(peer string, lk *Link) *QueuedMsg {
	if method := QueueHead(lk.State.Queue); method != "" {
		return &QueuedMsg{Method: method}
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
