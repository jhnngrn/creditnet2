package creditnet

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

func newLinkHandler(n *Node) *linkHandler {
	return &linkHandler{node: n}
}

func (h *linkHandler) HandleRequest(peer string, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
	lk := h.node.links[peer]
	switch method {
	case "connect":
		if lk.State.Status == Waiting {
			lk.State.Status = Connected
			if QueueHead(lk.State.Queue) == "connect" {
				Dequeue(&lk.State.Queue) // mutual connect — no need to send own
			}
			h.node.NotifyLinkConnected(peer)
			return true, nil, nil
		}
		return false, nil, nil // Incoming → NACK (invite flow)

	case "close":
		h.node.DeleteLink(peer)
		return true, nil, nil
	}
	return false, nil, ErrBadMethod
}

func (h *linkHandler) HandleResponse(peer string, method string, payload json.RawMessage, ok bool, ts int64) []string {
	lk := h.node.links[peer]
	switch method {
	case "connect":
		if ok {
			lk.State.Status = Connected
			h.node.NotifyLinkConnected(peer)
		}
		// !ok: stays Waiting, peer has incoming

	case "close":
		if ok {
			h.node.DeleteLink(peer)
			return nil
		}
		// !ok: refs > 0 on peer side
	}
	Dequeue(&lk.State.Queue)
	return nil
}

func (h *linkHandler) NextTx(peer string) *QueuedMsg {
	lk := h.node.links[peer]
	if method := QueueHead(lk.State.Queue); method != "" {
		msg := &QueuedMsg{Method: method}
		if method == "connect" && lk.Owner != "" {
			msg.Payload = Enc(map[string]string{"protocol": lk.Owner})
		}
		return msg
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
	case "status":
		return h.node.Status(), nil
	}
	return nil, ErrBadMethod
}
