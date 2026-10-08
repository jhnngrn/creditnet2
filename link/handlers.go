package link

import "encoding/json"

// ---------------------------------------------------------------------------
// linkHandler — handles link.connect, link.disconnect, link.reject
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
		switch lk.State {
		case Connecting, Waiting:
			lk.State = Connected
			return true, nil
		default:
			return false, nil
		}

	case "reject":
		delete(h.node.links, peer)
		return true, nil

	case "disconnect":
		delete(h.node.links, peer)
		return true, nil
	}
	return false, ErrBadMethod
}

func (h *linkHandler) HandleResponse(peer string, lk *Link, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "connect":
		if ok {
			lk.State = Connected
		} else {
			lk.State = Waiting
		}

	case "reject":
		delete(h.node.links, peer)

	case "disconnect":
		delete(h.node.links, peer)
	}
}

func (h *linkHandler) NextTx(peer string, lk *Link) *QueuedMsg {
	switch lk.State {
	case Connecting:
		return &QueuedMsg{Method: "connect"}
	case Rejecting:
		return &QueuedMsg{Method: "reject"}
	case Disconnecting:
		return &QueuedMsg{Method: "disconnect"}
	}
	return nil
}

func (h *linkHandler) ResetState(json.RawMessage) {}
