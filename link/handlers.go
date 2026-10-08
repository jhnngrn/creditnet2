package link

import "encoding/json"

// ---------------------------------------------------------------------------
// Core sync methods: link.connect, link.disconnect, link.reject
// ---------------------------------------------------------------------------

var syncMethods = map[string]bool{
	"link.connect":    true,
	"link.disconnect": true,
	"link.reject":     true,
}

func (n *Node) handleRequest(peer string, link *Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "link.connect":
		switch link.State {
		case Connecting, Waiting:
			link.State = Connected
			return true, nil
		default:
			return false, nil
		}

	case "link.reject":
		delete(n.links, peer)
		return true, nil

	case "link.disconnect":
		delete(n.links, peer)
		return true, nil
	}
	return false, nil
}

func (n *Node) handleResponse(peer string, link *Link, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "link.connect":
		if ok {
			link.State = Connected
		} else {
			link.State = Waiting
		}

	case "link.reject":
		// Our rejection was ACK'd — clean up.
		delete(n.links, peer)

	case "link.disconnect":
		// Our disconnect was ACK'd — clean up.
		delete(n.links, peer)
	}
}

// ---------------------------------------------------------------------------
// Promote: next message to send for a peer
// ---------------------------------------------------------------------------

func (n *Node) nextTx(peer string) *QueuedMsg {
	link := n.links[peer]
	if link == nil {
		return nil
	}

	switch link.State {
	case Connecting:
		return &QueuedMsg{Method: "link.connect"}
	case Rejecting:
		return &QueuedMsg{Method: "link.reject"}
	case Disconnecting:
		return &QueuedMsg{Method: "link.disconnect"}
	default:
		return nil
	}
}
