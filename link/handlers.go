package link

import "encoding/json"

// ---------------------------------------------------------------------------
// Core sync methods: link.connect, link.disconnect, link.reject
// ---------------------------------------------------------------------------

var coreSyncMethods = map[string]bool{
	"link.connect":    true,
	"link.disconnect": true,
	"link.reject":     true,
}

func (n *Node) handleCoreSyncRequest(peer string, link *Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "link.connect":
		switch link.State {
		case Connecting, Pending:
			// Mutual add — both sides initiated. Accept.
			link.State = Connected
			return true, nil
		case Connected:
			// Already connected — idempotent ACK.
			return true, nil
		default:
			// Requested, rejecting, disconnecting — don't accept yet.
			return false, nil
		}

	case "link.reject":
		// Peer rejected our link.connect — clean up.
		delete(n.links, peer)
		return true, nil

	case "link.disconnect":
		// Peer disconnecting — clean up.
		delete(n.links, peer)
		return true, nil
	}
	return false, nil
}

func (n *Node) handleCoreSyncResponse(peer string, link *Link, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "link.connect":
		if ok {
			link.State = Connected
		} else {
			link.State = Pending
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
