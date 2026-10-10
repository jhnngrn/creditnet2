// Package link defines the bilateral sync primitive — the data structures
// and protocol types for a single link between two parties.
package link

import "encoding/json"

// ---------------------------------------------------------------------------
// Link — bilateral sync state
// ---------------------------------------------------------------------------

// LinkStatus — what the link IS.
type LinkStatus string

const (
	Waiting   LinkStatus = "waiting"   // exists, not yet connected
	Incoming  LinkStatus = "incoming"  // peer initiated, awaiting local decision
	Connected LinkStatus = "connected" // bilaterally established
)

// LinkState combines what the link is with what it wants to send.
type LinkState struct {
	Status LinkStatus `json:"status"`
	Queue  []string   `json:"queue,omitempty"`
}

// QueuedMsg is a message queued for sending on a link.
type QueuedMsg struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState       `json:"state"`
	Counter        uint64          `json:"counter"`
	TurnBit        uint64          `json:"turn_bit"`
	Modules        map[string]bool `json:"modules,omitempty"` // false=pending, true=active
	Pending        *QueuedMsg      `json:"pending,omitempty"`
	CachedResponse *SyncResponse   `json:"cached_response,omitempty"`
	ReqTimestamp   int64           `json:"req_ts"`
	RespTimestamp  int64           `json:"resp_ts"`
}

// ModuleEnabled returns true if the named module is bilaterally active on this link.
func (l *Link) ModuleEnabled(name string) bool {
	return l.Modules[name] // true only if present and true
}

// Dominant returns true when this side owns the current counter slot.
func (l *Link) Dominant() bool { return l.Counter%2 == l.TurnBit }

// NewLink creates a Link with the turn bit derived from identity ordering.
func NewLink(self, peer string) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{TurnBit: bit}
}

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

// SyncRequest is the wire envelope for an outgoing sync message.
type SyncRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *SyncResponse   `json:"resp,omitempty"`
}

// SyncResponse is the wire envelope for a sync response.
type SyncResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Queue helpers
// ---------------------------------------------------------------------------

func Enqueue(q *[]string, method string) { *q = append(*q, method) }
func Dequeue(q *[]string)                { if len(*q) > 0 { *q = (*q)[1:] } }
func QueueHead(q []string) string        { if len(q) > 0 { return q[0] }; return "" }
