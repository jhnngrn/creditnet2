// Package link defines the bilateral sync primitive — the data structures
// and protocol types for a single link between two parties.
package link

import (
	"encoding/json"
	"errors"
)

// ---------------------------------------------------------------------------
// Errors — link-level protocol errors
// ---------------------------------------------------------------------------

var (
	ErrStaleCounter = errors.New("stale counter")
	ErrCounterAhead = errors.New("counter ahead")
	ErrCollision    = errors.New("collision")
)

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
	Status      LinkStatus            `json:"status"`
	Queue       []string              `json:"queue,omitempty"`
	ModuleQueue []string              `json:"module_queue,omitempty"`
	Modules     map[string]LinkStatus `json:"modules,omitempty"`
}

// QueuedMsg is a message queued for sending on a link.
type QueuedMsg struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *SyncResponse `json:"cached_response,omitempty"`
	ReqTimestamp   int64         `json:"req_ts"`
	RespTimestamp  int64         `json:"resp_ts"`
}

// ModuleEnabled returns true if the named module is bilaterally active on this link.
func (l *Link) ModuleEnabled(name string) bool {
	return l.State.Modules[name] == Connected
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
// Sync lifecycle
// ---------------------------------------------------------------------------

// ValidateRequest checks whether an incoming request at the given counter
// is valid for this link. Returns:
//   - (*SyncResponse, nil) — stale counter but cached response exists
//   - (nil, error) — invalid (stale without cache, ahead, or collision)
//   - (nil, nil) — valid, proceed with dispatch
func (l *Link) ValidateRequest(counter uint64) (*SyncResponse, error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && l.CachedResponse.Counter == counter {
			resp := *l.CachedResponse
			return &resp, nil
		}
		return nil, ErrStaleCounter
	}
	if counter > l.Counter {
		return nil, ErrCounterAhead
	}
	if l.Pending != nil && l.Dominant() {
		return nil, ErrCollision
	}
	return nil, nil
}

// AcceptRequest finalizes an incoming request. Call after the handler
// has processed the request. Returns the response to send back.
func (l *Link) AcceptRequest(reqCounter uint64, ok bool, ts int64) *SyncResponse {
	resp := SyncResponse{Counter: reqCounter, OK: ok, Timestamp: ts}
	l.CachedResponse = &resp
	l.Counter++
	l.ReqTimestamp = ts
	l.Pending = nil
	return &resp
}

// AcceptResponse finalizes a received response to our outgoing message.
// Call after the response handler has processed the response.
func (l *Link) AcceptResponse(ts int64) {
	l.Counter++
	l.RespTimestamp = ts
	l.Pending = nil
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
