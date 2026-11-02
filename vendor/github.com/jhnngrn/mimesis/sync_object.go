package mimesis

import "encoding/json"

// SyncObject is embedded in module-owned structs to give them a
// framework-managed outbox queue. Each SyncObject is a 1:1 relationship
// with a peer — the Peer field identifies who this object syncs with,
// and Send() uses it automatically.
//
//	type Trustline struct {
//	    mimesis.SyncObject
//	    CreditLimit int64 `json:"credit_limit"`
//	}
//
// The framework assigns an internal uint64 ID — modules never manage IDs.
// Go has no destructors, so the module must call Close() before removing
// a SyncObject to clean up any remaining outbox entries.
type SyncObject struct {
	ID   uint64 `json:"_id,omitempty"`
	Peer string `json:"peer,omitempty"`

	// Closures — set by NewSyncObject/Reattach, capture module name + node.
	send        func(method string, payload json.RawMessage)
	connect     func()
	disconnect  func()
	deleteQueue func()
	hasPending  func() bool
	connected   bool
}

// Send enqueues an outgoing message to this object's peer.
func (so *SyncObject) Send(method string, payload json.RawMessage) {
	if so.send != nil {
		so.send(method, payload)
	}
}

// DeleteQueue removes all queued outbox entries for this sync object.
func (so *SyncObject) DeleteQueue() {
	if so.deleteQueue != nil {
		so.deleteQueue()
	}
}

// HasPending reports whether the outbox has entries for this sync object.
func (so *SyncObject) HasPending() bool {
	if so.hasPending != nil {
		return so.hasPending()
	}
	return false
}

// Connect marks this SyncObject as bilaterally connected, taking a ref
// on the module's ConnectionState. Call once both sides have confirmed
// (e.g. in HandleResponse after ack, or in HandleRequest when acking).
// Close releases the ref.
func (so *SyncObject) Connect() {
	if so.connect != nil && !so.connected {
		so.connected = true
		so.connect()
	}
}

// Close cleans up outbox entries and releases the ref if connected.
func (so *SyncObject) Close() {
	if so.connected {
		so.connected = false
		if so.disconnect != nil {
			so.disconnect()
		}
	}
	so.DeleteQueue()
}
