package mimesis

import "encoding/json"

// SyncObject is embedded in module-owned structs to give them a
// framework-managed outbox queue. The framework assigns an internal
// uint64 queue ID — modules never see or manage queue IDs.
//
//	type Novation struct {
//	    mimesis.SyncObject
//	    Amount int64     `json:"amount"`
//	    Side   [2]string `json:"side"`
//	}
//
// The embedded SyncObject provides Send, DeleteQueue, HasPending,
// QueueExists, QueuePeers, and Close methods directly on the struct.
//
// Go has no destructors, so the module must call Close() before
// removing a SyncObject to clean up any remaining outbox entries.
type SyncObject struct {
	QID uint64 `json:"_qid,omitempty"`

	// Closures — set by NewQueue/Reattach, capture module name + node.
	send        func(peer, method string, payload json.RawMessage)
	deleteQueue func(peer string)
	hasPending  func(peer string) bool
	queueExists func() bool
	queuePeers  func() map[string]bool
}

// Send enqueues an outgoing message for the given peer.
func (so *SyncObject) Send(peer, method string, payload json.RawMessage) {
	if so.send != nil {
		so.send(peer, method, payload)
	}
}

// DeleteQueue removes all queued outbox entries for the given peer.
func (so *SyncObject) DeleteQueue(peer string) {
	if so.deleteQueue != nil {
		so.deleteQueue(peer)
	}
}

// HasPending reports whether the outbox has an entry for this queue and peer.
func (so *SyncObject) HasPending(peer string) bool {
	if so.hasPending != nil {
		return so.hasPending(peer)
	}
	return false
}

// QueueExists reports whether any peer has an outbox entry for this queue.
func (so *SyncObject) QueueExists() bool {
	if so.queueExists != nil {
		return so.queueExists()
	}
	return false
}

// QueuePeers returns the set of peers that have an outbox entry for this queue.
func (so *SyncObject) QueuePeers() map[string]bool {
	if so.queuePeers != nil {
		return so.queuePeers()
	}
	return nil
}

// Close cleans up outbox entries for all peers. Call before removing
// the struct from the module's map.
func (so *SyncObject) Close() {
	peers := so.QueuePeers()
	for peer := range peers {
		so.DeleteQueue(peer)
	}
}
