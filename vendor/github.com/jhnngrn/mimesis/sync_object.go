package mimesis

import "encoding/json"

// SyncObject wraps module-owned data with a framework-managed outbox queue.
//
// Each SyncObject has a queue ID (chosen by the module, e.g. a novation ID)
// and delegates Send/DeleteQueue/HasPending to the framework via ModuleAPI.
// The module never sees raw queue IDs in its handlers — they are internal
// to the SyncObject.
//
// Go has no destructors, so the module must call Close() when removing a
// SyncObject to clean up any remaining outbox entries.
type SyncObject[T any] struct {
	Data    T      `json:"data"`
	queueID string
	api     *ModuleAPI
}

// NewSyncObject creates a SyncObject with the given data, queue ID, and API.
// The queue ID is typically the key used to store this object in the module's
// map (e.g. the novation ID).
func NewSyncObject[T any](data T, queueID string, api *ModuleAPI) *SyncObject[T] {
	return &SyncObject[T]{
		Data:    data,
		queueID: queueID,
		api:     api,
	}
}

// QueueID returns the queue identifier for this SyncObject.
func (so *SyncObject[T]) QueueID() string {
	return so.queueID
}

// Send enqueues an outgoing message for the given peer.
func (so *SyncObject[T]) Send(peer, method string, payload json.RawMessage) {
	so.api.Send(peer, so.queueID, method, payload)
}

// DeleteQueue removes all queued outbox entries for the given peer.
func (so *SyncObject[T]) DeleteQueue(peer string) {
	so.api.DeleteQueue(peer, so.queueID)
}

// HasPending reports whether the outbox has an entry for this queue and peer.
func (so *SyncObject[T]) HasPending(peer string) bool {
	return so.api.QueueHas(peer, so.queueID)
}

// QueueExists reports whether any peer has an outbox entry for this queue.
func (so *SyncObject[T]) QueueExists() bool {
	return so.api.QueueExists(so.queueID)
}

// QueuePeers returns the set of peers that have an outbox entry for this queue.
func (so *SyncObject[T]) QueuePeers() map[string]bool {
	return so.api.QueuePeers(so.queueID)
}

// Close cleans up outbox entries for all peers that have entries for this
// queue. Call this before removing the SyncObject from the module's map.
// Go has no destructors, so this must be called explicitly.
func (so *SyncObject[T]) Close() {
	peers := so.api.QueuePeers(so.queueID)
	for peer := range peers {
		so.api.DeleteQueue(peer, so.queueID)
	}
}

// Reattach reconnects the SyncObject to a ModuleAPI after deserialization.
// Called by the module's reattach/reload logic.
func (so *SyncObject[T]) Reattach(queueID string, api *ModuleAPI) {
	so.queueID = queueID
	so.api = api
}
