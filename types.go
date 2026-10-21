package creditnet

import (
	"encoding/json"

	"github.com/jhnngrn/mimesis"
)

// Re-export mimesis symbols used throughout creditnet and its modules.
var (
	ErrBadMethod = mimesis.ErrBadMethod
	ErrNotFound  = mimesis.ErrNotFound
)

// Aliases so modules don't need to import mimesis directly for these.
var ParseIdentity = mimesis.ParseIdentity

// ---------------------------------------------------------------------------
// Side constants — used by bandwidth and Reserver
// ---------------------------------------------------------------------------

const (
	IN  = 0
	OUT = 1
)

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// BandwidthFunc returns available capacity for a peer on a given side.
type BandwidthFunc func(peer string, side int) int64

// PendingBalanceFunc returns effective balance minus reservations.
// Positive means debt exists: peer owes us (OUT) or we owe them (IN).
type PendingBalanceFunc func(peer string, side int) int64

// Reserver reports capacity reserved by a module on a given peer/side.
// Modules that hold temporary reservations (e.g. in-flight clearings)
// implement this so the bandwidth computation can subtract them.
type Reserver interface {
	Reserved(peer string, side int) int64
}

// ---------------------------------------------------------------------------
// Receipts
// ---------------------------------------------------------------------------

const MaxReceipts = 64

type Receipt struct {
	Peer      string `json:"peer"`
	Amount    int64  `json:"amount"`
	Status    bool   `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
