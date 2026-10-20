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

// Barter is the single-hop payment interface. Both the barter module
// (simple payments) and the demurrage module (payments with decay)
// implement this. Triad/loops use it without knowing which is active.
type Barter interface {
	Bandwidth(peer string, side int) int64
	PendingBalance(peer string, side int) int64
	Pay(peer string, amount int64, ts int64) (bool, error)
	AdjustBalance(peer string, delta int64, ts int64)
	RegisterReserver(r Reserver)
}

// Reserver reports capacity reserved by a module on a given peer/side.
// Modules that hold temporary reservations (e.g. in-flight clearings)
// implement this so the bandwidth computation can subtract them.
type Reserver interface {
	Reserved(peer string, side int) int64
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
