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
// Interfaces
// ---------------------------------------------------------------------------

// BandwidthFunc returns available (in, out) capacity for a peer,
// accounting for effective balance, credit limits, and all reservations.
type BandwidthFunc func(peer string) (in, out int64)

// Reserver reports capacity reserved by a module on a given peer.
// Modules that hold temporary reservations (e.g. in-flight ripple hops)
// implement this so the bandwidth computation can subtract them.
type Reserver interface {
	Reserved(peer string) (in, out int64)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
