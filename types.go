package creditnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

func ParseIdentity(id string) (user, domain string, err error) {
	id = strings.ToLower(id)
	i := strings.Index(id, "@")
	if i <= 0 || i == len(id)-1 {
		return "", "", fmt.Errorf("invalid identity %q (want user@domain)", id)
	}
	user, domain = id[:i], id[i+1:]
	if !ValidUsername(user) {
		return "", "", fmt.Errorf("invalid username %q", user)
	}
	return user, domain, nil
}

func ValidUsername(u string) bool {
	if len(u) == 0 || len(u) > 64 {
		return false
	}
	for _, c := range u {
		if !(c == '_' || c == '-' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func PeerDomain(peer string) string {
	_, d, err := ParseIdentity(peer)
	if err != nil {
		return ""
	}
	return d
}

func IdentityInDomain(identity, domain string) bool {
	_, d, err := ParseIdentity(identity)
	return err == nil && d == domain
}

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// ScheduleFunc schedules a callback to run after a delay.
// The node runs the callback under its own lock, commits, and kicks
// any peers returned by the callback.
type ScheduleFunc func(delay time.Duration, fn func() []string)

// BandwidthFunc returns available (in, out) capacity for a peer,
// accounting for effective balance, credit limits, and all reservations.
type BandwidthFunc func(peer string) (in, out int64)

// Module processes sync methods and account API calls for a namespace.
// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
// that need kicking on other links (cross-link kicks, e.g. novation routing).
// HandleResponse returns kicks for the same purpose.
// HandleAccount handles user-facing API calls (via /api/account).
type Module interface {
	Name() string
	HandleRequest(peer string, t *Trustline, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer string, t *Trustline, method string, payload json.RawMessage, ok bool, ts int64) []string
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error)
	NextTx(peer string) *link.QueuedMsg
}

// Transport sends sync messages to peers.
type Transport interface {
	Send(peerDomain string, msg link.SyncRequest) ([]byte, error)
}

// Reserver reports capacity reserved by a module on a given peer.
// Modules that hold temporary reservations (e.g. in-flight ripple hops)
// implement this so the bandwidth computation can subtract them.
type Reserver interface {
	Reserved(peer string) (in, out int64)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	ErrNoLink          = errors.New("no link")
	ErrMaxLinks        = errors.New("max links")
	ErrInvitesDisabled = errors.New("invites disabled")
	ErrBadMethod       = errors.New("unknown sync method")
	ErrNotFound        = errors.New("not found")
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
func NowSecs() int64            { return time.Now().Unix() }

func SplitMethod(method string) (ns, name string) {
	i := strings.Index(method, ".")
	if i < 0 {
		return "", method
	}
	return method[:i], method[i+1:]
}

