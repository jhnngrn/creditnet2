package creditnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
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
	ErrBadMethod = errors.New("unknown sync method")
	ErrNotFound  = errors.New("not found")
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
func NowSecs() int64            { return time.Now().Unix() }


