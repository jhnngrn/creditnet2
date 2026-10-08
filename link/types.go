package link

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
// Wire envelopes
// ---------------------------------------------------------------------------

type SyncRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *SyncResponse   `json:"resp,omitempty"`
}

type SyncResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Link — bilateral sync primitive
// ---------------------------------------------------------------------------

type LinkState string

const (
	Waiting       LinkState = "waiting"        // connect sent, waiting for peer to connect back
	Connecting    LinkState = "connecting"     // outgoing link.connect in flight
	Incoming      LinkState = "incoming"       // incoming request, awaiting local acceptance
	Connected     LinkState = "connected"      // bilaterally established
	Rejecting     LinkState = "rejecting"      // outgoing link.reject in flight
	Disconnecting LinkState = "disconnecting"  // outgoing link.disconnect in flight
)

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
// Interfaces
// ---------------------------------------------------------------------------

// Handler processes sync methods for a namespace.
type Handler interface {
	HandleRequest(peer string, link *Link, method string, payload json.RawMessage, ts int64) (bool, error)
	HandleResponse(peer string, link *Link, method string, payload json.RawMessage, ok bool, ts int64)
	NextTx(peer string, link *Link) *QueuedMsg
	ResetState(json.RawMessage)
}

// Transport sends sync messages to peers.
type Transport interface {
	Send(peerDomain string, msg SyncRequest) ([]byte, error)
}

// NodeStore persists and loads node state.
type NodeStore interface {
	Save(id string, state any) error
	Load(id string) (json.RawMessage, error)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	ErrNoLink           = errors.New("no link")
	ErrMaxLinks         = errors.New("max links")
	ErrInvitesDisabled  = errors.New("invites disabled")
	ErrStaleCounter     = errors.New("stale counter")
	ErrCounterAhead     = errors.New("counter ahead")
	ErrCollision        = errors.New("collision")
	ErrBadMethod        = errors.New("unknown sync method")
	ErrPersistFailed    = errors.New("persist failed")
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

const MaxLinks = 64
