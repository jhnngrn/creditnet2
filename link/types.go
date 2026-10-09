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

// LinkStatus — what the link IS.
type LinkStatus string

const (
	Waiting   LinkStatus = "waiting"    // exists, not yet connected
	Incoming  LinkStatus = "incoming"   // peer initiated, awaiting local decision
	Connected LinkStatus = "connected"  // bilaterally established
)

// LinkState combines what the link is with what it wants to send.
type LinkState struct {
	Status LinkStatus `json:"status"`
	Queue  []string   `json:"queue,omitempty"`
}

type QueuedMsg struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Owner          string        `json:"protocol,omitempty"` // registered LinkOwner name
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *SyncResponse `json:"cached_response,omitempty"`
	ReqTimestamp   int64         `json:"req_ts"`
	RespTimestamp  int64         `json:"resp_ts"`
}

// LinkOwner provides lifecycle callbacks for links with a given protocol.
// OnConnect is called when the link reaches Connected (constructor).
// OnClose is called before the link is deleted (destructor).
type LinkOwner struct {
	OnConnect func(peer string, lk *Link)
	OnClose   func(peer string, lk *Link)
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
// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
// that need kicking on other links (cross-link kicks, e.g. novation routing).
// HandleResponse returns kicks for the same purpose.
type Handler interface {
	HandleRequest(peer string, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer string, method string, payload json.RawMessage, ok bool, ts int64) []string
	NextTx(peer string) *QueuedMsg
	ResetState(json.RawMessage)
}

// AccountHandler processes user-facing API methods for a namespace.
// Handlers that also implement this interface receive account API calls.
type AccountHandler interface {
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error)
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

// Queue helpers

func Enqueue(q *[]string, method string) { *q = append(*q, method) }
func Dequeue(q *[]string)                { if len(*q) > 0 { *q = (*q)[1:] } }
func QueueHead(q []string) string        { if len(q) > 0 { return q[0] }; return "" }

func SplitMethod(method string) (ns, name string) {
	i := strings.Index(method, ".")
	if i < 0 {
		return "", method
	}
	return method[:i], method[i+1:]
}

const MaxLinks = 64
