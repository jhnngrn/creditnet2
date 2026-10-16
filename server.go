package creditnet

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jhnngrn/link"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Domain  string
	Listen  string
	DataDir string
	Store   Store

	SessionTTL  time.Duration
	KeyCacheTTL time.Duration
	HTTPTimeout time.Duration
	PeerScheme  string

	AdminUser string
	AdminPass string
	Verbose   bool
}

func (c *Config) fill() error {
	if c.Domain == "" {
		return errors.New("Config.Domain is required")
	}
	c.Domain = strings.ToLower(c.Domain)
	if c.Listen == "" {
		if _, port, err := net.SplitHostPort(c.Domain); err == nil {
			c.Listen = ":" + port
		} else {
			c.Listen = ":8471"
		}
	}
	if c.SessionTTL <= 0 {
		c.SessionTTL = 24 * time.Hour
	}
	if c.KeyCacheTTL <= 0 {
		c.KeyCacheTTL = time.Hour
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 10 * time.Second
	}
	if c.PeerScheme == "" {
		c.PeerScheme = "https"
	}
	return nil
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// ModuleFactory creates a module for a newly loaded or created node.
// Modules receive the node ID, a scheduler for periodic work, the
// trustlines map for direct access, a bandwidth function, and persisted state (nil if new).
type ModuleFactory func(nodeID string, schedule ScheduleFunc, trustlines map[string]*Trustline, bandwidth BandwidthFunc, state json.RawMessage) link.Module

type moduleReg struct {
	name    string
	factory ModuleFactory
}

type Server struct {
	cfg       Config
	store     Store
	key       ed25519.PrivateKey
	client    *http.Client
	mux       *http.ServeMux
	factories []moduleReg

	nmu   sync.RWMutex
	nodes map[string]*link.Links

	umu      sync.Mutex
	users    map[string]*UserRecord
	sessions map[string]*session

	kmu      sync.Mutex
	keyCache map[string]cachedKey
}

// Register adds a module factory. Call before Init/RunAndWait.
func (s *Server) Register(name string, factory ModuleFactory) {
	s.factories = append(s.factories, moduleReg{name: name, factory: factory})
}

// Send posts a sync message to a peer's domain.
func (s *Server) Send(peerDomain string, msg link.SyncRequest) ([]byte, error) {
	status, body, err := s.postSigned(peerDomain, "/p2p/tx", msg)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d", peerDomain, status)
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// Node state — persisted per identity
// ---------------------------------------------------------------------------

type nodeState struct {
	ID         string                     `json:"id"`
	Links      map[string]*link.Link      `json:"links"`
	InviteMode link.InviteMode            `json:"invite_mode,omitempty"`
	Handlers   map[string]json.RawMessage `json:"handlers,omitempty"`
}

// newLinks creates a Links from persisted state (or nil for new).
func (s *Server) newLinks(id string, raw json.RawMessage) *link.Links {
	var st nodeState
	if raw != nil {
		if err := json.Unmarshal(raw, &st); err != nil {
			log.Fatalf("node %s: corrupt state: %v", id, err)
		}
	}

	// ls is captured by the commit closure below — declared first
	// so the closure can reference it.
	var ls *link.Links

	commit := func() {
		ns := nodeState{ID: id, Links: ls.Map(), InviteMode: ls.InviteMode}
		ns.Handlers = make(map[string]json.RawMessage)
		for name, mod := range ls.ModuleMap() {
			if data, err := json.Marshal(mod); err == nil {
				ns.Handlers[name] = data
			}
		}
		if err := s.store.SaveNode(id, ns); err != nil {
			log.Fatalf("node %s: persist failed: %v", id, err)
		}
	}

	send := func(peer string, msg link.SyncRequest) ([]byte, error) {
		return s.Send(PeerDomain(peer), msg)
	}

	ls = link.NewLinks(id, st.Links, st.InviteMode, send, commit)
	ls.Protected = map[string]struct{}{"trustline": {}}

	// Create and register the app-level Node first — other modules
	// reference its Trustlines map and Bandwidth function.
	th := NewNode(id, ls, handlerState(raw, "trustline"))
	ls.RegisterModule(th)

	// Register external modules from factories.
	schedule := func(delay time.Duration, fn func() []string) {
		ls.Schedule(delay, fn)
	}
	for _, f := range s.factories {
		m := f.factory(id, schedule, th.Trustlines, th.Bandwidth, handlerState(raw, f.name))
		ls.RegisterModule(m)
		if r, ok := m.(Reserver); ok {
			th.RegisterReserver(r)
		}
	}

	return ls
}

// handlerState extracts a handler's persisted state from the node's raw JSON.
func handlerState(raw json.RawMessage, name string) json.RawMessage {
	if raw == nil {
		return nil
	}
	var st struct {
		Handlers map[string]json.RawMessage `json:"handlers"`
	}
	if json.Unmarshal(raw, &st) != nil {
		return nil
	}
	return st.Handlers[name]
}

func NewServer(cfg Config) (*Server, error) {
	if err := cfg.fill(); err != nil {
		return nil, err
	}
	store := cfg.Store
	if store == nil {
		var err error
		if store, err = NewFileStore(cfg.DataDir); err != nil {
			return nil, err
		}
	}
	key, err := loadOrCreateKey(store)
	if err != nil {
		return nil, err
	}
	users, err := store.LoadUsers()
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg: cfg, store: store, key: key,
		client:   &http.Client{Timeout: cfg.HTTPTimeout},
		nodes:    map[string]*link.Links{},
		users:    users,
		sessions: map[string]*session{},
		keyCache: map[string]cachedKey{},
	}, nil
}

func (s *Server) Init() error {
	ids, err := s.store.ListNodes()
	if err != nil {
		return err
	}
	for _, id := range ids {
		raw, err := s.store.LoadNode(id)
		if err != nil {
			return err
		}
		user, domain, err := ParseIdentity(id)
		if err != nil || domain != s.cfg.Domain {
			continue
		}
		s.nodes[user] = s.newLinks(id, raw)
	}
	// Resume pending sends from boot.
	for _, ls := range s.nodes {
		ls.KickAll()
	}
	if s.cfg.AdminUser != "" && s.cfg.AdminPass != "" {
		if err := s.bootstrapAdmin(s.cfg.AdminUser, s.cfg.AdminPass); err != nil {
			return err
		}
	}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /p2p/key", s.handleKey)
	s.mux.HandleFunc("POST /p2p/tx", s.handleTx)
	s.mux.HandleFunc("POST /api/auth", s.handleAuth)
	s.mux.HandleFunc("POST /api/admin", s.handleAdmin)
	s.mux.HandleFunc("POST /api/account", s.handleAccount)
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{Handler: s.mux}
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-tctx.Done()
		sc, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		hs.Shutdown(sc)
	}()
	s.logf("listening on %s as domain %s", ln.Addr(), s.cfg.Domain)
	err := hs.Serve(ln)
	cancel()
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) logf(format string, args ...any) {
	if s.cfg.Verbose {
		log.Printf("[creditnet %s] "+format, append([]any{s.cfg.Domain}, args...)...)
	}
}

func (s *Server) getLinks(user string) *link.Links {
	s.nmu.RLock()
	defer s.nmu.RUnlock()
	return s.nodes[user]
}

func (s *Server) linksByIdentity(id string) *link.Links {
	user, domain, err := ParseIdentity(id)
	if err != nil || domain != s.cfg.Domain {
		return nil
	}
	return s.getLinks(user)
}

// ---------------------------------------------------------------------------
// P2P sync handler
// ---------------------------------------------------------------------------

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	sigDomain, sig, body, err := ReadP2P(r)
	if err != nil {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var env link.SyncRequest
	if json.Unmarshal(body, &env) != nil {
		HttpError(w, http.StatusBadRequest, "malformed")
		return
	}
	if !IdentityInDomain(env.From, sigDomain) {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	ls := s.linksByIdentity(env.To)
	if ls == nil {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	// Early rejection before expensive signature verification.
	if ls.QuickReject(env.From, env.Method) {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, syncErr := ls.HandleSync(env)
	if syncErr != nil {
		switch {
		case errors.Is(syncErr, link.ErrNoLink):
			HttpError(w, http.StatusNotFound, "no link")
		case errors.Is(syncErr, link.ErrInvitesDisabled):
			HttpError(w, http.StatusForbidden, "invites disabled")
		case errors.Is(syncErr, link.ErrMaxLinks):
			HttpError(w, http.StatusBadRequest, "max links")
		case errors.Is(syncErr, link.ErrStaleCounter), errors.Is(syncErr, link.ErrCounterAhead), errors.Is(syncErr, link.ErrCollision):
			HttpError(w, http.StatusConflict, syncErr.Error())
		case errors.Is(syncErr, link.ErrTimestamp):
			HttpError(w, http.StatusConflict, syncErr.Error())
		case errors.Is(syncErr, ErrBadMethod):
			HttpError(w, http.StatusBadRequest, syncErr.Error())
		default:
			HttpError(w, http.StatusBadRequest, syncErr.Error())
		}
		return
	}
	WriteJSON(w, http.StatusOK, resp)
}
