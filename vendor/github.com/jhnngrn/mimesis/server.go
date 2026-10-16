package mimesis

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
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config configures a Server.
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
// ScheduleFunc + ModuleFactory
// ---------------------------------------------------------------------------

// ScheduleFunc schedules a callback to run after a delay.
// The callback runs under the node's lock; it returns peers to kick.
type ScheduleFunc func(delay time.Duration, fn func() []string)

// ModuleFactory creates a module for a newly loaded or created node.
// The factory receives the node identity, a scheduler for periodic work,
// and persisted state (nil if new). App-specific dependencies (e.g.
// shared data structures between modules) are captured in the closure.
type ModuleFactory func(nodeID string, schedule ScheduleFunc, state json.RawMessage) Module

type moduleReg struct {
	name      string
	factory   ModuleFactory
	protected bool
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server is a multi-tenant p2p server that hosts Links nodes.
type Server struct {
	cfg       Config
	store     Store
	key       ed25519.PrivateKey
	client    *http.Client
	mux       *http.ServeMux
	factories []moduleReg

	nmu   sync.RWMutex
	nodes map[string]*Links

	umu      sync.Mutex
	users    map[string]*UserRecord
	sessions map[string]*session

	kmu      sync.Mutex
	keyCache map[string]cachedKey
}

// Register adds a module factory. Call before Init.
// If protected is true, the module cannot be removed via SetModules.
func (s *Server) Register(name string, factory ModuleFactory, protected bool) {
	s.factories = append(s.factories, moduleReg{name: name, factory: factory, protected: protected})
}

// Send posts a signed sync message to a peer's domain.
func (s *Server) Send(peerDomain string, msg SyncRequest) ([]byte, error) {
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
	Links      map[string]*Link           `json:"links"`
	InviteMode InviteMode                 `json:"invite_mode,omitempty"`
	Handlers   map[string]json.RawMessage `json:"handlers,omitempty"`
}

// NewLinks creates a Links from persisted state (or nil for new).
func (s *Server) newLinks(id string, raw json.RawMessage) *Links {
	var st nodeState
	if raw != nil {
		if err := json.Unmarshal(raw, &st); err != nil {
			log.Fatalf("node %s: corrupt state: %v", id, err)
		}
	}

	var ls *Links

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

	send := func(peer string, msg SyncRequest) ([]byte, error) {
		return s.Send(PeerDomain(peer), msg)
	}

	ls = NewLinks(id, st.Links, st.InviteMode, send, commit)

	schedule := func(delay time.Duration, fn func() []string) {
		ls.Schedule(delay, fn)
	}

	for _, f := range s.factories {
		m := f.factory(id, schedule, handlerState(raw, f.name))
		ls.RegisterModule(m)
		if f.protected {
			ls.Protected[f.name] = struct{}{}
		}
	}

	return ls
}

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

// NewServer creates a new server with the given configuration.
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
		nodes:    map[string]*Links{},
		users:    users,
		sessions: map[string]*session{},
		keyCache: map[string]cachedKey{},
	}, nil
}

// Init loads persisted nodes and sets up HTTP routes.
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

// Run listens on the configured address and serves until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve serves on the given listener until ctx is cancelled.
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
		log.Printf("[mimesis %s] "+format, append([]any{s.cfg.Domain}, args...)...)
	}
}

func (s *Server) getLinks(user string) *Links {
	s.nmu.RLock()
	defer s.nmu.RUnlock()
	return s.nodes[user]
}

func (s *Server) linksByIdentity(id string) *Links {
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
	sigDomain, sig, body, err := readP2P(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var env SyncRequest
	if json.Unmarshal(body, &env) != nil {
		httpError(w, http.StatusBadRequest, "malformed")
		return
	}
	if !IdentityInDomain(env.From, sigDomain) {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	ls := s.linksByIdentity(env.To)
	if ls == nil {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if ls.QuickReject(env.From, env.Method) {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, syncErr := ls.HandleSync(env)
	if syncErr != nil {
		switch {
		case errors.Is(syncErr, ErrNoLink):
			httpError(w, http.StatusNotFound, "no link")
		case errors.Is(syncErr, ErrInvitesDisabled):
			httpError(w, http.StatusForbidden, "invites disabled")
		case errors.Is(syncErr, ErrMaxLinks):
			httpError(w, http.StatusBadRequest, "max links")
		case errors.Is(syncErr, ErrStaleCounter), errors.Is(syncErr, ErrCounterAhead), errors.Is(syncErr, ErrCollision):
			httpError(w, http.StatusConflict, syncErr.Error())
		case errors.Is(syncErr, ErrTimestamp):
			httpError(w, http.StatusConflict, syncErr.Error())
		case errors.Is(syncErr, ErrBadMethod):
			httpError(w, http.StatusBadRequest, syncErr.Error())
		default:
			httpError(w, http.StatusBadRequest, syncErr.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
