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
// The factory receives the node identity, the owning Node, a scheduler
// for periodic work, and persisted state (nil if new). App-specific
// dependencies (e.g. shared data structures between modules) are
// captured in the closure.
type ModuleFactory func(name, nodeID string, node *Node, schedule ScheduleFunc, state json.RawMessage) Module

type moduleReg struct {
	name    string
	factory ModuleFactory
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server is a multi-tenant p2p server that hosts nodes.
type Server struct {
	cfg       Config
	store     Store
	key       ed25519.PrivateKey
	client    *http.Client
	mux       *http.ServeMux
	factories []moduleReg
	apps      map[string][]string // app name → child module names

	nmu   sync.RWMutex
	nodes map[string]*Node

	umu      sync.Mutex
	users    map[string]*UserRecord
	sessions map[string]*session

	kmu      sync.Mutex
	keyCache map[string]cachedKey
}

// Register adds a module factory. Call before Init.
func (s *Server) Register(name string, factory ModuleFactory) {
	s.factories = append(s.factories, moduleReg{name: name, factory: factory})
}

// DeclareApp declares a module as an app — a module that owns the lifecycle
// on a link. children lists the module names this app supports under it.
// Multiple apps can be declared; a given link has at most one.
// Call before Init.
func (s *Server) DeclareApp(name string, children []string) {
	if s.apps == nil {
		s.apps = map[string][]string{}
	}
	s.apps[name] = children
}

// Send posts a signed sync message to a peer's domain.
func (s *Server) Send(peerDomain string, msg txRequest) ([]byte, error) {
	status, body, err := s.postSigned(peerDomain, "/p2p/tx", msg)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d", peerDomain, status)
	}
	return body, nil
}

// newNode creates a Node from persisted state (or nil for new).
func (s *Server) newNode(id string, raw json.RawMessage) *Node {
	// Unmarshal persisted state into a temporary Node to extract fields.
	var loaded Node
	if raw != nil {
		if err := json.Unmarshal(raw, &loaded); err != nil {
			log.Fatalf("node %s: corrupt state: %v", id, err)
		}
	}

	var n *Node

	commit := func() {
		n.Handlers = make(map[string]json.RawMessage)
		for name, mod := range n.ModuleMap() {
			if data, err := json.Marshal(mod); err == nil {
				n.Handlers[name] = data
			}
		}
		if err := s.store.SaveNode(id, n); err != nil {
			log.Fatalf("node %s: persist failed: %v", id, err)
		}
		n.Handlers = nil
	}

	send := func(peer string, msg txRequest) ([]byte, error) {
		return s.Send(peerDomain(peer), msg)
	}

	n = newNode(id, loaded.Links, loaded.InviteMode, send, commit)

	schedule := func(delay time.Duration, fn func() []string) {
		n.schedule(delay, fn)
	}

	for _, f := range s.factories {
		m := f.factory(f.name, id, n, schedule, loaded.Handlers[f.name])
		n.registerModule(f.name, m)
	}
	n.apps = s.apps

	return n
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
		nodes:    map[string]*Node{},
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
		s.nodes[user] = s.newNode(id, raw)
	}
	for _, n := range s.nodes {
		n.kickAll()
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

func (s *Server) getNode(user string) *Node {
	s.nmu.RLock()
	defer s.nmu.RUnlock()
	return s.nodes[user]
}

func (s *Server) nodeByIdentity(id string) *Node {
	user, domain, err := ParseIdentity(id)
	if err != nil || domain != s.cfg.Domain {
		return nil
	}
	return s.getNode(user)
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
	var env txRequest
	if json.Unmarshal(body, &env) != nil {
		httpError(w, http.StatusBadRequest, "malformed")
		return
	}
	if !identityInDomain(env.From, sigDomain) {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	n := s.nodeByIdentity(env.To)
	if n == nil {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if n.quickReject(env.From, env.Method) {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil {
		httpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, syncErr := n.handleIncoming(env)
	if syncErr != nil {
		switch {
		case errors.Is(syncErr, errNoLink):
			httpError(w, http.StatusNotFound, "no link")
		case errors.Is(syncErr, errInvitesDisabled):
			httpError(w, http.StatusForbidden, "invites disabled")
		case errors.Is(syncErr, errMaxLinks):
			httpError(w, http.StatusBadRequest, "max links")
		case errors.Is(syncErr, errStaleCounter), errors.Is(syncErr, errCounterAhead), errors.Is(syncErr, errCollision):
			httpError(w, http.StatusConflict, syncErr.Error())
		case errors.Is(syncErr, errTimestamp):
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
