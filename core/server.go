package core

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

	"creditnet/base"
	"creditnet/link"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Domain  string
	Listen  string
	DataDir string
	Store   Store

	SessionTTL time.Duration
	KeyCacheTTL   time.Duration
	HTTPTimeout   time.Duration
	PeerScheme    string

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
// nodeStoreAdapter — bridges core.Store to link.NodeStore
// ---------------------------------------------------------------------------

type nodeStoreAdapter struct {
	store Store
}

func (a *nodeStoreAdapter) Save(id string, state any) error {
	return a.store.SaveNode(id, state)
}

func (a *nodeStoreAdapter) Load(id string) (json.RawMessage, error) {
	return a.store.LoadNode(id)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// HandlerFactory creates a module handler for a newly loaded or created node.
// Modules receive a PeerView for querying trustline state and a BandwidthFunc
// for checking available capacity (accounting for all reservations).
type HandlerFactory func(n *link.Node, peers base.PeerView, bw base.BandwidthFunc) link.Handler

type handlerReg struct {
	name    string
	factory HandlerFactory
}

type Server struct {
	cfg       Config
	store     Store
	key       ed25519.PrivateKey
	client    *http.Client
	mux       *http.ServeMux
	factories []handlerReg

	nmu   sync.RWMutex
	nodes map[string]*link.Node

	umu      sync.Mutex
	users    map[string]*UserRecord
	sessions map[string]*session

	kmu      sync.Mutex
	keyCache map[string]cachedKey
}

// Register adds a handler factory. Call before Init/RunAndWait.
func (s *Server) Register(name string, factory HandlerFactory) {
	s.factories = append(s.factories, handlerReg{name: name, factory: factory})
}

// Send implements link.Transport.
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

func (s *Server) newNode(id string, raw json.RawMessage) *link.Node {
	n := link.NewNode(id, raw, s, &nodeStoreAdapter{s.store})
	n.RegisterHandler("link", link.NewLinkHandler(n))
	bh := base.NewHandler(n)
	n.RegisterHandler("trustline", bh)

	for _, f := range s.factories {
		h := f.factory(n, bh, bh.Bandwidth)
		n.RegisterHandler(f.name, h)
		if r, ok := h.(base.Reserver); ok {
			bh.RegisterReserver(r)
		}
	}
	return n
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
		nodes:    map[string]*link.Node{},
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
		user, domain, err := link.ParseIdentity(id)
		if err != nil || domain != s.cfg.Domain {
			continue
		}
		s.nodes[user] = s.newNode(id, raw)
	}
	// Resume pending sends from boot.
	for _, n := range s.nodes {
		n.KickAll()
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

func (s *Server) getNode(user string) *link.Node {
	s.nmu.RLock()
	defer s.nmu.RUnlock()
	return s.nodes[user]
}

func (s *Server) nodeByIdentity(id string) *link.Node {
	user, domain, err := link.ParseIdentity(id)
	if err != nil || domain != s.cfg.Domain {
		return nil
	}
	return s.getNode(user)
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
	if !link.IdentityInDomain(env.From, sigDomain) {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	n := s.nodeByIdentity(env.To)
	if n == nil {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	// Early rejection before expensive signature verification.
	if n.QuickReject(env.From, env.Method) {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil {
		HttpError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, syncErr := n.HandleSync(env)
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
		case errors.Is(syncErr, link.ErrBadMethod):
			HttpError(w, http.StatusBadRequest, syncErr.Error())
		case errors.Is(syncErr, link.ErrPersistFailed):
			HttpError(w, http.StatusInternalServerError, "persist failed")
		default:
			HttpError(w, http.StatusBadRequest, syncErr.Error())
		}
		return
	}
	WriteJSON(w, http.StatusOK, resp)
}
