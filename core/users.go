package core

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"creditnet2/link"
	"golang.org/x/crypto/argon2"
)

const MaxUsers = 1000

type apiRequest struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type session struct {
	user    string
	admin   bool
	expires time.Time
}

// argon2id parameters — OWASP recommended minimum.
const (
	argonTime    = 2
	argonMemory  = 19 * 1024 // 19 MiB
	argonThreads = 1
	argonKeyLen  = 32
)

func hashPassword(password string) (salt, hash []byte) {
	salt = make([]byte, 16)
	rand.Read(salt)
	hash = argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return
}

func verifyPassword(rec *UserRecord, password string) bool {
	h := argon2.IDKey([]byte(password), rec.Salt, argonTime, argonMemory, argonThreads, uint32(len(rec.Hash)))
	return subtle.ConstantTimeCompare(h, rec.Hash) == 1
}

var errUserExists = errors.New("user already exists")

func (s *Server) createUser(username, password string, admin bool) error {
	username = strings.ToLower(username)
	if !link.ValidUsername(username) { return fmt.Errorf("invalid username %q", username) }
	s.umu.Lock()
	if _, ok := s.users[username]; ok { s.umu.Unlock(); return errUserExists }
	if len(s.users) >= MaxUsers { s.umu.Unlock(); return fmt.Errorf("max users reached") }
	salt, hash := hashPassword(password)
	s.users[username] = &UserRecord{Name: username, Salt: salt, Hash: hash, Admin: admin}
	if err := s.store.SaveUsers(s.users); err != nil { delete(s.users, username); s.umu.Unlock(); return err }
	s.umu.Unlock()
	if admin { return nil }
	nodeID := username + "@" + s.cfg.Domain
	n := s.newNode(nodeID, nil)
	n.Lock(); err := n.Commit(); n.Unlock()
	if err != nil { return err }
	s.nmu.Lock(); s.nodes[username] = n; s.nmu.Unlock()
	return nil
}

func (s *Server) bootstrapAdmin(user, pass string) error {
	err := s.createUser(user, pass, true)
	if errors.Is(err, errUserExists) { return nil }
	return err
}

func (s *Server) newSession(user string, admin bool) string {
	b := make([]byte, 32); rand.Read(b)
	token := hex.EncodeToString(b)
	s.umu.Lock(); s.sessions[token] = &session{user: user, admin: admin, expires: time.Now().Add(s.cfg.SessionTTL)}; s.umu.Unlock()
	return token
}

func (s *Server) authenticate(r *http.Request) (*session, string, error) {
	h := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || token == "" { return nil, "", errors.New("missing bearer token") }
	s.umu.Lock(); defer s.umu.Unlock()
	sess := s.sessions[token]
	if sess == nil || time.Now().After(sess.expires) { delete(s.sessions, token); return nil, "", errors.New("invalid or expired session") }
	return sess, token, nil
}

func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	req, ok := readAPIRequest(w, r); if !ok { return }
	var c struct{ Username, Password, Old, New string }
	json.Unmarshal(req.Payload, &c)
	switch req.Method {
	case "register":
		if err := s.createUser(c.Username, c.Password, false); err != nil { ApiError(w, err.Error()); return }
		ApiOK(w, map[string]string{"user": c.Username})
	case "login":
		s.umu.Lock(); rec := s.users[strings.ToLower(c.Username)]; s.umu.Unlock()
		if rec == nil || !verifyPassword(rec, c.Password) { ApiError(w, "invalid credentials"); return }
		ApiOK(w, map[string]any{"token": s.newSession(rec.Name, rec.Admin), "admin": rec.Admin})
	case "logout":
		_, token, err := s.authenticate(r); if err != nil { ApiError(w, err.Error()); return }
		s.umu.Lock(); delete(s.sessions, token); s.umu.Unlock(); ApiOK(w, true)
	case "change_password":
		sess, _, err := s.authenticate(r); if err != nil { ApiError(w, err.Error()); return }
		s.umu.Lock(); rec := s.users[sess.user]
		if rec == nil || !verifyPassword(rec, c.Old) { s.umu.Unlock(); ApiError(w, "invalid credentials"); return }
		rec.Salt, rec.Hash = hashPassword(c.New); err = s.store.SaveUsers(s.users); s.umu.Unlock()
		if err != nil { ApiError(w, "persist failed"); return }; ApiOK(w, true)
	default:
		ApiError(w, "unknown method")
	}
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	sess, _, err := s.authenticate(r)
	if err != nil { HttpError(w, http.StatusUnauthorized, err.Error()); return }
	if !sess.admin { HttpError(w, http.StatusForbidden, "admin only"); return }
	req, ok := readAPIRequest(w, r); if !ok { return }
	var p struct{ Username, Password string }
	json.Unmarshal(req.Payload, &p); p.Username = strings.ToLower(p.Username)
	switch req.Method {
	case "create_user":
		if err := s.createUser(p.Username, p.Password, false); err != nil { ApiError(w, err.Error()); return }
		ApiOK(w, map[string]string{"user": p.Username})
	case "delete_user":
		s.umu.Lock()
		if _, ok := s.users[p.Username]; !ok { s.umu.Unlock(); ApiError(w, "no such user"); return }
		delete(s.users, p.Username)
		for t, ss := range s.sessions { if ss.user == p.Username { delete(s.sessions, t) } }
		err := s.store.SaveUsers(s.users); s.umu.Unlock()
		if err != nil { ApiError(w, "persist failed"); return }
		s.nmu.Lock(); delete(s.nodes, p.Username); s.nmu.Unlock()
		s.store.DeleteNode(p.Username + "@" + s.cfg.Domain); ApiOK(w, true)
	case "list_users":
		s.umu.Lock()
		type u struct{ Name string `json:"name"`; Admin bool `json:"admin"` }
		list := make([]u, 0, len(s.users))
		for _, rec := range s.users { list = append(list, u{rec.Name, rec.Admin}) }
		s.umu.Unlock(); ApiOK(w, list)
	case "reset_password":
		s.umu.Lock(); rec := s.users[p.Username]
		if rec == nil { s.umu.Unlock(); ApiError(w, "no such user"); return }
		rec.Salt, rec.Hash = hashPassword(p.Password)
		err := s.store.SaveUsers(s.users); s.umu.Unlock()
		if err != nil { ApiError(w, "persist failed"); return }; ApiOK(w, true)
	default:
		ApiError(w, "unknown method")
	}
}

func readAPIRequest(w http.ResponseWriter, r *http.Request) (apiRequest, bool) {
	var req apiRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil || json.Unmarshal(body, &req) != nil || req.Method == "" {
		HttpError(w, http.StatusBadRequest, "expected JSON {method, payload}"); return req, false
	}
	return req, true
}

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json"); w.WriteHeader(code); json.NewEncoder(w).Encode(v)
}

func HttpError(w http.ResponseWriter, code int, msg string) { WriteJSON(w, code, map[string]string{"error": msg}) }
func ApiOK(w http.ResponseWriter, result any) { WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result}) }
func ApiError(w http.ResponseWriter, msg string) { WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": msg}) }
