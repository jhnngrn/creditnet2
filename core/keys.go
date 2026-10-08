package core

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	hdrDomain    = "X-P2PTX-Domain"
	hdrSignature = "X-P2PTX-Signature"
	sigPrefix    = "P2PTX1\n"
	maxBodySize  = 1 << 20
	MaxKeyCache  = 256
)

func signable(path string, body []byte) []byte {
	buf := make([]byte, 0, len(sigPrefix)+len(path)+1+len(body))
	buf = append(buf, sigPrefix...)
	buf = append(buf, path...)
	buf = append(buf, '\n')
	buf = append(buf, body...)
	return buf
}

func loadOrCreateKey(store Store) (ed25519.PrivateKey, error) {
	seed, err := store.LoadServerKey()
	switch {
	case err == nil:
		if len(seed) != ed25519.SeedSize { return nil, fmt.Errorf("bad seed length %d", len(seed)) }
		return ed25519.NewKeyFromSeed(seed), nil
	case errors.Is(err, ErrNotFound):
		seed = make([]byte, ed25519.SeedSize)
		rand.Read(seed)
		if err := store.SaveServerKey(seed); err != nil { return nil, err }
		return ed25519.NewKeyFromSeed(seed), nil
	default:
		return nil, err
	}
}

type cachedKey struct {
	key     ed25519.PublicKey
	fetched time.Time
}

func (s *Server) peerKey(domain string) (ed25519.PublicKey, error) {
	if domain == "" { return nil, errors.New("empty peer domain") }
	s.kmu.Lock()
	if c, ok := s.keyCache[domain]; ok && time.Since(c.fetched) < s.cfg.KeyCacheTTL {
		s.kmu.Unlock(); return c.key, nil
	}
	if len(s.keyCache) >= MaxKeyCache {
		for k, v := range s.keyCache { if time.Since(v.fetched) > s.cfg.KeyCacheTTL { delete(s.keyCache, k) } }
	}
	cacheable := len(s.keyCache) < MaxKeyCache
	s.kmu.Unlock()
	url := fmt.Sprintf("%s://%s/p2p/key", s.cfg.PeerScheme, domain)
	resp, err := s.client.Get(url)
	if err != nil { return nil, fmt.Errorf("key fetch %s: %w", domain, err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil, fmt.Errorf("key fetch %s: status %d", domain, resp.StatusCode) }
	var body struct { Domain string `json:"domain"`; PublicKey string `json:"public_key"` }
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodySize)).Decode(&body); err != nil { return nil, err }
	raw, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil || len(raw) != ed25519.PublicKeySize { return nil, fmt.Errorf("key fetch %s: malformed", domain) }
	key := ed25519.PublicKey(raw)
	s.kmu.Lock()
	if cacheable { s.keyCache[domain] = cachedKey{key: key, fetched: time.Now()} }
	s.kmu.Unlock()
	return key, nil
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{
		"domain": s.cfg.Domain, "public_key": base64.StdEncoding.EncodeToString(s.key.Public().(ed25519.PublicKey)),
	})
}

func ReadP2P(r *http.Request) (domain string, sig []byte, body []byte, err error) {
	body, err = io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBodySize))
	if err != nil { return "", nil, nil, err }
	domain = r.Header.Get(hdrDomain)
	rawSig, derr := base64.StdEncoding.DecodeString(r.Header.Get(hdrSignature))
	if domain == "" || derr != nil || len(rawSig) != ed25519.SignatureSize {
		return "", nil, nil, errors.New("missing or malformed signature headers")
	}
	return domain, rawSig, body, nil
}

func (s *Server) verifyP2PSignature(domain, path string, sig, body []byte) error {
	pub, err := s.peerKey(domain)
	if err != nil { return err }
	if !ed25519.Verify(pub, signable(path, body), sig) { return fmt.Errorf("bad signature from %s", domain) }
	return nil
}

func (s *Server) postSigned(domain, path string, v any) (int, []byte, error) {
	body, _ := json.Marshal(v)
	url := fmt.Sprintf("%s://%s%s", s.cfg.PeerScheme, domain, path)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hdrDomain, s.cfg.Domain)
	req.Header.Set(hdrSignature, base64.StdEncoding.EncodeToString(ed25519.Sign(s.key, signable(path, body))))
	resp, err := s.client.Do(req)
	if err != nil { return 0, nil, err }
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil { return resp.StatusCode, nil, err }
	return resp.StatusCode, rb, nil
}
