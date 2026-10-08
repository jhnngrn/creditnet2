package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrNotFound = errors.New("not found")

type Store interface {
	SaveNode(id string, v any) error
	LoadNode(id string) (json.RawMessage, error)
	DeleteNode(id string) error
	ListNodes() ([]string, error)
	SaveUsers(users map[string]*UserRecord) error
	LoadUsers() (map[string]*UserRecord, error)
	SaveServerKey(seed []byte) error
	LoadServerKey() ([]byte, error)
}

type UserRecord struct {
	Name  string `json:"name"`
	Salt  []byte `json:"salt"`
	Hash  []byte `json:"hash"`
	Admin bool   `json:"admin"`
}

type FileStore struct{ dir string }

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "nodes"), 0o700); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (fs *FileStore) nodePath(id string) string {
	safe := strings.NewReplacer("@", "_at_", ":", "_", "/", "_", "\\", "_").Replace(id)
	return filepath.Join(fs.dir, "nodes", safe+".json")
}

func (fs *FileStore) SaveNode(id string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil { return err }
	return atomicWrite(fs.nodePath(id), b)
}

func (fs *FileStore) LoadNode(id string) (json.RawMessage, error) {
	b, err := os.ReadFile(fs.nodePath(id))
	if os.IsNotExist(err) { return nil, ErrNotFound }
	return json.RawMessage(b), err
}

func (fs *FileStore) DeleteNode(id string) error {
	err := os.Remove(fs.nodePath(id))
	if os.IsNotExist(err) { return nil }
	return err
}

func (fs *FileStore) ListNodes() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(fs.dir, "nodes"))
	if err != nil { return nil, err }
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") { continue }
		b, err := os.ReadFile(filepath.Join(fs.dir, "nodes", e.Name()))
		if err != nil { continue }
		var peek struct{ ID string `json:"id"` }
		if json.Unmarshal(b, &peek) == nil && peek.ID != "" { ids = append(ids, peek.ID) }
	}
	return ids, nil
}

func (fs *FileStore) SaveUsers(users map[string]*UserRecord) error {
	b, _ := json.MarshalIndent(users, "", " ")
	return atomicWrite(filepath.Join(fs.dir, "users.json"), b)
}

func (fs *FileStore) LoadUsers() (map[string]*UserRecord, error) {
	b, err := os.ReadFile(filepath.Join(fs.dir, "users.json"))
	if os.IsNotExist(err) { return map[string]*UserRecord{}, nil }
	if err != nil { return nil, err }
	users := map[string]*UserRecord{}
	if err := json.Unmarshal(b, &users); err != nil { return nil, err }
	return users, nil
}

func (fs *FileStore) SaveServerKey(seed []byte) error {
	return atomicWrite(filepath.Join(fs.dir, "server_key"), []byte(fmt.Sprintf("%x\n", seed)))
}

func (fs *FileStore) LoadServerKey() ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(fs.dir, "server_key"))
	if os.IsNotExist(err) { return nil, ErrNotFound }
	if err != nil { return nil, err }
	var seed []byte
	_, err = fmt.Sscanf(strings.TrimSpace(string(b)), "%x", &seed)
	return seed, err
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil { return err }
	if _, err = f.Write(data); err == nil { err = f.Sync() }
	if cerr := f.Close(); err == nil { err = cerr }
	if err != nil { os.Remove(tmp); return err }
	if err := os.Rename(tmp, path); err != nil { os.Remove(tmp); return err }
	if d, derr := os.Open(filepath.Dir(path)); derr == nil { d.Sync(); d.Close() }
	return nil
}
