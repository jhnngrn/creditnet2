# mimesis

A framework for building federated peer-to-peer applications in Go.

Mimesis provides bilateral sync, multi-tenant HTTP serving, ed25519 peer
authentication, argon2 user auth, file-based persistence and an identity
format (`user@domain`) — so an application only has to implement its own
logic as modules.

## Concepts

A **Server** hosts multiple nodes keyed by username, each identified as
`user@domain`. Nodes communicate via signed HTTP messages.

A **Link** is a bilateral sync primitive between two identities. Each side
holds mirrored state with a shared counter and a deterministic turn bit
— both sides can send simultaneously, but the turn bit resolves every
collision deterministically, so conflicts never need manual resolution.
Pending sends and cached responses are persisted, so nothing is lost if
a node crashes mid-sync.

A **Module** handles application-specific sync methods and account API
calls. Modules are registered as factories and instantiated per node.

## Usage

```go
package main

import (
    "encoding/json"
    "github.com/jhnngrn/mimesis"
)

type myModule struct{ /* ... */ }

func (m *myModule) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
    // handle incoming sync
    return true, nil, nil
}
func (m *myModule) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
    return nil
}
func (m *myModule) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
    return nil, nil, mimesis.ErrBadMethod
}
func (m *myModule) NextTx(peer string) (string, json.RawMessage, int64) {
    return "", nil, 0
}
func (m *myModule) Status() json.RawMessage {
    return nil
}

func main() {
    srv, err := mimesis.Boot()
    if err != nil {
        panic(err)
    }
    srv.Register("myapp", func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
        return &myModule{}
    })
    mimesis.RunAndWait(srv)
}
```

```
go run . -domain myapp.example.com -port 3000
```

## API

### Server

- `Boot()` — parse flags, create a server
- `NewServer(Config)` — create with explicit config
- `srv.Register(name, factory)` — register a module factory
- `srv.Init()` — load persisted nodes, set up HTTP routes
- `srv.Run(ctx)` / `srv.Serve(ctx, listener)` — start serving
- `RunAndWait(srv)` — init + serve + block until SIGINT/SIGTERM

### HTTP endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/p2p/tx` | Signed peer-to-peer sync |
| GET | `/p2p/key` | Public key for signature verification |
| POST | `/api/auth` | User login (argon2) |
| POST | `/api/admin` | User management |
| POST | `/api/account` | Module account API |

### Module interface

```go
type Module interface {
    HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
    HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
    HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)
    NextTx(peer string) (string, json.RawMessage, int64)
    Status() json.RawMessage
}
```

### Node

The `*Node` received by the module factory:

- `node.Get(peer) *Link` — get an existing link
- `node.Connect(peer) (*Link, error)` — create an outgoing link (errors if exists or at capacity)
- `node.ModuleMap() map[string]Module` — access other module instances

### Link

Methods available on a `*Link`:

- `lk.Closing() bool` — true if the link is closing
- `lk.RefModule(name)` — hold a ref on this module (keeps the link open)
- `lk.UnrefModule(name)` — release the ref (sets IntentClose when all refs reach 0)
- `lk.ModuleEnabled(name) bool` — true if module is bilaterally connected
- `lk.ConnectModules(modules, registered)` — request connection for modules (additive)
- `lk.SetModules(modules, registered)` — propose modules on this link (closes non-desired)

### Account commands (link.*)

Built-in account API commands handled by mimesis directly:

- `link.accept {peer, modules?}` — accept an incoming link
- `link.reject {peer}` — reject an incoming link
- `link.set_modules {peer, modules}` — propose modules on a link
- `link.accept_module {peer, module}` — accept an incoming module proposal
- `link.reject_module {peer, module}` — reject an incoming module proposal
- `link.set_invite_mode {mode}` — set invite mode (open/invite/closed)

### Identity

Identities follow the format `user@domain`, normalized to lowercase.

```go
user, domain, err := mimesis.ParseIdentity("alice@example.com")
```
