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

An **App** is a module that owns the lifecycle on a link. The app name is
sent in `app.connect` and stored in `LinkState.App`. A link runs exactly
one app. Multiple apps can be declared on a server; which one runs is
decided at connect time by the code calling `Connect(peer, app)`.

A **Module** handles application-specific sync methods and account API
calls. Modules are registered as factories and instantiated per node.
Child modules are declared under an app with `DeclareApp` — they can only
connect on a link that runs their parent app.

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
func (m *myModule) Busy(peer string) bool {
    return false
}

func main() {
    srv, err := mimesis.Boot()
    if err != nil {
        panic(err)
    }
    srv.Register("myapp", func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
        return &myModule{}
    })
    srv.Register("helper", newHelper)
    srv.DeclareApp("myapp", []string{"helper"}) // myapp is an app with helper as child module
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
- `srv.DeclareApp(name, children)` — declare a module as an app with optional child modules
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
    Busy(peer string) bool
}
```

### App interface

A module declared as an app with `DeclareApp` must implement `App`, which
embeds `Module` and adds lifecycle hooks:

```go
type App interface {
    Module
    OnConnect(peer string)
    OnClose(peer string)
}
```

`OnConnect` is called when `app.connect` completes bilaterally — on both
the initiating and receiving side. `OnClose` is called when `app.close`
completes bilaterally, just before the link is deleted. The app uses these
to create and clean up per-peer state. Declared apps are validated at
startup — if a module registered as an app does not implement `App`,
the server fatals.

### Node

The `*Node` received by the module factory:

- `node.Get(peer) *Link` — get an existing link
- `node.Connect(peer, app) (*Link, error)` — create an outgoing link running the named app
- `node.ModuleMap() map[string]Module` — access other module instances
- `node.ModuleAvailable(peer, module) bool` — true if module is bilaterally connected and not closing

### Module lifecycle

Modules implement `Busy(peer string) bool` to report whether they have
active work with a peer. The sync loop checks `Busy` before sending
`module.close` or `app.close`, deferring the close until the module is
idle. Because `Busy` reads bilateral state that only changes through
req/resp exchanges, the peer is always in the sync loop when the
transition happens — no polling or timers needed.

`SetModules` marks unwanted modules with `IntentClose`. If a module is
re-added via `SetModules` before the close message is sent, the opposing
`IntentConnect` cancels `IntentClose` in `nextTx`.

### Link

Methods available on a `*Link`:

- `lk.Close()` — mark the link for closing (deferred until no module is Busy)
- `lk.Closing() bool` — true if the link is closing
- `lk.ConnectModules(modules, registered, apps)` — request connection for child modules (additive)


### Account commands

Built-in account API commands handled by mimesis directly:

**app.\*** — app-level:
- `app.accept {peer}` — accept an incoming link
- `app.reject {peer}` — reject an incoming link
- `app.set_modules {peer, modules}` — propose modules on a link
- `app.set_invite_mode {mode}` — set invite mode (open/invite/closed)

**module.\*** — module-level:
- `module.accept {peer, module}` — accept an incoming module proposal
- `module.reject {peer, module}` — reject an incoming module proposal

### Identity

Identities follow the format `user@domain`, normalized to lowercase.

```go
user, domain, err := mimesis.ParseIdentity("alice@example.com")
```
