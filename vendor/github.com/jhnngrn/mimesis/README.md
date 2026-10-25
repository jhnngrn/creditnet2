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

A **Root module** is a module that owns the lifecycle on a link. The root
module name is sent in `link.connect` and stored in `LinkState.Root`. A
link runs exactly one root module. Multiple root modules can be declared
on a server; which one runs is decided at connect time by the code calling
`Connect(peer, root)`.

A **Module** handles application-specific sync methods and account API
calls. Modules are registered as factories and instantiated per node.
Child modules are declared under a root module with `DeclareRoot` — they
can only connect on a link that runs their parent root module.

## Usage

```go
package main

import (
    "encoding/json"
    "github.com/jhnngrn/mimesis"
)

type myModule struct{ /* ... */ }

func (m *myModule) OnConnect(peer string)   {}
func (m *myModule) OnClose(peer string)     {}
func (m *myModule) ForceRemove(peer string) {}
func (m *myModule) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, error) {
    // handle incoming sync
    return true, nil
}
func (m *myModule) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) {
}
func (m *myModule) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error) {
    return nil, mimesis.ErrBadMethod
}
func (m *myModule) Status() json.RawMessage {
    return nil
}
func (m *myModule) Busy(peer string) bool {
    return false
}

func main() {
    srv, err := mimesis.Boot("myapp")
    if err != nil {
        panic(err)
    }
    srv.Register("myapp", func(name, nodeID string, node *mimesis.Node, api mimesis.ModuleAPI, state json.RawMessage) mimesis.Module {
        return &myModule{}
    })
    srv.Register("helper", newHelper)
    srv.DeclareRoot("myapp", []string{"helper"}) // myapp is a root module with helper as child
    mimesis.RunAndWait(srv)
}
```

```
go run . -domain myapp.example.com -port 3000
```

## API

### Server

- `Boot(appName)` — parse flags, create a server (default data dir `/var/lib/<appName>`)
- `NewServer(Config)` — create with explicit config
- `srv.Register(name, factory)` — register a module factory
- `srv.DeclareRoot(name, children)` — declare a module as a root module with optional child modules
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
    OnConnect(peer string)
    OnClose(peer string)
    HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, error)
    HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64)
    HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error)
    Status() json.RawMessage
    Busy(peer string) bool
    ForceRemove(peer string)
}
```

`OnConnect` is called when a module becomes bilaterally connected — on
`link.connect` for the root module, and on `module.connect` for child
modules. `OnClose` is called when a module is disconnected — on
`module.close` for child modules, and on `link.close` for all connected
modules (children first, then root), just before the link is deleted.
Only modules that received `OnConnect` will receive `OnClose`. Modules
use these to create and clean up per-peer state. Modules that don't need
lifecycle hooks provide empty implementations.

Modules enqueue outgoing messages via `ModuleAPI.Send`. The framework
manages the outbox and automatically kicks the peer for delivery when a
message is enqueued. `ModuleAPI` also provides `DeleteQueue`, `QueueHas`,
`QueueExists`, `QueuePeers`, and `Schedule`.

`ForceRemove` is called when a link is unilaterally removed without
bilateral consensus. The module must clean up state that would normally
be garbage collected through the bilateral close cycle.

### Node

The `*Node` received by the module factory:

- `node.Get(peer) *Link` — get an existing link
- `node.Connect(peer, root) (*Link, error)` — create an outgoing link running the named root module
- `node.ForceRemove(peer)` — unilaterally remove a link without bilateral consensus
- `node.ModuleMap() map[string]Module` — access other module instances
- `node.ModuleAvailable(peer, module) bool` — true if module is bilaterally connected and not closing

### Module lifecycle

Modules implement `Busy(peer string) bool` to report whether they have
active work with a peer. The sync loop checks `Busy` before sending
`module.close` or `link.close`, deferring the close until the module is
idle. Because `Busy` reads bilateral state that only changes through
req/resp exchanges, the peer is always in the sync loop when the
transition happens — no polling or timers needed.

`SetModules` marks unwanted modules with `IntentClose`. If a module is
re-added via `SetModules` before the close message is sent, the opposing
`IntentConnect` cancels `IntentClose` in `nextTx`.

### Link

Methods available on a `*Link`:

- `node.Close(peer)` — mark the link for closing and kick (deferred until no module is Busy)
- `lk.Closing() bool` — true if the link is closing
- `node.ConnectModules(peer, modules)` — request connection for child modules on a link (additive)


### Account commands

Built-in account API commands handled by mimesis directly:

**link.\*** — link-level:
- `link.accept {peer}` — accept an incoming link
- `link.reject {peer}` — reject an incoming link
- `link.set_modules {peer, modules}` — propose modules on a link
- `link.set_invite_mode {mode}` — set invite mode (open/invite/closed)
- `link.force_remove {peer}` — unilaterally remove a link

**module.\*** — module-level:
- `module.accept {peer, module}` — accept an incoming module proposal
- `module.reject {peer, module}` — reject an incoming module proposal

### Identity

Identities follow the format `user@domain`, normalized to lowercase.

```go
user, domain, err := mimesis.ParseIdentity("alice@example.com")
```
