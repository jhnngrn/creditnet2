# creditnet

Distributed systems are hard because nodes live on different machines. Synchronization breaks, state diverges, race conditions appear, and developers spend most of their time on problems that only exist because the network is distributed.

The fix is simple: force every node-to-node interaction through a sequential, persisted, atomic sync protocol with guaranteed delivery. Every message is persisted before it is sent. Every response is persisted before it is returned. If a node crashes, it picks up exactly where it left off. Two nodes can never disagree about what happened, because they share a single counter and process one decision at a time.

The cost is that all interaction hits storage. But if you accept that cost, the entire class of distributed sync problems disappears. Developers write peer-to-peer applications as if all nodes were on the same machine — no sync logic, no conflict resolution, no defensive coding against network failures. The protocol handles it.

creditnet is a federated bilateral credit network built on this protocol. Each server hosts users identified as `alice@example.com`. Each user is a node with bilateral links to other nodes. Application logic is built as modules on top — credit limits, balances, payments, or anything else. Because the sync layer has already solved delivery, ordering and crash recovery, a module is just regular request/response logic with no distributed systems complexity. Anyone can write a module with their own custom logic and plug it in.

## Build

```
go build ./cmd/base         # server with base handler (peers, credit, payments)
```

## Run

```
./base -domain node1.example.com -port 3000 -scheme http
```

Flags:

- `-domain` — server identity (required, or `DOMAIN` env)
- `-port` — HTTP port (default 3000)
- `-data` — data directory (default `/var/lib/creditnet`)
- `-scheme` — peer communication scheme, `https` or `http` (default `https`)

## Architecture

The bilateral sync primitive lives in its own repository
([github.com/jhnngrn/link](https://github.com/jhnngrn/link)) and is
imported as an external package. It has zero dependencies back to
creditnet and can be used standalone in any p2p application.

```
github.com/jhnngrn/link   (external)
  link.go              bilateral sync primitive: Link, Links collection,
                       sync lifecycle, wire types, dispatch, module negotiation,
                       InviteMode, PeerSend, EngineConfig

types.go               identities, Module interface, errors, helpers
node.go                Node: app-level module implementing link.Module,
                       holds trustlines and bandwidth, registers reservers
trustline.go           trustline data and sync: credit limits, balances, payments
demurrage.go           demurrage computation (decay over time)
server.go              Server, Config, module factory registration, P2P
account.go             namespace-routed account API dispatch
keys.go                Ed25519 signing and verification
users.go               auth, sessions, admin, argon2 password hashing
store.go               persistence (file-based)
run.go                 Boot/RunAndWait helpers

modules/
  triad/               three-party novation for circular debt elimination
  loops/               multi-hop loop clearing with hash-chain 2PC settlement

cmd/
  base/                server with built-in trustline module
  triad/               server with trustline + triad modules
  loops/               server with trustline + loops modules
```

The link package ([github.com/jhnngrn/link](https://github.com/jhnngrn/link))
is a self-contained bilateral sync primitive with no dependencies back to
creditnet. It owns all bilateral state, protocol logic, module negotiation,
collection management, policy (InviteMode, MaxLinks), dispatch, locking,
persistence (via Commit callback), and the full sync loop (send, retry, kick).
Node is the app-level module that implements link.Module — it holds trustlines
and bandwidth that other modules depend on.
Modules register under a namespace via `Server.Register(name, ModuleFactory)`
and receive sync dispatch. Different binaries under `cmd/` combine different
sets of modules.

## Link layer

The link layer provides bilateral connections between nodes.

### Module negotiation

Links start as bare connections. Modules are proposed explicitly — when application code sets `IntentConnect` on a module's `ConnectionState`, the node sends a `link.module` message to the peer. The peer ACKs if it supports the module. Both sides track each module as a `ConnectionState` (same type as the link itself: `ConnectionStatus` + `IntentConnect`/`IntentClose`). Once a module is bilaterally `connected`, the node dispatches sync messages for that module on the link.

This means application-level traffic (trustline operations, triad novations) only flows after both sides have agreed on the module. Adding a new module to the network is backward-compatible — nodes that don't know it simply NACK the proposal.

### Connected gate

The node's `nextTx` always checks for pending link-lifecycle messages first (connect/close/module work regardless of connection state). For modules, `nextTx` requires `link.State.Status == Connected` and the module bilaterally active before asking. This means application-level sync methods (credit limits, payments, etc.) are only sent on connected links with the module enabled, without each module needing to check connection state itself.

### Link state machine

```
                   AddLink               incoming link.connect
                      |                         |
                      v                         v
                +-----------+             +----------+
                |  waiting  |             | incoming  |
                +--+--+--+-+             +-+------+-+
                   |  |  |                 |      |
recv link.connect  |  |  |                 |      |  RejectLink
(mutual) ----------+  |  | RemoveLink     |      |  or RemoveLink
                       |  | (link.close)   |      |  (link.close)
          resp OK -----+  +------+        |      +------+
                       |          |        |             |
                       |     resp OK       |        resp OK
          resp NACK    |          |        |             |
          (stays       |          v        |             v
           waiting) <--+      (deleted)    |         (deleted)
                       |                   |
                       v                   | AcceptLink
                 +----------+              | (sets IntentConnect,
                 |connected |<-------------+ status stays incoming)
                 +-----+----+
                       |
          RemoveLink   |
          (link.close) |
                       |
                  resp OK
                       |
                       v
                   (deleted)
```

**States** (ConnectionStatus):

- **waiting** — sent `link.connect`, got NACKed. Peer has the invite (`incoming` on their side).
- **incoming** — received `link.connect` from peer. Awaiting local decision (`AcceptLink` or `RejectLink`).
- **connected** — bilaterally established. Modules are proposed explicitly by application code.

A new link starts with no status and `IntentConnect = true`. Status becomes `waiting` after a NACK, `connected` after mutual detection or ACK.

Links use reference counting — modules that hold a link open call `Ref()` on the link's `ConnectionState` when they connect and `UnrefClose()` when they close. `UnrefClose` decrements refs and sets `IntentClose` automatically when refs reach 0. `NextTx` only sends `link.close` when `IntentClose && Refs == 0`. This ensures modules finish their close handshakes before the link itself closes, and the link closes automatically when the last module disconnects.

### Invite mode

Three-state `InviteMode` controls how a node handles incoming connections from unknown peers:

- **closed** — reject all incoming `link.connect` from unknown peers (before signature verification).
- **invite** — accept the link as `incoming`, NACK it, and wait for manual `accept_peer` / `reject_peer`. This is the default.
- **open** — auto-accept at every layer. `link.connect` → ACK (connected), `link.module` → ACK (module enabled), `trustline.connect` → ACK (peer connected). No manual step needed.

## Trustline layer

Trustline handling is built into Node under the `"trustline"` namespace. It manages per-peer credit data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), `balance`, and `demurrage` rates. Changes are transactional — a credit limit, demurrage change or payment is pending until the peer confirms.

### Trustline lifecycle

Trustlines use two booleans: `Connected` (bilateral agreement) and `IntentClose` (local intent to disconnect). Module consent implies trustline consent — accepting the trustline module on a link is sufficient to establish trustlines on it.

```
  add_peer                        incoming link.connect
  (creates link +                 (from peer)
   empty Trustline)                     |
       |                                v
       v                          link incoming
  link waiting                    (no trustline yet)
       |                                |
       |                          accept_peer
  link.connect completes          (AcceptLink + module)
  + module negotiation                  |
  + trustline.connect sent        link connects
       |                          + module negotiation
       v                          + trustline.connect
  trustline Connected                   |
  bilaterally                           v
       |                          trustline Connected
  remove_peer
  (sets IntentClose)
       |
  trustline.close sent
  peer.Connected = false
  (IntentClose stays true
   → tombstone)
```

The initiator creates an empty `Trustline` in `add_peer`. After link and module connect, `trustline.connect` is sent and auto-accepted — the receiver creates the `Trustline` with `Connected = true`.

Disconnection uses `trustline.close` — `remove_peer` sets `IntentClose` on the trustline. After the close handshake, `Connected` becomes false but `IntentClose` stays true, forming a tombstone: `!Connected && IntentClose` causes `trustline.connect` to be rejected, preventing reconnection. If the close is rejected by the peer, `IntentClose` is cleared.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Sync methods

Sync methods are namespace-routed: `namespace.method`. Node dispatches by namespace prefix.

**link namespace:**

`link.connect` — establish bilateral connection. Mutual add → instant `connected`. Incoming from unknown → `incoming` (NACKed, awaiting accept/reject). In `open` mode, auto-ACKed.

`link.module` — bilateral module negotiation. Sent automatically after a link connects. Payload carries `{"name": "trustline"}`. Peer ACKs if it supports the module, enabling it on the link. In `open` mode, supported modules are auto-ACKed.

`link.close` — close a connection. Modules clean up before close via ref counting. If the peer has no link, a synthetic ACK is returned.

**trustline namespace:**

`trustline.connect` — establish bilateral peer relationship. Auto-accepted if the trustline module is active. Sets `Connected = true` on both sides. Rejected if the trustline is tombstoned (`!Connected && IntentClose`).

`trustline.close` — close a peer relationship. Both sides clear peer State, retaining data (balance, limits) for display until explicitly removed.

`trustline.set_credit_limit` — tell peer what credit limit I've set for them. Pending until peer confirms, then applied locally.

`trustline.set_demurrage` — tell peer my demurrage rate. Materializes existing demurrage before applying. Pending until peer confirms.

`trustline.payment` — send payment to peer. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `trustline.add_peer` (`{peer, modules?}`) — create link + peer with `IntentConnect`, initiates `link.connect` → `link.module` → `trustline.connect`. Optional `modules` list (e.g. `["loops"]`) proposes those modules on the link. Returns error if the link exists and is closing (`IntentClose`).
- `trustline.accept_peer` (`{peer, modules?}`) — accept incoming link + module + peer (sets `IntentConnect` at all three layers). Optional `modules` list proposes those modules on the link.
- `trustline.reject_peer` (`{peer}`) — reject incoming link (sends `link.close`)
- `trustline.remove_peer` (`{peer}`) — sets `IntentClose` on peer. Trustline close handshake runs; after close, the trustline becomes a tombstone (`!Connected && IntentClose`), preventing reconnection.
- `trustline.set_credit_limit` (`{peer, amount}`) — set credit limit, queue sync
- `trustline.set_demurrage` (`{peer, rate}`) — set demurrage rate, queue sync
- `trustline.payment` (`{peer, amount}`) — send payment to peer
- `trustline.status` — peers with credit data, link state, incoming invites
- `triad.status` — active novation count
- `loops.status` — active path and clearing count
- `link.set_modules` (`{peer, modules}`) — propose modules on an existing link. Modules not yet connected get `IntentConnect`; non-desired pending modules are removed. Trustline is never removed.
- `link.accept_module` (`{peer, module}`) — accept an incoming module proposal
- `link.reject_module` (`{peer, module}`) — reject an incoming module proposal
- `link.set_invite_mode` (`{mode}`) — set invite mode: `"closed"`, `"invite"` (default), or `"open"`
- `link.status` — raw link state and invite mode

**POST /p2p/tx** — bilateral sync (`link.*`, `trustline.*`, `triad.*`, `loops.*`)

**GET /p2p/key** — public key for signature verification
