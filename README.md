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

Two packages: `creditnet` (module root) and `creditnet/link` (bilateral
sync primitive). The link package has zero dependencies back to creditnet
and can be imported standalone.

```
link/
  link.go              Link, ConnectionState, sync lifecycle (ValidateRequest,
                       AcceptRequest, AcceptResponse), wire types, queue helpers

types.go               identities, Module/Transport interfaces, errors, helpers
node.go                Node: sync engine, handler dispatch, persistence
link_handler.go        link lifecycle (connect/close/module negotiation)
trustline.go           TrustlineHandler: credit limits, balances, payments
triad.go               TriadHandler: three-party novation for circular debt
demurrage.go           demurrage computation (decay over time)
server.go              Server, Config, module factory registration, P2P
account.go             namespace-routed account API dispatch
keys.go                Ed25519 signing and verification
users.go               auth, sessions, admin, argon2 password hashing
store.go               persistence (file-based)
run.go                 Boot/RunAndWait helpers

cmd/
  base/                server with built-in trustline module
```

Modules register under a namespace and receive sync dispatch. The trustline
module is built in. External modules (like triad) register via
`Server.Register(name, ModuleFactory)` before Init.

## Link layer

The link layer provides bilateral connections between nodes. Handlers register under a namespace and receive sync dispatch.

### Module negotiation

Links start as bare connections. Modules are proposed explicitly — when application code sets `IntentConnect` on a module's `ConnectionState`, the link handler sends a `link.module` message to the peer. The peer ACKs if it supports the module. Both sides track each module as a `ConnectionState` (same type as the link itself: `ConnectionStatus` + `IntentConnect`/`IntentClose`). Once a module is bilaterally `connected`, the node dispatches sync messages for that module on the link.

This means application-level traffic (trustline operations, triad novations) only flows after both sides have agreed on the module. Adding a new module to the network is backward-compatible — nodes that don't know it simply NACK the proposal.

### Connected gate

The node's `nextTx` always asks the link handler for pending messages (connect/close/module work regardless of connection state). For modules, `nextTx` requires `link.State.Status == Connected` and the module bilaterally active before asking. This means application-level sync methods (credit limits, payments, etc.) are only sent on connected links with the module enabled, without each module needing to check connection state itself.

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
                 +----------+              | (clears incoming,
                 |connected |<-------------+ sets IntentConnect)
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

On deletion (link.close completes), `LinkDeleted` is called on each module before the link is removed.

## Trustline layer

The trustline handler registers as a module under the `"trustline"` namespace. It manages per-peer credit data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), `balance`, and `demurrage` rates. Changes are transactional — a credit limit, demurrage change or payment is pending until the peer confirms.

### Peer lifecycle

```
  add_peer                        incoming link.connect
  (creates link +                 (from peer)
   local Peer, enqueues                |
   trustline.peer sync)                v
       |                          link incoming
       v                          (no peer yet)
  link waiting                         |
  peer exists locally             accept_peer / reject_peer
       |                          (AcceptLink / RejectLink)
       |                               |
  link.connect completes          link connects
  + module negotiation            + module negotiation
  + trustline.peer sent           + incoming trustline.peer
       |                          creates Peer
       v                               |
  peer synced                          v
  bilaterally                     peer exists
       |
  remove_peer
  (RemoveLink -> link.close)
       |
  LinkDeleted
  (peer.Disconnected = true)
       |
  remove_peer again
  (deletes peer data)
```

The initiator creates the `Peer` locally in `add_peer` and syncs it to the remote via `trustline.peer`. The receiver creates the `Peer` when the `trustline.peer` sync arrives. `LinkDeleted` marks a peer as disconnected, retaining its data (balance, limits) for display until explicitly removed with a second `remove_peer`.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Sync methods

Sync methods are namespace-routed: `namespace.method`. Node dispatches to the handler registered under that namespace.

**link namespace:**

`link.connect` — establish bilateral connection. Mutual add -> instant `connected`. Incoming from unknown -> `incoming` (NACKed, awaiting accept/reject).

`link.module` — bilateral module negotiation. Sent automatically after a link connects. Payload carries `{"name": "trustline"}`. Peer ACKs if it supports the module, enabling it on the link.

`link.close` — close a connection. `LinkDeleted` is called on each module, then the link is removed. If the peer has no link, a synthetic ACK is returned.

**trustline namespace:**

`trustline.peer` — establish bilateral peer relationship. Sent after link connects and module is enabled. Receiver creates a `Peer` if one doesn't exist.

`trustline.set_credit_limit` — tell peer what credit limit I've set for them. Pending until peer confirms, then applied locally.

`trustline.set_demurrage` — tell peer my demurrage rate. Materializes existing demurrage before applying. Pending until peer confirms.

`trustline.payment` — send payment to peer. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `trustline.add_peer` (`{peer}`) — create link, queue `link.connect` and `trustline.peer` sync
- `trustline.accept_peer` (`{peer}`) — accept incoming link
- `trustline.reject_peer` (`{peer}`) — reject incoming link (sends `link.close`)
- `trustline.remove_peer` (`{peer}`) — close link (`link.close`), peer marked disconnected. Call again to delete peer data.
- `trustline.set_credit_limit` (`{peer, amount}`) — set credit limit, queue sync
- `trustline.set_demurrage` (`{peer, rate}`) — set demurrage rate, queue sync
- `trustline.payment` (`{peer, amount}`) — send payment to peer
- `trustline.status` — peers with credit data, link state, incoming invites
- `triad.status` — active novation count
- `link.set_allow_invites` (`{enabled}`) — toggle incoming invites (default: true)
- `link.status` — raw link state and allow_invites setting

**POST /p2p/tx** — bilateral sync (`link.*`, `trustline.*`)

**GET /p2p/key** — public key for signature verification
