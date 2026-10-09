# creditnet

Distributed systems are hard because nodes live on different machines. Synchronization breaks, state diverges, race conditions appear, and developers spend most of their time on problems that only exist because the network is distributed.

The fix is simple: force every node-to-node interaction through a sequential, persisted, atomic sync protocol with guaranteed delivery. Every message is persisted before it is sent. Every response is persisted before it is returned. If a node crashes, it picks up exactly where it left off. Two nodes can never disagree about what happened, because they share a single counter and process one decision at a time.

The cost is that all interaction hits storage. But if you accept that cost, the entire class of distributed sync problems disappears. Developers write peer-to-peer applications as if all nodes were on the same machine — no sync logic, no conflict resolution, no defensive coding against network failures. The protocol handles it.

creditnet is a federated bilateral credit network built on this protocol. Each server hosts users identified as `alice@example.com`. Each user is a node with bilateral links to other nodes. The sync protocol lives in `link/` as a standalone package. Application logic is built as modules on top — credit limits, balances, payments, or anything else. Because the sync layer has already solved delivery, ordering and crash recovery, a module is just regular request/response logic with no distributed systems complexity. Anyone can write a module with their own custom logic and plug it in.

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

```
link/                    generic bilateral sync protocol
  types.go               wire types, Link, LinkOwner, Handler interfaces
  node.go                Node: sync engine, handler dispatch, owner registry
  handlers.go            linkHandler: link lifecycle (connect/close)

base/                    foundation layer — peers, credit, payments
  base.go                Handler: sync + account API for credit operations
  demurrage.go           demurrage computation (decay over time)

core/                    application layer (HTTP, auth, persistence)
  server.go              server, config, handler factory registration, P2P
  account.go             namespace-routed account API dispatch
  keys.go                Ed25519 signing and verification
  users.go               auth, sessions, admin, argon2 password hashing
  store.go               persistence (file-based)
  run.go                 Boot/RunAndWait helpers

cmd/
  base/                  server with base handler
```

`link/` is a standalone package — no dependency on `core/` or `base/`. It defines `Handler`, `AccountHandler`, `Transport` and `NodeStore` interfaces. `base/` imports `link/` only. `core/` imports `link/` only — handlers are registered via factories from `cmd/`.

## Link layer

The link layer (`link/`) provides bilateral connections between nodes. Handlers register under a namespace and receive sync dispatch. The link handler itself is registered as `"link"`, base as `"trustline"`, and modules add their own namespaces.

### Link ownership

Every link has an **owner** — a registered protocol that determines what runs on top of the link. The owner is identified by a string (e.g. `"trustline"`) stored in the link's `Owner` field and sent on the wire as `"protocol"` in the `link.connect` payload.

Owners register lifecycle callbacks via `RegisterOwner`:

- **OnConnect** (constructor) — called when the link reaches `connected`. Creates per-peer state for the protocol (e.g. base creates a `Peer` with credit data).
- **OnClose** (destructor) — called before the link is deleted. Cleans up per-peer state (e.g. base marks the peer as disconnected).

This separates concerns: the link layer handles the connection lifecycle, and the owner handles what happens at each transition. Peers only exist when a link is connected — no orphaned state.

### Connected gate

The node's `nextTx` always asks the link handler for pending messages (connect/close work regardless of connection state). For all other handlers, `nextTx` requires `link.State.Status == Connected` before asking. This means application-level sync methods (credit limits, payments, etc.) are only sent on connected links, without each handler needing to check connection state itself.

### Link state machine

```
                 add_peer / AddLink          incoming link.connect
                 (with protocol)             (with protocol in payload)
                        |                         |
                        v                         v
                  +-----------+             +----------+
                  |  waiting  |             | incoming  |
                  +--+--+--+-+             +-+------+-+
                     |  |  |                 |      |
  recv link.connect  |  |  |                 |      |  reject_link
  (mutual) ----------+  |  | RemoveLink     |      |  or RemoveLink
                         |  | (link.close)   |      |  (link.close)
            resp OK -----+  +------+        |      +------+
                         |          |        |             |
                         |     resp OK       |        resp OK
            resp NACK    |          |        |             |
            (stays       |          v        |             v
             waiting) <--+      (deleted)    |         (deleted)
                         |                   |
                         v                   | accept_link
                   +----------+              |
                   |connected | <-- OnConnect+---> waiting
                   +-----+----+
                         |
            RemoveLink   |
            (link.close) |
                         |
                    resp OK
                         |
                         v
                  OnClose + (deleted)
```

**States:**

- **waiting** — outgoing `link.connect` queued (carries protocol in payload). If NACKed, peer has the invite (`incoming` on their side).
- **incoming** — received `link.connect` from peer. Protocol validated against registered owners. Awaiting local decision (`accept_link` or `reject_link`).
- **connected** — bilaterally established. `OnConnect` fires, creating per-peer state for the protocol.

On deletion (close completes), `OnClose` fires before the link is removed.

## Base layer

Base (`base/`) is the foundation that everything else builds on. It registers as a link owner with protocol `"trustline"` and provides lifecycle callbacks that manage per-peer data.

When a link with protocol `"trustline"` reaches `connected`, the `OnConnect` callback creates a `Peer` with default values (zero balances, no limits). When the link is deleted, `OnClose` marks the peer as `disconnected`. There is no separate peer state machine — the link layer drives the lifecycle.

Peer data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), `balance`, and `demurrage` rates. Changes are transactional — a credit limit, demurrage change or payment is pending until the peer confirms.

### Peer lifecycle

```
  add_peer                        incoming link.connect
  (creates link with              (from peer with protocol="trustline")
   protocol="trustline")
       |                                |
       v                                v
  link waiting                     link incoming
  (no peer yet)                    (no peer yet)
       |                                |
  link.connect completes           accept_peer / reject_peer
       |                           (AcceptLink / RejectLink)
       v                                |
  OnConnect                        link connects -> OnConnect
  (creates Peer)                   (creates Peer)
       |                                |
       v                                v
  peer exists                      peer exists
  (connected)                      (connected)
       |
  remove_peer
  (RemoveLink -> link.close)
       |
  OnClose
  (peer.Disconnected = true)
       |
  remove_peer again
  (deletes peer data)
```

No peer exists until a link connects. Peers are born in `OnConnect` and marked disconnected in `OnClose`. A disconnected peer retains its data (balance, limits) for display until explicitly removed.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Sync methods

Sync methods are namespace-routed: `namespace.method`. Node dispatches to the handler registered under that namespace.

**link namespace:**

`link.connect` — establish bilateral connection. Payload carries `{"protocol": "trustline"}` to identify the link owner. Mutual add -> instant `connected` + `OnConnect`. Incoming from unknown -> `incoming` (NACKed, protocol stored on link).

`link.close` — close a connection. `OnClose` fires, then the link is deleted. If the peer has no link, a synthetic ACK is returned.

**trustline namespace:**

`trustline.set_credit_limit` — tell peer what credit limit I've set for them. Pending until peer confirms, then applied locally.

`trustline.set_demurrage` — tell peer my demurrage rate. Materializes existing demurrage before applying. Pending until peer confirms.

`trustline.payment` — send payment to peer. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `trustline.add_peer` (`{peer}`) — create link with protocol `"trustline"`, queue `link.connect`
- `trustline.accept_peer` (`{peer}`) — accept incoming link (peer created by `OnConnect` when link connects)
- `trustline.reject_peer` (`{peer}`) — reject incoming link (sends `link.close`)
- `trustline.remove_peer` (`{peer}`) — close link (`link.close`), peer marked disconnected by `OnClose`. Call again to delete peer data.
- `trustline.set_credit_limit` (`{peer, amount}`) — set credit limit, queue sync
- `trustline.set_demurrage` (`{peer, rate}`) — set demurrage rate, queue sync
- `trustline.payment` (`{peer, amount}`) — send payment to peer
- `trustline.status` — peers with credit data, link state, incoming invites
- `link.accept_link` (`{peer}`) — accept incoming link
- `link.reject_link` (`{peer}`) — reject incoming link
- `link.set_allow_invites` (`{enabled}`) — toggle incoming invites (default: true)
- `link.status` — raw link state and allow_invites setting

**POST /p2p/tx** — bilateral sync (`link.*`, `trustline.*`)

**GET /p2p/key** — public key for signature verification
