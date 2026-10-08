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
  types.go               wire types, Link, Handler/AccountHandler interfaces
  node.go                Node: sync engine, handler dispatch, retry
  handlers.go            linkHandler: link lifecycle (connect/disconnect/reject)

base/                    foundation layer — peers, credit, payments
  base.go                Handler: sync + account API for credit operations

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

The link layer (`link/`) provides bilateral connections between peers: establishing, accepting, rejecting and disconnecting. Handlers register under a namespace and receive sync dispatch. The link handler itself is registered as `"link"`, base as `"base"`, and modules add their own namespaces.

## Base layer

Base (`base/`) is the foundation that everything else builds on. It owns per-peer data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), and `balance`. Changes are transactional — a credit limit or payment is pending until the peer confirms. Base wraps link operations: `add_peer` creates a peer and triggers a link, `accept_peer` creates a peer and accepts an incoming link.

### Link state machine

```
                         add_peer              incoming link.connect
                        (account)              (allowInvites=true)
                            |                        |
                            v                        v
                      +-----------+            +----------+
                      |connecting |            | incoming  |
                      +--+--+--+--+            +-+------+-+
                         |  |  |                 |      |
     recv link.connect   |  |  | resp NACK       |      |  reject_peer
     (mutual add) -------+  |  +------+          |      |  (account)
                             |         |          |      |
               resp OK ------+         v          |      v
                             |    +---------+     |  +----------+
                             |    | waiting |     |  | rejecting |
                             |    +----+----+     |  +-----+----+
                             |         |          |        |
                             |  recv   |          |        | resp ACK'd
                             |  link.connect      |        v
                             |  from peer         |    (deleted)
                             |         |          |
                             v         v          | accept_peer
                          +----------+            | (account)
                          |connected |            |
                          +-----+----+            +---> connecting
                                |
                   remove_peer  |
                   (account)    |
                                v
                       +--------------+
                       |disconnecting |
                       +------+-------+
                              |
                              | resp ACK'd
                              v
                          (deleted)
```

**States:**

- **connecting** — outgoing `link.connect` in flight. Entered by `add_peer` or `accept_peer`.
- **waiting** — `link.connect` was NACKed. Peer has the invite (`incoming` on their side). Waiting for peer to `accept_peer` or `reject_peer`.
- **incoming** — received `link.connect` from peer. Awaiting local user decision (`accept_peer` or `reject_peer`).
- **connected** — bilaterally established. Both sides agree the link is up.
- **rejecting** — outgoing `link.reject` in flight. Entered by `reject_peer` on an incoming link.
- **disconnecting** — outgoing `link.disconnect` in flight. Entered by `remove_peer` on a connected link.

**Flows:**

*Mutual add* — both peers call `add_peer` independently (out-of-band coordination). First `link.connect` to arrive finds the other side already in `connecting` or `waiting` -> both transition to `connected` immediately.

*Incoming request* — unknown peer sends `link.connect`. If `allowInvites=true` (default), a link is created in `incoming` state and the connect is NACKed. Sender transitions to `waiting`. User can `accept_peer` (-> creates peer, `connecting` -> sends `link.connect` back -> `connected`) or `reject_peer` (-> `rejecting` -> sends `link.reject` -> link deleted on both sides, no peer created).

*Disconnect* — `remove_peer` on a `connected` link sets `disconnecting`. Sends `link.disconnect`, peer deletes their link and ACKs, we delete ours. The peer entry is also removed.

*Anti-spam* — set `allowInvites=false` via `link.set_allow_invites` to reject incoming connections from unknown peers. Sender stays in `connecting` and retries periodically — the connect goes through once the peer enables invites or adds the sender via `add_peer`. Links can always be established via mutual add.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Sync methods

Sync methods are namespace-routed: `namespace.method`. Node dispatches to the handler registered under that namespace.

**link namespace:**

`link.connect` — establish bilateral connection. Mutual add -> instant `connected`. Incoming from unknown -> `incoming` (NACKed).

`link.reject` — reject a connection. Peer deletes their link, ACKs.

`link.disconnect` — tear down a connection. Peer deletes their link, ACKs.

**base namespace:**

`base.set_credit_limit` — tell peer what credit limit I've set for them. Pending until peer confirms, then applied locally.

`base.payment` — send payment to peer. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `base.add_peer` (`{peer}`) — create peer + link, send `link.connect`
- `base.remove_peer` (`{peer}`) — remove peer + disconnect link
- `base.accept_peer` (`{peer}`) — create peer + accept incoming link
- `base.reject_peer` (`{peer}`) — reject incoming link (no peer created)
- `base.set_credit_limit` (`{peer, amount}`) — set credit limit, queue sync
- `base.payment` (`{peer, amount}`) — send payment to peer
- `base.status` — peers with credit data and link state, incoming invites
- `link.set_allow_invites` (`{enabled}`) — toggle incoming invites (default: true)
- `link.status` — raw link state and allow_invites setting

**POST /p2p/tx** — bilateral sync (`link.*`, `base.*`)

**GET /p2p/key** — public key for signature verification
