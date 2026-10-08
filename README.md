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

The link layer (`link/`) provides bilateral connections between nodes: establishing, accepting, rejecting and disconnecting. Handlers register under a namespace and receive sync dispatch. The link handler itself is registered as `"link"`, base as `"base"`, and modules add their own namespaces.

Links are general-purpose channels. What runs on top (peers, payments, or something else) is determined by the handler layer. A link can exist without a peer — accepting a link and creating a peer are separate operations.

Links track a reference count (`Refs`). Handlers call `RefLink`/`UnrefLink` when claiming or releasing a link. `RemoveLink` and incoming `link.disconnect` are refused while refs > 0, enforcing that the application layer is wound down before a link can be disconnected.

### Link state machine

```
                     add_peer / AddLink         incoming link.connect
                                                (allowInvites=true)
                            |                        |
                            v                        v
                      +-----------+            +----------+
                      |connecting |            | incoming  |
                      +--+--+--+--+            +-+------+-+
                         |  |  |                 |      |
     recv link.connect   |  |  | resp NACK       |      |  link.reject_link
     (mutual) ----------+  |  +------+          |      |  (account)
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
                             v         v          | link.accept_link
                          +----------+            | (account)
                          |connected |            |
                          +-----+----+            +---> connecting
                                |
                   RemoveLink   |  (refused if refs > 0)
                                |
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

- **connecting** — outgoing `link.connect` in flight.
- **waiting** — `link.connect` was NACKed. Peer has the invite (`incoming` on their side).
- **incoming** — received `link.connect` from peer. Awaiting local decision (`link.accept_link` or `link.reject_link`).
- **connected** — bilaterally established.
- **rejecting** — outgoing `link.reject` in flight.
- **disconnecting** — outgoing `link.disconnect` in flight. Refused while refs > 0.

## Base layer

Base (`base/`) is the foundation that everything else builds on. It owns per-peer data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), `balance`, and `demurrage` rates. Changes are transactional — a credit limit, demurrage change or payment is pending until the peer confirms.

Links and peers are separate. A link is the communication channel (link layer). A peer is what base adds on top: balance, credit limits, demurrage. Establishing a peer requires a bilateral `peer_request` exchange after the link is connected.

### Peer state machine

```
                     add_peer                   recv base.peer_request
                    (account)                   (link already connected)
                        |                              |
                        v                              v
                  +-----------+                  +----------+
                  |connecting |                  | incoming  |
                  +--+-----+--+                  +-+------+-+
                     |      |                      |      |
  recv peer_request  |      | resp NACK            |      |  reject_peer
  (mutual) ---------+      +------+               |      |  (account)
                     |              |               |      |
       resp OK ------+              v               |      v
                     |        +---------+           |  +----------+
                     |        | waiting |           |  | rejecting |
                     |        +----+----+           |  +-----+----+
                     |             |                |        |
                     |  recv       |                |        | resp ACK'd
                     |  peer_request                |        | -> delete peer
                     |  from peer  |                |        |    + UnrefLink
                     |             |                |        v
                     v             v                |    (deleted)
                  +----------+                      |
                  |connected |                      | accept_peer
                  +-----+----+                      | (account)
                        |                           |
           remove_peer  |                           +---> connecting
           (account)    |
                        v
                   (deleted)
                   + UnrefLink
```

**States:**

- **connecting** — `peer_request` queued, not yet acked.
- **waiting** — `peer_request` was NACKed. Peer has the request (`incoming` on their side).
- **incoming** — received `peer_request`. Awaiting local decision (`accept_peer` or `reject_peer`).
- **connected** — bilaterally confirmed. Operations (credit limit, demurrage, payment) require this state.
- **rejecting** — outgoing `peer_reject` in flight.
- **rejected** — denied by peer (received `peer_reject`).

**Flows:**

*Add peer* — `add_peer` creates a link and a peer in `connecting`. After the link connects, `peer_request` is sent. If the other side has no peer, it creates one in `incoming` and NACKs. Sender transitions to `waiting`. Acceptor calls `accept_peer` -> `connecting` -> sends `peer_request` back -> both sides `connected`.

*Pre-accept* — acceptor calls `accept_peer` before `peer_request` arrives (while link is still `incoming`). This accepts the link and creates a peer in `connecting`. When the initiator's `peer_request` arrives, it finds `connecting` -> `connected` + ACK.

*Mutual add* — both sides call `add_peer` independently. First `peer_request` to arrive finds the other side in `connecting` -> `connected` immediately.

*Reject* — `reject_peer` on an `incoming` peer sets `rejecting`, sends `peer_reject`. The rejected side transitions to `rejected` (visible in their status). After ACK, rejector deletes their peer and unrefs the link.

*Anti-spam* — set `allowInvites=false` via `link.set_allow_invites` to reject incoming connections from unknown peers.

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

`base.peer_request` — declare intent to establish a peer relationship. If the other side is already in `connecting` or `waiting`, both transition to `connected` (ACK). Otherwise a new `incoming` peer is created (NACK).

`base.peer_reject` — reject a peer request. The rejected side transitions to `rejected` (visible in status). Rejector deletes their peer and unrefs the link after ACK.

`base.set_credit_limit` — tell peer what credit limit I've set for them. Pending until peer confirms, then applied locally.

`base.set_demurrage` — tell peer my demurrage rate. Materializes existing demurrage before applying. Pending until peer confirms.

`base.payment` — send payment to peer. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `base.add_peer` (`{peer}`) — create link + peer, queue `link.connect` then `peer_request`
- `base.accept_peer` (`{peer}`) — accept incoming `peer_request`, or pre-accept link + create peer if no request yet
- `base.reject_peer` (`{peer}`) — reject incoming `peer_request`, or reject link if no peer yet
- `base.remove_peer` (`{peer}`) — delete peer, unref link
- `base.set_credit_limit` (`{peer, amount}`) — set credit limit, queue sync
- `base.set_demurrage` (`{peer, rate}`) — set demurrage rate, queue sync
- `base.payment` (`{peer, amount}`) — send payment to peer
- `base.status` — peers with credit data, link state, incoming invites
- `link.accept_link` (`{peer}`) — accept incoming link
- `link.reject_link` (`{peer}`) — reject incoming link
- `link.set_allow_invites` (`{enabled}`) — toggle incoming invites (default: true)
- `link.status` — raw link state and allow_invites setting

**POST /p2p/tx** — bilateral sync (`link.*`, `base.*`)

**GET /p2p/key** — public key for signature verification
