# creditnet

Federated bilateral credit network. Each server hosts users identified as `alice@example.com`. Each user is a node in the network with its own links. Servers communicate peer-to-peer over HTTP with Ed25519 signatures.

## Build

```
go build ./cmd/base         # core only — pure link layer
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
  types.go               wire types, Link, LinkState, identities, interfaces
  node.go                Node: sync engine, account ops, retry
  handlers.go            link lifecycle handlers, nextTx

core/                    application layer (HTTP, auth, persistence)
  server.go              server, config, P2P handler, retry loop
  account.go             user API dispatch
  keys.go                Ed25519 signing and verification
  users.go               auth, sessions, admin, argon2 password hashing
  store.go               persistence (file-based)
  run.go                 Boot/RunAndWait helpers

cmd/
  base/                  core only
```

`link/` is a standalone package — no dependency on `core/`. It defines `Transport` and `NodeStore` interfaces that `core/` implements.

## Core

Core provides a bilateral link layer: establishing, accepting, rejecting and disconnecting links between peers. No credit, no balances, no modules — pure transport.

### Link state machine

```
                         add_link              incoming link.connect
                        (account)              (allowInvites=true)
                            |                        |
                            v                        v
                      +-----------+            +---------+
                      |connecting |            | pending  |
                      +-+-+-+-----+            +-+-----+-+
                        | | |                    |     |
     recv link.connect  | | |  recv              |     |  reject_link
     request -----------+ | |  link.reject       |     |  (account)
     (mutual add)         | |  request           |     |
                          | |    |               |     v
               resp OK ---+ |    |               |  +----------+
                             |    |               |  | rejecting |
                             v    v               |  +-----+----+
                          +----------+            |        |
                          |connected |            |        | resp ACK'd
                          +-----+----+            |        v
                                |                 |    (deleted)
                   remove_link  |                 |
                   (account)    |                 | accept_link
                                v                 | (account)
                       +--------------+           |
                       |disconnecting |           +---> connecting
                       +------+-------+
                              |
                              | resp ACK'd
                              v
                          (deleted)
```

**States:**

- **pending** — incoming `link.connect` from unknown peer. Awaiting local user acceptance. The connect is NACKed; peer knows we received it.
- **connecting** — outgoing `link.connect` in flight. Entered by `add_link` or `accept_link`.
- **connected** — bilaterally established. Both sides agree the link is up.
- **rejecting** — outgoing `link.reject` in flight. Entered by `reject_link` on a pending link.
- **disconnecting** — outgoing `link.disconnect` in flight. Entered by `remove_link` on a connected link.

**Flows:**

*Mutual add* — both peers call `add_link` independently (out-of-band coordination). First `link.connect` to arrive finds the other side already in `connecting` -> both transition to `connected` immediately.

*Incoming request* — unknown peer sends `link.connect`. If `allowInvites=true` (default), a link is created in `pending` state and the connect is NACKed. User can `accept_link` (-> `connecting` -> sends `link.connect` back -> `connected`) or `reject_link` (-> `rejecting` -> sends `link.reject` -> link deleted on both sides).

*Disconnect* — `remove_link` on a `connected` link sets `disconnecting`. Sends `link.disconnect`, peer deletes their link and ACKs, we delete ours.

*Anti-spam* — set `allowInvites=false` via `set_allow_invites` to reject all incoming connections from unknown peers. Links can still be established via mutual add.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Core sync methods

`link.connect` — establish bilateral connection. Mutual add -> instant `connected`. Incoming from unknown -> `pending` (NACKed).

`link.reject` — reject a connection. Peer deletes their link, ACKs.

`link.disconnect` — tear down a connection. Peer deletes their link, ACKs.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token:

- `add_link` (`{peer}`) — create link, send `link.connect`
- `remove_link` (`{peer}`) — disconnect or remove link
- `accept_link` (`{peer}`) — accept pending link, send `link.connect`
- `reject_link` (`{peer}`) — reject pending link, send `link.reject`
- `set_allow_invites` (`{enabled}`) — toggle incoming invites (default: true)
- `status` — list links with state, allow_invites setting

**POST /p2p/tx** — bilateral sync (`link.connect`, `link.disconnect`, `link.reject`)

**GET /p2p/key** — public key for signature verification
