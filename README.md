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

creditnet is built on [mimesis](https://github.com/jhnngrn/mimesis), a
framework for federated p2p applications. Mimesis provides the bilateral
sync primitive, multi-tenant HTTP server, ed25519 peer authentication,
argon2 user auth, and file-based persistence. creditnet only implements
the application logic as modules.

```
github.com/jhnngrn/mimesis   (external)
  bilateral sync, server, crypto, auth, persistence, identity

creditnet.go           thin wrapper: Boot, RegisterNode, Register
types.go               creditnet-specific types and re-exports
node.go                Node: creditnet app implementing mimesis.App,
                       holds trustlines and bandwidth, registers reservers,
                       OnClose lifecycle, ClosedBalances archive,
                       trustline sync: credit limits, balances, payments
demurrage.go           demurrage computation (decay over time)
queue.go               generic enqueue/dequeue helpers

modules/
  triad/               three-party novation for circular debt elimination
  loops/               multi-hop loop clearing with hash-chain 2PC settlement

cmd/
  base/                server with creditnet app only
  triad/               server with creditnet + triad module
  loops/               server with creditnet + loops module
```

Node is the app — it implements `mimesis.App` (with `OnConnect` and
`OnClose` lifecycle hooks) and holds trustlines and bandwidth that other
modules depend on. `RegisterNode` registers it as `"creditnet"` with
`DeclareApp`. Child modules (triad, loops) receive trustlines and
bandwidth from the Node via `links.ModuleMap()["creditnet"]`. Different
binaries under `cmd/` combine different sets of child modules.

## Link layer

Link lifecycle (connect, close, module negotiation, invite mode) is
handled entirely by mimesis. See the
[mimesis README](https://github.com/jhnngrn/mimesis) for the link state
machine, module negotiation, and invite modes.

creditnet's Node implements `mimesis.App`, so it receives lifecycle
callbacks:

- **OnConnect(peer)** — called when `app.connect` completes bilaterally.
  No-op; trustline is created lazily on first use.
- **OnClose(peer)** — called when `app.close` completes bilaterally,
  just before the link is deleted. Archives non-zero balance to
  `ClosedBalances` and deletes the trustline if one exists.

Connected checks use link existence: `node.Get(peer)` returns non-nil
and `link.State.App == "creditnet"`. There is no separate connected
state on the trustline itself.

## Trustline layer

Trustline handling is built into Node under the `"creditnet"` namespace. It manages per-peer credit data: `credit_limit` (what I extend), `credit_limit_in` (what peer extends to me), `balance`, and `demurrage` rates. Changes are transactional — a credit limit, demurrage change or payment is pending until the peer confirms.

### Trustline lifecycle

Trustlines are created lazily and destroyed by the app destructor:

- **Creation** — a `Trustline{}` is created on first `set_credit_limit`
  or `set_demurrage` operation. Peers that connect but never set a credit
  limit leave no trustline state.
- **OnClose** archives non-zero balance to `ClosedBalances` and deletes
  the trustline when `app.close` completes.

There is no separate trustline state machine — link existence is the
sole source of truth. If `node.Get(peer)` is non-nil and
`link.State.App == "creditnet"`, the peer is connected.

The creditnet app's `Busy(peer)` always returns false — child modules
hold their own Busy, and trustline state is archived unconditionally
in `OnClose`.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

Response piggybacking: when sending a new request, the peer includes its response to our pending request. Both are processed atomically.

### Sync methods

Sync methods are namespace-routed: `namespace.method`. Node dispatches by namespace prefix.

**app namespace:**

`app.connect` — establish bilateral connection. Mutual add → instant `connected`. Incoming from unknown → `incoming` (NACKed, awaiting accept/reject). In `open` mode, auto-ACKed.

`app.close` — close a connection. Deferred until modules report not `Busy`. If the peer has no link, a synthetic ACK is returned.

**module namespace:**

`module.connect` — bilateral module negotiation. Sent automatically after a link connects. Payload carries `{"name": "triad"}`. Peer ACKs if it supports the module, enabling it on the link. In `open` mode, supported modules are auto-ACKed.

`module.close` — close a child module on a link. Deferred until the module reports not Busy.

**creditnet namespace:**

`creditnet.set_credit_limit` — tell peer what credit limit I've set for them. Creates trustline lazily. Pending until peer confirms, then applied locally.

`creditnet.set_demurrage` — tell peer my demurrage rate. Creates trustline lazily. Materializes existing demurrage before applying. Pending until peer confirms.

`creditnet.payment` — send payment to peer. Requires existing trustline. Receiver validates against their credit limit, both sides adjust balance on success.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token. Methods are namespace-routed (`namespace.method`):

- `creditnet.add_peer` (`{peer, modules?}`) — create link and initiate `app.connect`. Optional `modules` list (e.g. `["loops"]`) proposes child modules on the link.
- `creditnet.remove_peer` (`{peer}`) — initiate `app.close`. On completion, `OnClose` archives non-zero balance and deletes the trustline.
- `creditnet.set_credit_limit` (`{peer, amount}`) — set credit limit (creates trustline lazily), queue sync
- `creditnet.set_demurrage` (`{peer, rate}`) — set demurrage rate (creates trustline lazily), queue sync
- `creditnet.payment` (`{peer, amount}`) — send payment to peer (requires existing trustline)
- `status` — combined status from all modules: trustline peers with credit data, link state, incoming invites, triad novation count, loops path and clearing count

Link lifecycle (accept/reject peers, module negotiation, invite mode) is handled by mimesis directly.

**POST /p2p/tx** — bilateral sync (`app.*`, `creditnet.*`, `triad.*`, `loops.*`)

**GET /p2p/key** — public key for signature verification
