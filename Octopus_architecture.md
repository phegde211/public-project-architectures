# Octopus — Transparent Network Proxy

**Author:** [Prabhat Hegde](https://github.com/phegde211) (prabhat.hegde@gmail.com)

**Private source repository:** [phegde211/octopus](https://github.com/phegde211/octopus)

The source repository is private. Please contact the author, Prabhat Hegde,
to request permission and access to the implementation.

---

# Octopus — Architecture

Octopus (`octo`) is a multi-threaded, TLS-terminating network proxy written in C.
It accepts mutual-TLS (mTLS) connections from clients, opens its own mTLS
connection to an upstream server, and shuttles traffic between the two. Because
it terminates TLS on *both* sides, it sits on the **plaintext** path and can
inspect or transform traffic (the "brain") and record per-connection state in an
in-memory hash table (the **GST**). Each concurrent connection is a "tentacle".

The same binary can also run as a **replay/relay server** (`OCTO_ECHO_SERVER=1`):
it terminates the client TLS and echoes the decrypted bytes straight back instead
of forwarding upstream. Chaining two instances (a proxy in front of a replay
server) is how the end-to-end tests exercise the full path.

This document is the deep reference. For a shorter summary see `DESIGN.md`; for
the debugging history of the non-blocking rework and known issues see
`ASYNC_DEBUG_NOTES.md`; to run the tests see `tests/README.md`.

---

## 1. Repository layout

```
  src/                 all C sources
    octo.c             core engine: socket contexts, epoll loop, worker pool,
                       async handshake state machine, data path, cert reload
    networking.c       sockets, epoll helpers, non-blocking connect, OpenSSL wrappers
    gst.c              the GST hash table (generic key -> value store)
    timer.c            generic timer infrastructure (stats printer)
    octo_stats.c       counters + periodic stats printing
    logging.c          timestamped stdout logging
    start_octo.c       main(): reads OCTO_* env and starts the service
  include/             all headers
    octo.h             core structs (socket_context, data node, global ctx), enums
    networking.h  gst.h  gst_handler.h  logging.h  octo_stats.h  timer.h
  obj/                 build objects (generated, git-ignored)
  Makefile             `make`, `make asan`, `make clean`
  tests/               test client + cert generation + run scripts + guide
  certs/               generated mTLS certificates (git-ignored)
  architecture.md      this file
  DESIGN.md            short design summary
  ASYNC_DEBUG_NOTES.md non-blocking rework history + known issues + dead ends
```

Build: `make` → `./octo`. `make asan` → `./octo-asan` (AddressSanitizer).
All local `#include`s are unqualified (`"octo.h"`); the compiler finds them via
`-Iinclude`.

---

## 2. High-level block diagram

```
                                   +--------------------------------------------+
                                   |                   octo                     |
                                   |                                            |
  +----------+   TLS (mTLS)        |  +-----------+          +-----------+      |  TLS (mTLS)    +-----------+
  |  client  | <=================> |  | client-   |  bridge  | upstream  |      | <===========> |  server   |
  |          |                     |  | facing    | <======> | socket    |      |               | (upstream)|
  +----------+                     |  | socket    |          | (backend) |      |               +-----------+
                                   |  +-----------+          +-----------+      |
                                   |       |  \               /  |              |
                                   |    inQ|   v             v   |inQ           |
                                   |   outQ|  +-----------------+ |outQ          |
                                   |       |  |  GST hash table | |             |
                                   |       |  | (bookkeeping;   | |             |
                                   |       |  |  inert today)   | |             |
                                   |       |  +-----------------+ |             |
                                   +--------------------------------------------+

  Legend:  <===>  TLS-encrypted link.
           bridge = the two sockets are paired (linked_sock_ctx) and share one
           owner worker thread.  inQ/outQ = per-socket plaintext queues (§6).
```

### Two-hop topology used by the tests

```
   client            octo (proxy, :14000)          octo (replay, :15000)
     |                       |                              |
     |===TLS: request=======>|                              |
     |                       |===TLS: forwarded payload====>|
     |                       |                              | (echo)
     |                       |<==TLS: echoed payload========|
     |<==TLS: reply==========|                              |
```

Every link is mutually authenticated. Both boxes are the same `octo` binary; the
replay box is just `octo` with `OCTO_ECHO_SERVER=1`.

---

## 3. Thread model

`octo` runs **1 master thread + N worker threads**, plus a cert-watcher thread and
zero-or-more timer threads. `N = OCTO_EPOLL_WTHREADS - 1` (default 4 → 3 workers).

```
                                     process
  +--------------------------------------------------------------------------+
  |                                                                          |
  |   +-------------------+                 +-----------------------------+  |
  |   |   master thread   |                 |      worker threads (N)     |  |
  |   |                   |   post task     |                             |  |
  |   |  epoll_wait() on  | --------------> |  tid 0 :  [ taskQ[0] ] <-+  |  |
  |   |  ONE shared epfd  |  cond_signal    |  tid 1 :  [ taskQ[1] ]   |  |  |
  |   |                   |                 |   ...                    |  |  |
  |   |  - accept() loop  |                 |  tid N-1: [ taskQ[N-1] ] |  |  |
  |   |    on listeners   |                 |                          |  |  |
  |   |  - post read/write|                 |  each worker drains ONLY |  |  |
  |   |    tasks to the   |                 |  its own queue (by tid)  |  |  |
  |   |    OWNING worker  |                 +-----------------------------+  |
  |   +-------------------+                                                  |
  |            |                                                            |
  |   +-------------------+    +----------------------+                     |
  |   |  cert watcher     |    |  timer thread(s)     |                     |
  |   |  (inotify on the  |    |  (stats printer;     |                     |
  |   |   cert directory) |    |   generic, reusable) |                     |
  |   +-------------------+    +----------------------+                     |
  +--------------------------------------------------------------------------+

  Per-worker state (one set per tid):  taskQ[tid]  taskQlock[tid]  cond_var[tid]
```

- The **master** runs the only `epoll_wait` loop. On a listener event it drains
  the whole accept backlog (§5); for every other ready fd it wraps the socket in a
  task and hands it to the *owning* worker's queue. It performs no payload I/O and
  — critically — **never blocks** (all connect/handshake work is non-blocking, §5).
- Each **worker** blocks on its own condition variable, drains its own task queue,
  and runs the socket's handler (handshake, read, or write).
- The **cert watcher** hot-reloads TLS certificates when the files on disk rotate.
- **Timer threads** are generic plumbing; the only current user is the optional
  periodic stats printer.

The per-worker task queues, locks and condition variables are initialised in
`start_octo_service()` **before any thread is created** — initialising them inside
the master thread (as older code did) races with workers that are already reading
them.

---

## 4. The concurrency invariant (the load-bearing rule)

A single OpenSSL `SSL` object must never be used by two threads at once —
concurrent `SSL_read`/`SSL_write` on the same `SSL*` corrupts its state. The rule:
**pin every socket (and therefore its `SSL` object) to exactly one owner worker
thread, and serialize all of its work through that thread.**

Two mechanisms enforce it:

1. **Owner-thread affinity.** Every `socket_context` carries a fixed `tid`
   assigned round-robin at accept time. All work for that socket is routed to
   `taskQ[tid]`, so only that one worker ever touches its `SSL`.

2. **`EPOLLET | EPOLLONESHOT`.** After a socket fires an event it is *disarmed* in
   the interest list, and only re-armed (via `mod_epoll`) once its owner finishes
   handling it. So a socket has at most one event in flight and its owner is never
   handed two overlapping tasks for it.

**Bridged sockets share a `tid`.** A client-facing socket and its upstream
(backend) socket are paired (`linked_sock_ctx`) and get the *same* owner thread,
so a read on one side and a write on the other never run concurrently, and the
common case (read client → write backend) needs no cross-thread hop.

```
   client-facing socket S1            backend socket S2
   +--------------------+   linked    +--------------------+
   |  type = CLIENT     |<===========>|  type = BACKEND    |
   |  tid  = X          |             |  tid  = X  (same)  |
   |  ssl, inQ, outQ    |             |  ssl, inQ, outQ    |
   +--------------------+             +--------------------+
             \___________  worker X owns both  ___________/
```

### Cross-thread handoff

When a thread must send on a socket it does *not* own, it never calls `SSL_write`
directly. It enqueues the bytes on that socket's `outQ` and posts an `EPOLLOUT`
task to the **owner** thread (`octo_create_task`). Any thread — master or worker —
may post a task to any worker's queue; the queue is guarded by `taskQlock[tid]`.
The refcount taken in `octo_create_task` keeps the target alive across the hop.

---

## 5. Non-blocking connection setup (the async handshake state machine)

**Everything is non-blocking and epoll-driven — including TCP connect and the TLS
handshakes.** This is the core of the engine. (Older code did the frontend
`SSL_accept`, the upstream `connect()` and `SSL_connect` *synchronously* in the
master thread; under many simultaneous connections this stalled the single epoll
loop and dropped replies. See `ASYNC_DEBUG_NOTES.md`.)

Each `socket_context` has an `hs_state` (`enum octo_hs_state`):

```
  HS_DATA        handshake complete; normal read/write data phase (default, == 0)
  HS_ACCEPT      frontend: non-blocking SSL_accept in progress
  HS_TCP_CONNECT backend:  non-blocking connect() in progress (wait EPOLLOUT)
  HS_TLS_CONNECT backend:  non-blocking SSL_connect in progress
```

The worker loop routes any task whose socket has `hs_state != HS_DATA` to
`octo_handshake_handler`, which drives the handshake one step and re-arms the
socket for whichever event OpenSSL is waiting on (`SSL_get_error` →
`WANT_READ`=EPOLLIN / `WANT_WRITE`=EPOLLOUT). On completion it transitions the
socket into `HS_DATA`; on fatal error it cleans up (and its bridged peer).

Lifecycle of one proxied connection:

```
  master: listener EPOLLIN
    accept_connection() [non-blocking, loop until EAGAIN drains the backlog]
    octo_setup_client_conn(fd):
        create client ctx, SSL_new (no handshake), tid = round-robin
        add to clientQ, add_epoll(EPOLLIN), hs_state = HS_ACCEPT
        (master returns immediately - never blocks)
        |
  worker X: client EPOLLIN -> octo_handshake_handler (HS_ACCEPT)
        SSL_accept() -> WANT_READ/WANT_WRITE ? re-arm and wait : done
        on done: octo_client_setup_backend(client):
            octo_tcp_connect_nb() to upstream  [non-blocking]
            SSL_new for backend (no handshake)
            bridge client<->backend (linked_sock_ctx, refcounts)
            add_epoll_ev(backend, EPOLLOUT), hs_state =
                HS_TCP_CONNECT (connect in flight) or HS_TLS_CONNECT (connected)
            NOTE: client is left UN-armed until the backend is ready, so no
                  request read can race ahead of an existing backend.
        |
  worker X: backend EPOLLOUT -> octo_handshake_handler
        HS_TCP_CONNECT: getsockopt(SO_ERROR) ok? -> HS_TLS_CONNECT (fall through)
        HS_TLS_CONNECT: SSL_connect() -> WANT_* ? re-arm : done
        on done: octo_handshake_complete_backend():
            backend hs_state = HS_DATA, arm backend EPOLLIN
            client  hs_state = HS_DATA, post an EPOLLIN task to the client
              (a task, not just a re-arm: the client may have already sent its
               request while the backend was handshaking, and that buffered data
               has no fresh edge coming - the task drains it now)
        |
  DATA PHASE: request/reply flow over the bridge (§7)
```

Special cases handled by `octo_client_setup_backend`:
- **echo/replay** (`OCTO_ECHO_SERVER=1`): no backend; after the frontend handshake
  the client is armed for read and echoes on the same socket.
- **remote/peer** clients (`OCTO_PEER_LISTEN_PORT`): the backend to the local
  server is created lazily on GST lookup, not here.

---

## 6. Queues: per-socket inQ/outQ and per-worker taskQ

Two different queue kinds, easy to conflate:

```
  Per WORKER (thread):  taskQ[tid]   - a queue of octo_thread_task (work items).
                                       The master/other workers push; the owner
                                       worker pops. Guarded by taskQlock[tid],
                                       signalled by cond_var[tid].

  Per SOCKET (context): ctx->inQ     - decrypted bytes read IN, awaiting routing.
                        ctx->outQ    - bytes queued to be encrypted and written OUT.
                                       Guarded by ctx->outQlock. Each queue holds
                                       octo_data nodes.
```

```
        SSL_read                    route (GST hook, framing)               SSL_write
  wire ---------> [ ctx.inQ ] ---> octo_process_incoming_data ---> [ dest.outQ ] ---------> wire
                  (reassembly)          (per node)                  (backpressure)
```

`octo_data` node (see `include/octo.h`) carries the payload plus the reassembly
bookkeeping used on the client-facing side: `tlv_type`, a 4-byte length being
accumulated (`tlv_len`, `tlv_len_bytes`), `app_data`/`app_data_len`/
`app_cur_offset`, and `app_data_complete`.

---

## 7. Data path (data phase)

### 7.1 Request (client → upstream), proxy mode

```
  worker X: client EPOLLIN
    octo_read_handler(client)
      octo_ssl_recv(client)                 [drain fully; loops until WANT_READ,
                                             and octo_read_handler repeats while
                                             the read buffer comes back full]
      octo_enqueue_incoming_data(client)    [length-prefixed reassembly -> inQ]
      octo_process_incoming_data(client)
        for each COMPLETE node (app_data_complete):
          [GST hook: no-op today]
          strip META_DATA_LEN prefix, move node -> backend.outQ
          backend owned by X -> octo_write_handler(backend) -> SSL_write
```

Only fully-reassembled nodes are processed; the framer stops at the first
incomplete node (always the tail) and leaves it in `inQ` for the next read.

### 7.2 Reply (upstream → client), proxy mode

```
  worker X: backend EPOLLIN
    octo_read_handler(backend) -> octo_process_incoming_data(backend)
      req_node = first node on backend.outQ (the sent request, kept as sent=1)
      if req_node present (first reply chunk):
          prepend REQ_ID_LEN request id to the reply, move -> client.outQ
      else (continuation chunk of a multi-read reply):
          relay the bytes straight to client.outQ (no re-prepend)
      client owned by X -> octo_write_handler(client) -> SSL_write
```

### 7.3 Replay/echo mode (`OCTO_ECHO_SERVER=1`)

An **opaque relay**: no backend, no framing assumptions. The decrypted bytes are
echoed straight back on the same socket. This is what lets two instances chain —
the proxy strips its framing header before forwarding, so the downstream instance
must treat what it receives as opaque.

---

## 8. epoll — how it is used here

octo uses a **single** epoll instance shared by the master and all workers, in
**edge-triggered one-shot** mode (`EPOLLET | EPOLLONESHOT`). Consequences and the
rules they impose:

- **One event per socket in flight.** `EPOLLONESHOT` disarms a socket the moment
  it fires. It stays disarmed until its owner worker explicitly re-arms it with
  `mod_epoll` (EPOLL_CTL_MOD). This is what guarantees the concurrency invariant
  (§4): the master cannot hand two overlapping tasks for the same socket, and only
  the owner ever re-enables it.

- **Edge-triggered ⇒ you must fully drain.** An edge fires only on a *transition*
  (new data arrives, socket becomes writable). If a handler reads only part of
  what is buffered, no new edge comes for the remainder and it stalls forever.
  So:
  - the **listener** is drained with an `accept()` loop until `EAGAIN`
    (`octo_clients_accept_handler`) — a burst of N simultaneous SYNs is a single
    edge;
  - `octo_read_handler` loops until the read buffer comes back not-full
    (`octo_ssl_recv` internally reads until `WANT_READ`, but is bounded by the
    2 MiB buffer, so the handler re-reads while the buffer fills completely).

- **Arming for the right event.** During a handshake the socket is armed for
  EPOLLIN or EPOLLOUT depending on `SSL_get_error` (`WANT_READ`/`WANT_WRITE`); a
  backend TCP connect in flight is armed for EPOLLOUT (writable == connected).

- **Re-arm vs. post-a-task.** Re-arming (`mod_epoll`) waits for the *next* edge.
  When data may already be buffered with no edge to come (e.g. a request that
  arrived while the backend was still handshaking), the code instead **posts an
  explicit EPOLLIN task** so the owner drains it immediately.

- **Cross-thread writes** don't touch epoll from the wrong thread: the writer
  enqueues to `outQ` and posts an EPOLLOUT task to the owner (§4).

`EPOLLOUT` is otherwise only armed when a write could not complete (SSL_write
returned `WANT_WRITE`); in the common case a read is immediately followed by a
write on the paired socket with no EPOLLOUT round-trip.

---

## 9. Connection lifecycle & reference counting

Each `socket_context` is reference counted (`ref_count` under `mlock`). It is
incremented when the context is: placed on a global list (`clientQ`/`o_remoteQ`),
added to epoll, bridged to its partner, or referenced by a queued task. It is
decremented on cleanup and on task completion. `octo_socket_ctx_decr` frees the
context when the count reaches zero.

```
  accept / connect
       |  create ctx --> ref++ (list) --> ref++ (epoll) --> ref++ (bridge)
       v
  ... normal operation: each in-flight task holds a ref ...
       |
       v  peer closes / error (EPOLLHUP) OR a handler returns < 0
  octo_sock_context_cleanup:
     mark CTX_DEAD; SSL_shutdown; close(fd)
     remove from epoll (ref--); remove from global list (ref--)
     for the bridged partner: drop the mutual back-pointers (ref--, ref--),
       then clean up the partner too (symmetric: whichever is freed second
       frees only itself)
       |
       v  when ref_count reaches 0
  octo_sock_context_free:
     drain inQ/outQ, free queued nodes, remove matching tasks from taskQ, free ctx
```

Cleanup is inline (refcount-driven), not timer-driven. Before running a handler,
the worker checks `CTX_DEAD` and drops the task if the socket is already being
torn down — this is what protects an in-flight handshake/data task whose bridged
peer was cleaned up underneath it.

---

## 10. Wire framing

On the client-facing side in proxy mode a message is
`<1-byte type><4-byte length><value>`, where `value` begins with a fixed
`META_DATA_LEN` (37-byte) metadata prefix (request id, tenant id, intent, session
id, hash) followed by the application payload. `octo_enqueue_incoming_data`
reassembles this across partial reads. When forwarding upstream the proxy strips
the 37-byte prefix; on the reply it prepends the `REQ_ID_LEN` (8-byte) request id
so a reply can be matched to its request.

Because the proxy strips the prefix before forwarding, a downstream instance must
be an **opaque relay** (replay mode) rather than expecting the same frame — this
is exactly why replay mode bypasses the framer. The bundled test client produces
this frame (`tests/octo_test_client.c`).

---

## 11. TLS / mutual TLS

OpenSSL over non-blocking sockets. Four SSL contexts, one per trust relationship:

```
  TLS_SERVER_LOCAL   accept connections from clients
  TLS_SERVER_PEER    accept connections from remote (peer) proxies
  TLS_CLIENT_SERVER  connect to the upstream server
  TLS_CLIENT_PEER    connect to a remote (peer) proxy
```

Every context sets `SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT`, i.e. mutual
TLS. Hostnames are not checked, so any CA-signed chain validates. Server contexts
are pinned to TLS 1.2. `octo_get_ssl_error` normalizes `SSL_get_error()` into
errno-style codes (`EAGAIN`, `EBADF`, …) so the engine branches uniformly.

**Cert hot-reload:** the cert-watcher thread watches the cert directory with
inotify; when the CA, cert and key files have all rotated it reloads them into the
live `SSL_CTX`. Existing connections keep their old material; only new handshakes
pick up the new certs.

---

## 12. GST hash table & timers

`gst.c` is a real hash table (FNV-1a keys, per-table mutex, chained buckets). It
sits on the data path so future features can record per-connection information
extracted from a payload; the interpretation hooks in `gst_handler.h` are the
seams. With no payload to interpret today the hooks are no-ops and the table stays
empty. `timer.c` is generic timer plumbing; the only current caller is the
optional periodic stats printer. Neither is involved in connection cleanup.

---

## 13. Configuration (`OCTO_*` environment variables)

| Variable | Meaning |
|---|---|
| `OCTO_LISTEN_HOST` / `OCTO_LISTEN_PORT` | Address the proxy listens on for clients |
| `OCTO_SERVER_HOST` / `OCTO_SERVER_PORT` | Upstream server to forward to |
| `OCTO_PEER_LISTEN_PORT` | Port for incoming remote-proxy (peer) connections |
| `OCTO_CERT_PATH` | Directory holding the certificate files |
| `OCTO_TLS_SERVER_{CA,CERT,KEY}` | Certs to accept client connections |
| `OCTO_TLS_CLIENT_{CA,CERT,KEY}` | Certs to connect to the upstream server |
| `OCTO_TLS_PEER_SERVER_{CA,CERT,KEY}` | Certs to accept peer-proxy connections |
| `OCTO_TLS_PEER_CLIENT_{CA,CERT,KEY}` | Certs to connect to a peer proxy |
| `OCTO_EPOLL_WTHREADS` | Total epoll threads (1 master + workers), default 4 |
| `OCTO_DEBUGLEVEL` | `LOG_ERROR` / `LOG_INFO` / `LOG_DEBUG` / `LOG_VERBOSE` |
| `OCTO_ECHO_SERVER` | `1` = run as a replay/relay server instead of proxying |
| `OCTO_STATS_ENABLED` | `1` = enable periodic stats printing |

The cert `*_{CA,CERT,KEY}` variables are file **names**; octo joins them onto
`OCTO_CERT_PATH`.

---

## 14. Build & test

```sh
make            # -> ./octo
make asan       # -> ./octo-asan (AddressSanitizer)
make clean

cd tests && ./gen_certs.sh   # one-time: generate mTLS material into ../certs
```

See `tests/README.md` for the full test guide (commands, the file-split
integrity client, concurrency and byte-boundary sweeps).

---

## 15. Large single-connection transfers (full-duplex correctness)

Large transfers on a *single* connection stress the fact that a bridged proxy
connection is a **full-duplex byte pipe**: the client's request is still being
written to the upstream while the upstream's reply is already streaming back. A
series of bugs here were all symptoms of the code assuming a strict, non-
overlapping request→reply order. They were fixed on 2026-07-13 by removing that
coupling:

1. **`octo_ssl_recv` read-buffer math** — an extra decrement capped every read at
   ~half the 2 MiB buffer, stranding the rest (§8 also covers the read drain loop).
2. **Reply correlated through the backend write queue.** `backend->outQ` was
   overloaded as both the write queue and the outstanding-request registry; the
   reply path recovered the request id from `TAILQ_FIRST` and freed that node on
   the first reply chunk. That breaks whenever request and reply overlap. Fixed by
   **decoupling**: the request id and a `reply_started` flag live on the
   connection (`reply_req_id`/`reply_started`, §6), captured once when the request
   is forwarded; the reply direction prepends the id once and then streams raw,
   never consulting the write queue. Bridged backend request nodes are freed on
   full send like any other write.
3. **`octo_write_handler` continued after a would-block.** On `WANT_WRITE` it did
   `break`, which only leaves the `switch`; the `TAILQ_FOREACH` then wrote the
   next queued node while OpenSSL still expected a retry of the same buffer →
   `SSL routines::bad write retry`, killing the connection. Fixed by stopping the
   write loop on would-block (EPOLLOUT is armed to resume).

Verified: 87/87 byte-boundary × concurrency combos (1 B…16 MiB × nconns 1…32) on
both the normal and ASAN builds, plus stress (64 MiB×1, 16 MiB×64, 32 MiB×32,
1 B×64) — all pass, no proxy/replay errors, no ASAN reports. See
`ASYNC_DEBUG_NOTES.md` for the detailed evidence and the dead ends already ruled
out.

Note on scope: this decoupled reply model is correct for the **bridged 1:1 proxy**
(one outstanding request/reply stream per connection). The remote-octo / GST
(`rm_backend_ctx`, N:1) path still correlates replies to requests via the retained
`sent` node — it is a separate branch, keyed off `linked_sock_ctx == NULL`, and is
left unchanged.
