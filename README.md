# Project Architectures — Prabhat Hegde

Public **architecture documents** for two independently built systems. The source code for
both is **private** — these documents exist so the design can be reviewed without exposing
the implementation. To request access, **to purchase or associate**, contact Prabhat Hegde
([prabhat.hegde@gmail.com](mailto:prabhat.hegde@gmail.com)).

| Project | What it is | Full architecture |
|---|---|---|
| **Octopus** | Transparent, asynchronous TLS-terminating interception proxy — C | [Octopus_architecture.md](Octopus_architecture.md) |
| **AI Tradebot Pro** | Event-driven intraday options-trading platform — Python | [AI_tradebot_architecture.md](AI_tradebot_architecture.md) |

---

## 1. Octopus — transparent, asynchronous TLS interception proxy

Octopus (`octo`) is a **multi-threaded, TLS-terminating interception proxy written in C**. It
accepts **mutual-TLS (mTLS)** connections from clients and opens its own mTLS connection to
the upstream server, shuttling traffic between the two. Because it **terminates TLS on both
sides**, it sits on the **plaintext path** and can inspect or transform traffic before
relaying it onward.

It is **purely asynchronous**: a single `epoll` instance (edge-triggered, one-shot) drives
everything — including non-blocking TCP `connect()` and *both* TLS handshakes — so a slow
handshake never stalls the accept loop. A **1-master + N-worker** thread model pins every
socket (and its OpenSSL object) to one owner worker — **connection affinity** — so the same
`SSL` is never touched by two threads, and a client-facing socket and its upstream are bridged
onto the **same** worker. Per-socket input/output queues handle partial reads/writes and
framed-message reassembly; connection lifetime is **reference-counted**; and TLS certificates
**hot-reload** via `inotify`. The same binary can also run as a replay/echo server to drive
end-to-end tests.

```
                        +----------------------------------+
                        |               octo               |
  +--------+   mTLS     |  +-----------+    +-----------+   |   mTLS    +----------+
  | client |<=========> |  |  client-  |    | upstream  |   |<=========>| upstream |
  +--------+            |  |  facing   |<==>| (backend) |   |           |  server  |
                        |  |  socket   |    |  socket   |   |           +----------+
                        |  +-----------+    +-----------+   |
                        |         \   inQ / outQ   /        |
                        |        +--------------------+     |
                        |        |   GST hash table   |     |
                        |        +--------------------+     |
                        +----------------------------------+

   TLS terminated on BOTH sides, so octo works on the plaintext.
   One worker thread owns both sockets of a bridged connection.
```

Full design: **[Octopus_architecture.md](Octopus_architecture.md)**.

---

## 2. AI Tradebot Pro — event-driven options-trading platform

AI Tradebot Pro is an **event-driven intraday options-trading platform** for the Indian F&O
(futures & options) market, in Python. A live **Dhan** broker feed drives a four-process
pipeline: market-data ingestion, a **two-ring scout** that watches ~212 underlyings on cheap
spot price and activates option chains only for the few it promotes, two pluggable strategies
— **ACOS** and **PAF** — with an **XGBoost** admission gate, risk/position management,
idempotent broker execution, deterministic replay, and a live React dashboard. It has been run
live against Dhan with real capital. Its defining property is a **modular, strategy-agnostic
spine** into which strategies plug as interchangeable components; the strategy decision rules
are the proprietary core and are withheld.

Full design: **[AI_tradebot_architecture.md](AI_tradebot_architecture.md)**.

---

*Source code for both projects is private. To purchase or associate, contact Prabhat Hegde
([prabhat.hegde@gmail.com](mailto:prabhat.hegde@gmail.com)).*
