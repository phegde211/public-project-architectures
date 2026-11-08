# AI Tradebot Pro — Architecture

**Author:** [Prabhat Hegde](https://github.com/phegde211)

**Private source repository:** [phegde211/ai-tradebot-pro](https://github.com/phegde211/ai-tradebot-pro)

The source repository is private. Please contact the author, Prabhat Hegde,
to request permission and access to the implementation.

AI Tradebot Pro is an **event-driven intraday options-trading platform** for the
Indian F&O (futures & options) market. A live broker feed drives a layered
pipeline: market-data ingestion, a tiered candidate-detection stage, independent
strategy engines with an ML admission gate, risk/position management, broker
execution, and a live operator dashboard.

This document describes the **system architecture and technology stack** at the
level of the companion [Octopus.md](Octopus.md). It is derived from the reviewed
`redesign-4` branch (last committed 15 Aug 2026; later than `main` at 15 Jul 2026).

> **Scope.** This is an engineering overview. The **trading logic is deliberately
> withheld** — detection criteria, strategy rules, ranking and scoring, ML feature
> definitions, calibration thresholds and parameters are proprietary and are **not**
> described here. Everything below is software architecture, infrastructure and
> technology: what the system is built from and how it is structured, never what it
> decides or why.

The design rests on two principles that recur at every layer:

1. **One owner per trading fact.** Market data, candidates, decisions, positions and
   orders each have exactly one component that may *create* them; everything else
   observes. This keeps a concurrent, restartable, broker-connected system auditable.
2. **Processes and the network exist only at failure-domain boundaries.** Work that
   must survive each other's crashes runs in separate OS processes over a message
   bus; tightly-coupled work runs in one interpreter on an in-process bus. The system
   is distributed exactly where distribution earns its cost, and no further (§9).

---

## 1. Technology stack

| Layer | Technology | Role |
|---|---|---|
| Language / runtime | **Python 3.12**, OS processes, worker threads | Separate failure domains; keep tightly-coupled stages in one interpreter. |
| Inter-process bus | **ZeroMQ** (`pyzmq`) + **MessagePack** (`msgpack`) | Topic-based pub/sub between processes with compact binary event envelopes. |
| In-process bus | Custom publish/subscribe `FeedBus` | Connect stages inside the trading process without a network hop per stage. |
| Ordered concurrency | Keyed dispatcher over a worker-thread pool | Per-symbol ordering with cross-symbol parallelism (§4). |
| Broker / market data | **Dhan** SDK (`dhanhq`), **WebSocket** feed (`websockets`, version-pinned), REST; **`pyotp`** (TOTP auth) | Live ticks, candles, option chains, instrument data, order placement, reconciliation. |
| Numerics | **NumPy**, **pandas** | Time-series handling, feature preparation, offline profile generation. |
| Options / indicators | **`py_vollib`** (Greeks/IV), **TA-Lib** (C-backed), **`ta`** | Derivatives math and technical indicators behind the (private) analytics. |
| Machine learning | **XGBoost**, **scikit-learn** | Gradient-boosted admission model loaded from local artifacts; trained offline. |
| Columnar data | **Apache Parquet** via **PyArrow** | Compact per-stock historical series and deterministic profile artifacts. |
| Durable state | **SQLite** snapshots + append-only **JSONL** ledgers | Recovery state independent of transient transport messages (§7). |
| Web backend | **FastAPI** + **Uvicorn** (+ Jinja2) | Dashboard API and streaming endpoints. |
| Frontend | **React 19**, **TypeScript**, **Vite**, **Material UI** (+ MUI X DataGrid), **Lightweight Charts**, Emotion | Typed operator UI, operational tables and streaming charts. |
| Frontend transport | **WebSockets** | Push changing dashboard state without full-page refreshes. |
| Verification | **pytest**, replay-regression harness, **ESLint** + Vite build checks | Data contracts, ordering, recovery, lifecycle and presentation boundaries. |

---

## 2. Process topology & failure domains

The launcher creates **four OS processes**; each is an independently restartable
failure domain.

```
                         system control / status
                    +----------------------------+
                    |      CENTRAL_MANAGER        |   control plane only:
                    |  readiness · alarms · start |   never on the data path.
                    +----------------------------+
                           |                |
                  lifecycle|                |lifecycle
                           v                v
   +-------------+  raw market data  +----------------+  display state  +-----------+
   |    FEED     | ----------------->|     TRADER     | -------------->  |   DASH    |
   | broker I/O  |<----------------- | all decisions  |                 | React UI  |
   | (one owner) |  activation and   | (one           |                 | read-only |
   +-------------+  held-contract     | interpreter)  |                 +-----------+
                    state             +----------------+                      |
                                             ^                                |
                                             +--------- user commands --------+
```

| Process | Owns | Boundary |
|---|---|---|
| **CENTRAL_MANAGER** | Startup, readiness, health, restart coordination, alarms. | Never relays market data or makes a trading decision. |
| **FEED** | Broker auth and all broker I/O: subscriptions, ticks, candles, option chains, history hydration, held-contract marks. | Owns no strategy, risk or position logic. |
| **TRADER** | Every decision, in one interpreter: detection, strategies, ML, risk/position management, execution. | The single authoritative trading-state owner. |
| **DASHBOARD** | Web presentation, streaming state, explicit operator commands. | Observes state; commands return to TRADER for handling. Can never create a trading fact. |

Only **FEED** and **TRADER** sit on the market-to-order path. CENTRAL_MANAGER never
forwards market data; DASH can only display state and submit commands TRADER chooses
to honor.

---

## 3. Message bus & event schema

Exactly **four ZeroMQ streams** cross process boundaries — the only use of the
network. Everything is publish/subscribe, so a slow or dead subscriber never blocks a
producer.

| Port | Stream | Owner → subscribers | Carries |
|---:|---|---|---|
| 5720 | `feed_raw` | FEED → TRADER, DASH | spot ticks, candles, option chains, feed status |
| 5723 | `trader_events` | TRADER → DASH, FEED | activation, held contracts, decisions, display state |
| 5726 | `system_events` | CENTRAL_MANAGER → all | readiness, alarms, start control |
| 5727 | `user_commands` | DASH → TRADER | explicit operator position commands |

- **Envelopes** are MessagePack-encoded; market events are keyed by symbol
  (`tick.<SYM>`, candle and chain topics) so subscribers filter at the topic layer.
- **Historical/data schema** is columnar Parquet with a fixed
  `timestamp, open, high, low, close, volume` contract per stock, one consolidated
  file per symbol.
- Two rules keep this surface small and safe: **the bus exists only between failure
  domains** (inside TRADER, stages use the in-process `FeedBus`), and **transport is
  not a database or a recovery log** — nothing is replayed off the wire to rebuild
  state; recovery comes from authoritative stores (§7).

---

## 4. Concurrency & ordering

TRADER is a single interpreter, so its concurrency model is the load-bearing part of
the design. The receive thread does **no** computation — it drains the socket and
hands each event to a **keyed dispatcher**:

```
   feed socket --> receive thread --> keyed dispatcher
                                         |-- key=SYM_A --> [ ordered ] --\
                                         |-- key=SYM_B --> [ ordered ]    >  run concurrently
                                         |-- key=SYM_C --> [ ordered ] --/
                                       (receive thread never blocks on downstream work)
```

- **Per-symbol serialization** — all events for one symbol run in arrival order.
- **Cross-symbol parallelism** — different symbols run concurrently on the pool.
- **No static sharding** — parallelism is keyed and dynamic; a busy key is resubmitted
  in turns so it cannot monopolize a worker; there is no symbol-to-process partition to
  get wrong or rebalance.
- **The socket keeps draining** while downstream work runs, so one slow symbol never
  backs up the feed.

Stages hand work to one another through a **bounded, sequenced fan-out queue** guarded
by a condition variable:

```
          producer --put()--> [ bounded sequenced queue ] --+--> consumer 1 (own cursor)
                               (deep-copies payloads,        +--> consumer 2 (own cursor)
                                monotonic sequence numbers)   `--> non-consuming observer
```

Its engineering properties are deliberate: **independent cursors** (consumers are not
competing — each sees every item), a **condition-variable doorbell** (consumers wait on
work, never poll), **deep-copied payloads** (no consumer can mutate another's state),
explicit **lag and overflow accounting**, and a **non-consuming projection** so the
dashboard can observe the queue without perturbing delivery.

---

## 5. Market-data ownership (FEED)

FEED is the single owner of the broker relationship. It resolves and subscribes to the
F&O spot universe, publishes lightweight spot packets and completed 1- and 5-minute
candles, maintains **dynamic** option subscriptions, hydrates bounded option history on
demand, retains subscriptions and marks for held contracts, restores completed intraday
history after a mid-session restart, and reports feed freshness and recovery state. It
computes no Greeks, no strategy and no positions — pure I/O ownership and normalization.

One representative engineering detail: **the broker's tick volume is cumulative for the
session.** FEED publishes it unchanged; the forming-bar tracker derives each bucket's
volume from the *difference* between the current cumulative value and that bucket's
baseline, and handles session reset, the auction-inclusive opening bucket, safe
mid-session baselines after a restart, and duplicate / out-of-order packets explicitly.
This is the kind of correctness concern the data layer owns so that nothing downstream
has to.

---

## 6. Decision layers & the ML boundary (proprietary — structure only)

The trading intelligence is layered, and the **layering is architecture; the contents
are proprietary and omitted.** What can be said is purely structural:

- **Tiered processing for scale.** A cheap stage runs across the entire universe on
  every tick; expensive work (option-chain activation, enrichment, analytics) is
  performed **only** for the small set of candidates the cheap stage promotes. This
  bounds work amplification as the universe grows — a scaling decision, not a signal.
- **Independent strategy engines.** Two strategy engines consume candidates as
  independent, non-competing consumers of the fan-out queue (§4); each decides on its
  own with no shared mutable state on the evaluation path. *What* they evaluate is
  withheld.
- **One admitter.** Strategy engines only *propose*. A single position service owns
  final admission, sizing and every capital/exposure/kill-switch limit — a clean
  separation between "find something" and "commit capital."
- **ML as an admission gate, engineered offline.** An **XGBoost** model scores
  candidates that already passed deterministic checks; it can decline but never
  originates a trade. Its engineering boundary is the notable part: **training and
  profile generation never run on the live path**; the live process only performs
  deterministic lookups of pre-built local artifacts through one cached API; the
  feature pipeline is **causal** (only decision-time information, by construction — no
  lookahead); thresholds are **calibrated per stock**; and a single production loader
  means there is never ambiguity about which model decided. Feature definitions,
  counts and thresholds are withheld.

---

## 7. State, recovery & execution integrity

Recovery rests on one principle (§3): **transport is never the source of truth.** After
any restart — including mid-session — state is rebuilt from authoritative stores:

| Authoritative source | Restores |
|---|---|
| Append-only JSONL trade ledger | open/closed positions and the session count |
| SQLite snapshots / event logs | each engine's internal state |
| Broker resubscription / snapshots | current live market state |
| Spot & option-history hydration | completed intraday context |
| **Broker reconciliation** | orders the broker acknowledged but the local side may not have recorded |
| Last-known-good local profile artifacts | per-stock reference data |

Snapshot writes go to a **background writer** so decision callbacks never block on a
SQLite commit; the queues are bounded and expose overflow, because an async enqueue is
not a durable commit. Execution is the narrowest, most defensive boundary: **idempotent
order intents** (a retry or replay cannot place a second real order — the broker is the
authority), **continuous broker reconciliation**, **mandatory protective exits** (every
position carries a non-optional stop/trailing, updated in the same transition as marks
and the ledger), and **one adapter contract shared by paper and live**, so replay and
paper exercise the same path as live. Because recovery reads authoritative state rather
than replaying messages, a dropped or duplicated message can never corrupt the
post-restart picture.

---

## 8. Deterministic replay & verification

**Runtime and replay construct the same orchestrator graph.** A recorded session is fed
through the identical pipeline live consumers use, on an **event-time** clock with
explicit processing barriers, so the same inputs reproduce the same outputs. This makes
behaviour investigable and is the basis of a full-pipeline regression harness. A replay
result is an engineering artifact — never, on its own, a claim about profitability.

The **pytest** suite is the acceptance gate, covering transport/event contracts,
per-symbol ordering, fan-out/deep-copy/overflow behaviour, forming-vs-completed-bar
separation, position lifecycle and risk limits, recovery/restart, and live-vs-replay
graph construction; **ESLint** and the Vite build gate the frontend.

**Reviewed scale** (`redesign-4` snapshot, identifying artifacts — not throughput or
profitability claims):

| Dimension | Reviewed value |
|---|---|
| Stock universe / profile coverage | **210** F&O stocks — 210 requested, **210 built, 0 failed, 0 missing** |
| Historical series | **210** per-stock Parquet files (one-year, five-minute OHLCV) |
| ML model input | **111** features in the production model-input artifact |
| Runtime failure domains | **4** OS processes (supervisor + three children) |

---

## 9. Design evolution — why this shape

The four-process design is the product of a deliberate consolidation, and the reasoning
is part of the architecture. An earlier version was **sharded across many processes** —
separate analytics/strategy shards, separate portfolio and execution processes,
inter-stage durable spools and stage-specific ports. That bought little and cost much:
every stage boundary became a serialization point, a recovery problem, an ordering
hazard and another thing to monitor, all to parallelize work that was never the
bottleneck.

It was retired in favour of the rule used throughout: **processes and the network exist
only at failure-domain boundaries.** The three things that must survive each other's
failures — broker I/O (FEED), decisions (TRADER) and presentation (DASH), under a
supervisor — are the only process splits. Everything inside the decision domain was
pulled back into one interpreter, where a **keyed dispatcher** (§4) recovers all the
useful concurrency without any cross-process handoff, durable-spool or static-sharding
cost. The payoffs are visible throughout: recovery reads authoritative state instead of
draining spools (§7); ordering is a local guarantee, not a distributed protocol (§4);
stage-to-stage handoff is an in-process condition variable, not a networked queue (§4);
and the whole pipeline reconstructs deterministically for replay because it is one graph
in one place (§8).

---

*This is an architecture and technology overview. The trading strategy, detection and
ranking logic, ML features and parameters, datasets and operational configuration are
proprietary and remain in the private repository. For access or an interview
walkthrough, contact Prabhat Hegde.*
