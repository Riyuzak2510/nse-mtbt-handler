# NSE MTBT Handler — Synthetic Feed, Durable Order Books & Matching

A Go project for learning how a binary market-data feed becomes a local order book, how a matching engine can recover from PostgreSQL, and how durability, queues and service boundaries affect latency.

**Stack:** Go · NATS JetStream · PostgreSQL · Docker · Kubernetes/kind

> **Scope and honesty:** This is an independent, synthetic NSE cash-equities/CM-inspired engineering lab. It is not affiliated with NSE, a certified live NSE feed decoder, an exchange-equivalent matching engine, a trading gateway or a production-ready HFT platform. All published workload data is synthetic. No live exchange credentials or market-data captures are included. More workers and Kubernetes did **not** consistently make this project faster; the regressions and failed capacity attempts are part of the findings.

## Contents

- [What we are building](#what-we-are-building)
- [Current architecture](#current-architecture)
- [Binary feed profile](#binary-feed-profile)
- [Matching and PostgreSQL model](#matching-and-postgresql-model)
- [Design evolution and measured consequences](#design-evolution-and-measured-consequences)
- [Latency results](#latency-results)
- [Run locally](#run-locally)
- [Kubernetes deployment](#kubernetes-deployment)
- [Tests and benchmarks](#tests-and-benchmarks)
- [Repository map](#repository-map)
- [Limitations and future work](#limitations-and-future-work)

## What we are building

The original goal was simple:

1. Receive native-style binary ticks from a local exchange-feed simulator.
2. Decode and validate the packets.
3. Preserve the feed in a durable message broker.
4. Apply events to a local order-book database.
5. Add synthetic order matching, restart recovery and reliable notifications.
6. Measure the full path rather than assuming that a fast decoder means a fast system.

Two different business modes remain available:

| Mode | Meaning |
| --- | --- |
| **Reconstruction** (`book-service`) | Apply synthetic exchange NEW/MODIFY/CANCEL/TRADE events to reconstruct the exchange-observed book. It does not invent exchange executions. |
| **Synthetic matching** (`matching-service` or split services) | Treat the supported order messages as local matching inputs and compute synthetic fills. Native trade messages are observed rather than applied as a second execution. |

These modes must **not** compete as writers on the same history. The current target topology separates order admission from dedicated matching instances per configured ticker.

## Current architecture

```mermaid
flowchart LR
    source["Synthetic binary source"] -->|UDP| publisher["Go receiver / async publisher"]
    publisher --> raw["Raw JetStream log"]
    raw --> intake["Order service: ordered admission"]
    intake -->|"Atomic PENDING order + work + progress"| db[("PostgreSQL")]
    db -->|"Commit hint / durable pending work"| dispatch["Per-ticker dispatcher goroutines"]
    dispatch --> routed["Ticker-addressed JetStream subjects"]
    routed --> m1["Ticker 1 matching pod: bids + asks"]
    routed --> m2["Ticker 2 matching pod: bids + asks"]
    m1 -->|"Atomic orders + applied progress + outcomes"| db
    m2 -->|"Atomic orders + applied progress + outcomes"| db
    db --> relay["Reliable outcome relay"]
    relay --> results["Outcome JetStream log"]
```

### Concurrency boundaries

| Component | Current behavior |
| --- | --- |
| UDP ingestion | Independent socket reader and bounded asynchronous publisher pipeline |
| Raw order admission | **One ordered worker across tickers**; validates native continuity and commits admission |
| Routing dispatch | Independent goroutines/SQL sessions per ticker; one in-flight publish per ticker, shared concurrency-safe NATS client |
| Matching across tickers | **Concurrent** dedicated processes/pods, consumers and database ownership leases |
| Matching within a ticker | **Sequential**, both bid and ask sides together |
| Outcome relay | Independent, ordered single-in-flight reliable publication |

This is **keyed delivery**, not random load balancing. Running several replicas against one consumer would not by itself preserve per-ticker state ownership or source ordering.

The configured split protocol supports **1–16 tickers**; the sample Kubernetes overlay explicitly creates **two** matching deployments. There is no automatic deployment/controller for arbitrary new symbols, and no HPA-based concurrent owner of one ticker.

### Commit and acknowledgement are different milestones

1. The order service inserts a NEW order as `PENDING`, stores immutable routing work and advances admission progress in **one transaction**.
2. Only after that transaction commits does it acknowledge the raw broker message.
3. A dispatcher publishes committed work to `mtbtmatch.<stream>.<token>`, validates JetStream PubAck and records publication progress.
4. That ticker's matcher verifies command identity/sequence/digest and performs matching.
5. It commits all changed orders, pending outcomes and applied progress atomically, then acknowledges the matching command.
6. The outcome relay publishes each committed outcome, validates PubAck and deletes its pending row.

**Raw ACK means admitted—not matched. PubAck means accepted by the configured broker—not filled. Matching commit and outcome publication are separate events.** Delivery is at least once with identity/version-aware handling; there is no blanket end-to-end exactly-once claim.

A command witness is retained until **both publication and matching application** are recorded. Notifications use a pending-only transactional outbox. Neither journal is a second authoritative business-order table.

## Binary feed profile

The local profile is `cm-mtbt-le64-v1`: packed, explicit-width, little-endian decoding rather than unsafe casts to host structs.

| Packet | Local length | Main fields |
| --- | ---: | --- |
| Header | 8 bytes | Message length, stream ID, native sequence |
| Order (`N`, `M`, `X`) | 38 bytes including header | Type, timestamp, order ID, token, side, price, quantity |
| Trade (`T`) | 45 bytes including header | Type, timestamp, buy/sell IDs, token, price, quantity |
| Heartbeat (`Z`) | 13 bytes including header | Zero header sequence and last data sequence |

- Price is an integer number of paise; quantities are integers.
- Order IDs retain their eight IEEE-754 DOUBLE wire bits rather than being blindly converted to integer IDs.
- Timestamp width is explicitly **eight bytes**. An older official CM/CD v6.3 specification defines LONG as eight bytes, while the inspected v6.5/v6.6 scalar tables contradict their nanosecond timestamp descriptions. No current capture/erratum was available to certify present live-wire compatibility.
- Native source sequence—not local timestamps—governs continuity. Heartbeats/control events cannot simply be filtered out without changing correctness.
- A version-2 JSON manifest binds native stream, profile, seed and catalog using a compatibility digest. Its creation time is provenance, not a new execution identity.
- The simulator is not a complete exchange emulator: multicast connectivity, exchange TCP recovery, broader session types and complete message coverage remain future work.

Public reference material: [NSE real-time products](https://www.nseindia.com/static/market-data/real-time-data-subscription), [official CM/CD v6.3 PDF](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Realtime_CM_CD_TBT%20ver%206.3.pdf), [inspected v6.6 PDF](https://nsearchives.nseindia.com/web/mediaattachment/2026-09/Realtime_CM_CD_TBT_ver_6.6.pdf). Availability and revisions may change; synthetic round trips are not independent exchange conformance evidence.

## Matching and PostgreSQL model

### In-memory matching

Each matching owner has its own in-memory bid/ask book:

- Best price first; live queues use arrival-order FIFO within a price level.
- Partial and full fills update remaining/filled quantities on both counterparties.
- MODIFY uses the adopted cancel/re-enter policy; it is not a complete exchange-specific modification policy.
- CANCEL removes the remaining live quantity.
- Observed native trade events do not double-execute synthetic matches.
- PENDING orders are not matchable liquidity.

**Recovery trade-off:** no persisted `priority_sequence` or timestamp ranking field was added. Database restoration uses deterministic bytewise order-ID ordering for ties, so historical same-price FIFO is **not** guaranteed across restart. This simplification is deliberate and unsuitable if exact exchange fairness is required.

### One authoritative business orders table

`mtbt.orders` stores both active and terminal matching state. The primary key is `(stream_id, order_id)`; the order identity is lossless `bytea`.

Important fields include:

```text
stream_id, order_id, token, side, price
accepted_quantity, filled_quantity, cancelled_quantity, remaining_quantity
status: PENDING | OPEN | PARTIALLY_FILLED | FILLED | CANCELLED
```

For a normal order lifecycle:

```text
accepted_quantity = filled_quantity + cancelled_quantity + remaining_quantity
```

A partially filled order remains one row with the residual quantity. Terminal orders do not require another completed-orders business table. Matching updates all affected counterparties in a single transaction. A partial active-depth index excludes PENDING and terminal rows.

Supporting tables serve different purposes:

| Table family | Purpose |
| --- | --- |
| `feed_state` | Native-stream manifest ownership, validity and admission/reconstruction progress |
| `ticker_topology`, `ticker_progress` | Split configuration and admitted/published local positions |
| `ticker_applied` | Independent matching position per ticker; avoids making every matcher lock global progress |
| `match_work` | Immutable, bounded pending routing/witness records |
| `order_outbox` | Bounded pending outcome delivery records, removed after confirmed publication |
| `active_orders` / subscriber metadata | Reconstruction-only compatibility mode; not another matching business book |

The source's invalidity transition is fenced against concurrent matching transactions. Dedicated database sessions/advisory leases prevent competing owners. These compatibility guards are **not** a security boundary against privileged SQL access.

### Crash recovery

PostgreSQL is authoritative; RAM is a cache for matching execution.

- Crash before commit: no durable matching effects; recovery can reprocess the command.
- Commit before broker ACK: redelivery must recognize the committed identity/progress without applying fills twice.
- Failed/ambiguous commit: stop the instance and rebuild from committed state before reusing tentative RAM.
- Restart: load only committed OPEN/PARTIALLY_FILLED orders for that ticker; exclude PENDING and terminal rows.
- Book reads suppress stale/invalid or behind-admission depth rather than presenting incomplete state as current.

Checkpoints therefore record **which inputs produced the committed state**; they are not redundant copies of orders.

### Why these tools?

- **Go:** straightforward concurrency, networking, testing and deployment. C++ can offer tighter allocation/layout/runtime control for microsecond-sensitive engines, but this lab's observed queue/database delays do not establish a Go-versus-C++ bottleneck.
- **PostgreSQL:** atomic multi-order/progress/outbox updates, constraints, indexed reads and recoverable committed state. Transactions alone do not make one-transaction-per-input fast enough for exchange peak rates.
- **JetStream:** retained messages, explicit acknowledgements, durable consumers and keyed subjects. More replicas improve availability; they do not automatically remove per-stream leader/serialization costs or a publisher waiting synchronously on every PubAck.
- **Kubernetes:** process isolation, pod replacement, resource limits and repeatable topology. It is the ongoing deployment target—not proof of lower latency or production HA.
- **Indexes before speculative partitioning:** indexes follow current access patterns. Partitioning and long-term terminal-order retention were not claimed as implemented performance fixes from tiny active-book tests.

## Design evolution and measured consequences

| Phase | Decision | What we learned |
| --- | --- | --- |
| Initial footprint | Go + local native-style CM packets → JetStream → PostgreSQL reconstruction | Full-pipeline latency includes network, queueing, transactions and ACKs—not just codec time. |
| Parallel reconstruction subscribers | Ordered source routing into instrument shards | Correctness and concurrency passed, but extra front-stage SQL/routing work made the tested path slower. |
| Kubernetes lab | Explicit-context kind, persistent single-instance PostgreSQL/NATS, resource limits | One matched-workload reconstruction observation improved, but multiple environment changes prevent Kubernetes-only attribution. |
| Synthetic matching | Add RAM matching with PostgreSQL-authoritative order status, atomic checkpoints and restore | Partial fills survive restart; persistent ordering/checkpoints remain necessary even with one business table. |
| Simplified identities | Remove synthetic `run_id` and explicit priority sequence | Fewer business identifiers; fresh history must still be deliberate. Same-price historical FIFO restore was traded away, not magically preserved. |
| High-rate ingest diagnosis | Separate socket reception from synchronous durable publication | Confirmed UDP receive-buffer overflow; application/DB throughput was a separate bottleneck. |
| Bounded async publisher | Independent UDP reader/FIFO, multiple tracked publish acknowledgements | A finite publisher-only control admitted every packet with zero socket loss. Not a full high-rate matching capacity result. |
| Reliable outcomes | Atomic changed-order snapshots in a pending-only outbox | Prevents lost notifications after a committed fill; publication is separate from matching. |
| Push-triggered relay | PostgreSQL commit hints + LISTEN-before-scan, retaining recovery scans | Removed the intentional 100ms idle-poll delay in the tested normal path without weakening durable pending delivery. |
| Dedicated matching per ticker | Separate PENDING admission, durable ticker routing and exclusive per-ticker matching pods | Real overlapping matching work, but worse end-to-end latency in the primary equal-budget pair. Serial admission/shared dependencies remained. |
| Focused handoff analysis | Correlate the same native inputs across admission, broker and matcher traces | At ~501/s the handoff averaged0.769ms;96% of mean time accumulated before admission completed. |

## Latency results

**Do not read these as one monotonically improving series.** Modes, workload mixes, active cardinality, warmup, clocks and infrastructure changed. Only within the explicitly described comparison are before/after numbers directly paired. Samples are finite local-lab observations, not exchange SLAs or maximum sustainable throughput.

Definitions:

- **Commit latency:** source UDP-send start → client observation of a successful PostgreSQL-processing return after commit. Not exact server fsync time or broker ACK latency.
- **Outcome latency:** source send → latest JetStream timestamp among all expected outcome messages for the input. Not subscriber delivery, client processing or notification ACK.
- **Processing span:** client method duration including checks, SQL/network waits and commit response. Not pure matching CPU time.
- Percentiles are nearest-rank for retained exact traces. Independent marginal percentiles must not be summed or subtracted to attribute tail latency.

### 1. Serial versus three reconstruction subscribers

Same mixed N/M/X/T workload, three instruments,10,000 measured events after500 warmup, same source/build/durability within the comparison.

| Actual offered rate | Topology | Mean send→commit | p50 | p99 |
| --- | --- | ---: | ---: | ---: |
|~100/s |Serial |5.016ms |3.780ms |33.284ms |
|~100/s |Three routed subscribers |179.635ms |8.495ms |1,993.783ms |
|~1,000/s |Serial |15.546s |15.650s |30.879s |
|~1,000/s |Three routed subscribers |30.933s |30.789s |60.823s |

The routed path added durable dispatcher work and another publication. A transient upstream backlog dominated its low-rate tail; it was retained, not trimmed away. Both high-rate finite runs queued heavily; neither established sustainable low-latency1,000/s capacity. “More consumers” was not a free optimization.

### 2. Host-local versus Kubernetes reconstruction

Same three-instrument reconstruction workload,10k measured +500 warmup at100/s:

| Setup | Mean | p50 | p99 |
| --- | ---: | ---: | ---: |
|Host Go clients + Docker-published PostgreSQL/NATS ports |5.016ms |3.780ms |33.284ms |
|Linux pods in local kind |1.507ms |1.377ms |4.228ms |

Observed p99 was87.3% lower. Host networking/storage history/cache/resource placement also changed. This is a whole-setup observation, **not proof Kubernetes caused an87.3% speedup**. Later matching workloads are different and cannot be substituted into this comparison.

### 3. Synchronous versus bounded asynchronous UDP ingestion

Publisher-only finite profile:161,000 data packets plus one heartbeat, requested stages100→1,000→5,000→10,000/s. Same-image comparison, not full-path matching.

| Counter | Synchronous | Asynchronous |
| --- | ---: | ---: |
|Broker admissions including heartbeat |115,480 |161,001 |
|Socket drops |45,521 |0 |
|Native holes |45,521 |0 |

Async admitted every expected packet and drained its bounded queue/in-flight acknowledgements. Sync's surviving per-publish call duration was small, yet waiting per PubAck let the UDP socket overflow. A sync post-source environment interruption remains a caveat; this is not a clean universal latency-speedup claim or a20M full-system pass.

### 4. Polling versus push-triggered reliable outcomes

Same-image normal-load control:10,000 inputs per window, achieved99.91/s polling versus99.80/s notify.

| Relay mode | Matching commit mean | Matching commit p99 | All outcomes stored mean | Outcome p99 |
| --- | ---: | ---: | ---: | ---: |
|100ms idle polling |2.258ms |4.252ms |53.728ms |103.303ms |
|Commit-triggered notification |2.326ms |5.291ms |3.240ms |7.491ms |

Outcome mean fell93.97% and p99 fell92.75%. Matching itself did not become faster. PostgreSQL NOTIFY is only a wake-up hint; durable outbox rows, startup scan and periodic reconciliation remain. High-rate mode arms achieved different rates, so they are not treated as a controlled capacity comparison. A timing-invalid earlier attempt was rejected rather than clamped into the accepted result.

### 5. Combined matching versus separate admission + ticker matching

Primary pair: same image,20,000 inputs per arm in two10k stages, two equally interleaved tickers, identical durability/push outcomes. Aggregate admission+matching ceilings **3CPU/1,280MiB**, requests300m/640MiB, identical placement policy. No warmup exclusion. Per-ticker source triplets: buy100, sell30, sell70.

| Actual input rate, combined → split | Combined matching mean | Split matching mean | Combined p99 | Split p99 |
| --- | ---: | ---: | ---: | ---: |
|85.36 →86.29/s |2.897ms |5.138ms |7.636ms |11.380ms (**+49.0%**) |
|498.32 →500.84/s |41.493ms |82.982ms |170.495ms |235.715ms (**+38.3%**) |

| Actual input rate, combined → split | Combined outcome mean | Split outcome mean | Combined outcome p99 | Split outcome p99 |
| --- | ---: | ---: | ---: | ---: |
|85.36 →86.29/s |4.016ms |6.047ms |10.995ms |14.569ms |
|498.32 →500.84/s |42.307ms |84.961ms |171.258ms |237.523ms |

Requested100/1,000/s were **not achieved** by this source. These are observations near86/500/s, not proof the system handled1,000/s.

Both arms passed exact20,000 orders,20,002 raw packets and33,332 outcomes, no application restarts and complete drain. Split additionally passed20,004 routed commands and local digest chains. Matching calls genuinely overlapped across pods: max concurrent calls2; at high rate their intervals overlapped for32.1% of matching-busy time. Intervals include waits, so this is not a CPU-parallelism guarantee.

**Conclusion:** per-ticker isolation works, but the extra admission/routing/transaction work did not improve latency in the primary pair. Earlier overlapping calibration had noisy tails and sometimes the opposite result direction. Sequential primary traffic, shared VM workloads, background readback during part of low-rate baseline and a NotReady control-plane node limit causal/statistical conclusions. No outcome-dependent trial selection or universal superiority claim.

### 6. Is the service-to-service handoff the problem?

Offline same-input attribution from the verified split high stage, actual500.84/s:

| Boundary | Mean | p99 |
| --- | ---: | ---: |
|Send → admission commit observed |79.661ms |232.620ms |
|Admission commit observed → routed broker timestamp |0.614ms |1.832ms |
|Routed timestamp → matcher delivery |0.155ms |1.055ms |
|**Full handoff to matcher delivery** |**0.769ms** |**2.227ms** |
|Matching ProcessCommand |2.522ms |4.901ms |
|Admission commit observed → matching commit observed |3.321ms |6.478ms |
|**Entire send → matching commit observed** |**82.982ms** |**235.715ms** |

Approximately96% of the mean elapsed time preceded admission completion. Handoff alone accounted for0.93%. The same slowest1% inputs also spent most time upstream; that conclusion is not a subtraction of unrelated p99s.

The handoff is **not pure network latency**: it includes dispatcher SQL, wake-up/scheduling, broker submission and consumer availability. Retained broker timestamps are not directly instrumented fsync/PubAck instants. Client commit observations can follow dispatcher activity; signed negative residuals are retained, not treated as negative network latency. The next useful target is admission/SQL/source profiling, not assuming XDP or another matcher pod fixes the queue.

### Published result summaries

[`results/latency-history.json`](results/latency-history.json) and [`results/combined-vs-tickers.json`](results/combined-vs-tickers.json) retain selected distributions, boundaries and caveats. Original result hashes are included for provenance. Large raw CSVs/logs, real Kubernetes state, kubeconfigs and the working notebook/document archive are intentionally **not** in this public source repository; hashes do not make absent raw traces independently auditable. Analysis tools are included for newly generated evidence. Archive-dependent regression tests explicitly skip when historical fixtures are absent.

## Run locally

### Requirements

- Go1.24+; container build pins Go1.25.4.
- Docker Engine/Desktop + Compose.
- Python3 for harnesses (standard library only).
- For Kubernetes: `kind`, `kubectl`, healthy nodes and sufficient Docker CPU/RAM/storage.

Development defaults bind to loopback: NATS14222, monitoring18222, PostgreSQL15432. Compose password `local-dev-only` and unauthenticated local NATS are **deliberate development examples, not production secrets**. Never expose them. Remote binds require explicit opt-in; production needs real authentication, TLS, database roles, network policy and secrets management.

### 1. Build and start fresh local dependencies

```sh
make build
make infra-up
docker compose ps
```

**Fresh means fresh broker AND database history.** Existing volumes are never implicitly cleared/migrated. Do not run reconstruction/combined and split writers together on the same stream. A new manifest filename/timestamp does not isolate old data. Do not run `down -v` against valued data to force this example to work.

### 2. Create a two-instrument source manifest

```sh
./bin/simulator -init -manifest run/demo.json -seed 42 -instruments 2 -stream 1
```

This creates tokens900001 and900002. The source file is exclusive; it is not overwritten. Only initialize this example against fresh development history. A generator restart at sequence1 on retained data is not a supported source resume.

### 3. Start the split services in separate terminals

All commands run from repository root with the same manifest. Start admission before matchers so topology/schema initialization completes. Wait for each service's startup log; pre-feed readiness can be REBUILDING.

```sh
# Terminal A — single native admission owner
./bin/order-service -manifest run/demo.json -http 127.0.0.1:19006
```

```sh
# Terminal B — both bid and ask for token900001
./bin/ticker-matcher -manifest run/demo.json -token 900001 -http 127.0.0.1:19007
```

```sh
# Terminal C — independent token900002 owner
./bin/ticker-matcher -manifest run/demo.json -token 900002 -http 127.0.0.1:19008
```

```sh
# Terminal D — committed outcome publication; notify mode is default
./bin/outcome-relay -manifest run/demo.json -http 127.0.0.1:19005
```

```sh
# Terminal E — UDP receiver → raw JetStream; async is default
./bin/publisher -manifest run/demo.json
```

### 4. Generate one finite matching fixture

```sh
# Run exactly once against this fresh native history.
./bin/ticker-workload -manifest run/demo.json -start 1 -end 12 -rate 10
```

This interleaves two instruments with deterministic partial/full-fill triplets, then a heartbeat. It is a correctness fixture, not a realistic exchange order-flow distribution. Do not use the independent reconstruction simulator's reference book as the synthetic matcher's fill oracle.

### 5. Inspect state

```sh
curl -sS http://127.0.0.1:19006/status
curl -sS http://127.0.0.1:19007/status
curl -sS 'http://127.0.0.1:19007/book?depth=5'
# Order ID1 encoded as little-endian DOUBLE bits:
curl -sS 'http://127.0.0.1:19006/orders?id=000000000000f03f'
```

Finite input stopping eventually produces **STALE readiness**; that is different from an application crash. `/healthz`, `/readyz`, `/status`, matcher `/book` and `/orders` expose their respective health/state boundaries. Database/NATS URLs can be overridden with `DATABASE_URL`/`NATS_URL`; do not log secrets or place production DSNs on command lines.

Stop foreground services with Ctrl-C. `make infra-down` stops Compose dependencies without requesting named-volume deletion; retain state if investigating recovery.

## Kubernetes deployment

Kubernetes is the intended ongoing deployment environment; Compose is a convenient development/test path.

| Assets | Purpose |
| --- | --- |
|`deploy/kind.yaml` |Explicit three-node local lab, two labeled workers |
|`deploy/kubernetes` |Reconstruction base and persistent dependencies |
|`deploy/matching` |Combined durable matching alternative |
|`deploy/outcomes` |Independent reliable outcome relay |
|`deploy/tickers` |Separate order service and two dedicated ticker matcher deployments |

Inspect before applying. Overlays expect credentials, manifest configuration, compatible image tags and suitable storage; **`kubectl apply -k` alone is not a complete bootstrap**. PostgreSQL/NATS are single persistent instances, not production HA.

Create the isolated local cluster only if you deliberately want a new lab:

```sh
mkdir -p run/k8s-lab
kind create cluster --name nse-mtbt-lab \
  --config deploy/kind.yaml --kubeconfig "$PWD/run/k8s-lab/kubeconfig"
kubectl --kubeconfig "$PWD/run/k8s-lab/kubeconfig" \
  --context kind-nse-mtbt-lab get nodes
kubectl kustomize deploy/tickers
```

Harnesses use the explicit project kubeconfig, loopback API and exact expected node set; they refuse other clusters, existing history/source jobs and automatic destructive cutovers. They generate local credentials at deployment time and create fresh namespaced histories. Some historical harnesses snapshot earlier lab namespaces and require that lab context; inspect `snapshot()`/`PROTECTED` and `--help` rather than assuming every script bootstraps a new empty cluster. Do not weaken the guard or create/reset old namespaces merely to reproduce a chart.

Current split correctness harness actions: `scripts/k8s_tickers_smoke.py build|up|smoke|readback --out <project-path>`. Build and run outputs must be fresh; smoke deliberately verifies an offline ticker, partial-fill recovery and real pod replacement. It is not a benchmark or an idempotent sequence1 replay.

The fixed overlay is a starting point for two configured ticker IDs, not a dynamic stock provisioning system. All default budgets and stream capacities are lab settings requiring sizing/review.

## Tests and benchmarks

### Unit, race, build and static checks

```sh
make test
make race
make vet
make build
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py'
```

Go service integration tests require explicit local test URLs and can skip when those are absent. Python archive-only checks skip if unpublished historical fixtures are missing. **Skipped service tests are not integration passes.** Use disposable test dependencies, not valued retained books:

```sh
MTBT_TEST_DATABASE_URL='postgres://mtbt:local-dev-only@127.0.0.1:15432/mtbt?sslmode=disable' \
MTBT_TEST_NATS_URL='nats://127.0.0.1:14222' \
go test -p 1 -count=1 -race ./...
```

Real-service verification during development covered atomic rollback, partial fills, duplicate ownership, witness corruption, out-of-order commands, PubAck-before-bookkeeping crashes, commit-before-ACK recovery, queued modify/cancel, invalidation fencing and independent ticker/intake progress. A blocked ticker transaction was shown not to block another ticker's matching/intake. Actual Kubernetes tests verified pod replacement and exact raw/routed/outcome data—not just counters.

### Reproduce bounded comparative benchmarks

The source and analysis tooling is included; raw historical artifacts are not required to generate new measurements.

- `scripts/run_latency.py`: reconstruction serial versus routed shards.
- `scripts/k8s_ticker_benchmark.py`: guarded fresh combined versus dedicated-ticker comparison.
- `scripts/analyze_ticker_benchmark.py`: full correctness-gated offline comparison.
- `scripts/ticker_benchmark_readback`: direct retained-message verification without advancing application consumers.
- `scripts/analyze_matching_handoff.py`: same-input attribution against a supplied evidence hash manifest; its fixture example refers to the unpublished working archive.
- `cmd/matching-capacity`, `scripts/run_matching_capacity.py`: engine capacity/memory experiments; not whole-pod cardinality guarantees.

Example comparison sequence on a deliberately prepared compatible lab:

```sh
# Helpers are host-side tools, not copied binaries in this repository.
go build -o bin/ticker-benchmark-readback ./scripts/ticker_benchmark_readback
python3 -B scripts/k8s_ticker_benchmark.py --help
python3 -B scripts/k8s_ticker_benchmark.py build --out verification/new-image
python3 -B scripts/k8s_ticker_benchmark.py deploy \
  --namespace nse-mtbt-ticker-bench-combined-r3 --out verification/new-combined
python3 -B scripts/k8s_ticker_benchmark.py run \
  --namespace nse-mtbt-ticker-bench-combined-r3 --out verification/new-combined
python3 -B scripts/k8s_ticker_benchmark.py readback \
  --namespace nse-mtbt-ticker-bench-combined-r3 --out verification/new-combined
# Only after baseline traffic/readback stop, repeat deploy/run/readback with
# nse-mtbt-ticker-bench-tickers-r3 and a different fresh output directory.
python3 -B scripts/analyze_ticker_benchmark.py \
  --combined verification/new-combined --tickers verification/new-tickers \
  --out verification/new-comparison.json
```

Do not reuse old image tags/output directories across different binaries without checking build/load and source identity. Tests and harnesses refuse incomplete exact data, missing ACKs, redelivery, process restarts or undrained queues as a clean comparison. Report actual arrival rate, completed rate, backlog, drops and drain separately; do not publish survivor-only latency after data loss as success.

## Repository map

```text
cmd/            Executables: source, receiver, reconstruction, matching, relay
internal/       Codec, books, matching, broker protocols, SQL/storage, metrics
scripts/        Guarded lab/load/recovery tooling and offline analysis/tests
  */            Host-only retained-message verifier commands
  verify_load.sql
deploy/        kind, Kubernetes and Kustomize assets
results/        Selected small, sanitized numeric summaries and source hashes
compose.yaml    Loopback-only development dependencies
Dockerfile      Pinned builder and non-root scratch runtime
Makefile        Build/test/race/vet/local-infrastructure entry points
README.md       The only published Markdown document
```

No credentials, kubeconfigs, compiled binaries, generated manifests with secrets, raw packet captures, large benchmark logs or additional Markdown notebooks are part of the public export. The working development archive remains separate. Runtime Go/SQL source is unchanged by publication; only historical-fixture Python tests have export-specific skip guards. No open-source license is implied by a public repository; licensing is a separate author decision.

## Limitations and future work

| Area | Current boundary / next step |
| --- | --- |
|Live NSE connectivity |Legitimate current reference packets, exchange/vendor access and broader protocol/recovery support needed |
|Source calibration |Requested rate was not always achieved; qualify sender scheduling/clocks before1k/10k capacity conclusions |
|Admission throughput |Single ordered admission loop remains; profile SQL roundtrips/server waits/ACKs before redesigning keyed workers or batching |
|Database tuning |Indexed access paths exist; terminal-data retention/partitioning and server-side bottlenecks need production-scale evidence |
|Routing retention |Default routed stream64MiB/24h; offline beyond retention can strand published/unapplied work. SQL witnesses remain, but safe automatic republish is not implemented |
|Recovery ordering |Deterministic ID tie-break after restart does not preserve historical same-price FIFO |
|Memory |Active-order cap is not a pod RAM guarantee; map/queue/GC/restore peaks and realistic sparse books need profiling/soak |
|Provisioning |Fixed two-ticker overlay, runtime max16 configured tokens; dynamic ownership/controller not implemented |
|HA/security |Single PG/NATS replicas, local credentials/HTTP diagnostics, no production failover/SLO/compliance certificate |
|Trade audit/order entry |Local order-state snapshots, not a customer order gateway, settlement system or durable execution-pair audit ledger |
|Telemetry |Bounded traces/SQL/socket metrics implemented; OpenTelemetry/Go continuous profiling/node-level eBPF integration discussed but **not implemented** |
|XDP/eBPF |Possible future ingress protection/profiling—not a demonstrated remedy for current admission/database queueing |

The attempted **20million-input/peak10k** full-path experiment did **not** complete successfully; UDP loss/native gaps and environment interruptions invalidated it. Exact surviving prefixes are not capacity certification. Subsequent async-ingest controls were narrower experiments, and the latest paired matching test was20k/arm at achieved~86/~500 inputs/s.

**Engineering takeaway:** preserve order identity, sequencing, atomicity and recovery first; measure the complete path and its queues; treat parallelism, extra services and kernel optimization as hypotheses—not performance guarantees.
