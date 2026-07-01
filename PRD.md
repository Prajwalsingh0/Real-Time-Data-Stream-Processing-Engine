# PRD: Real-Time Data Stream Processing Engine

**Author:** Prajwal Singh
**Status:** Draft v1
**Target stack:** Java (primary) — C++ acceptable for the storage/network core if you want raw performance work on the resume instead

---

## 1. Vision

Build a distributed, partitioned, log-based message broker + stream processing engine — architecturally similar to Apache Kafka — from scratch. Not a wrapper around RabbitMQ/Kafka client libraries. You own the wire protocol, the storage engine, replication, and consumer group coordination.

The point of this project on a resume isn't "I used Kafka." It's "I understood Kafka well enough to rebuild its core mechanics and reason about the tradeoffs myself." That's what makes it read as advanced rather than a CRUD-with-a-queue project.

---

## 2. Goals

1. **Durable, ordered, partitioned log storage** — the fundamental primitive everything else builds on.
2. **Pub/sub semantics** with producers, consumers, and consumer groups (with partition rebalancing).
3. **A real network protocol** — custom binary protocol over TCP, not HTTP/REST wrapping.
4. **Replication for fault tolerance** — leader/follower per partition, with failover.
5. **At-least-once delivery guarantees**, with a stretch goal of exactly-once.
6. **Observability** — metrics on throughput, lag, latency.
7. **A lightweight stream processing layer** on top (filter/map/aggregate over the log) — this is what elevates it from "broker" to "processing engine."

## 3. Non-Goals (explicitly out of scope for v1)

- Multi-datacenter replication / geo-distribution
- SQL-like query engine (ksqlDB equivalent) — a small typed API is enough
- A UI/dashboard beyond basic metrics endpoint (CLI + logs are fine)
- Kubernetes operator / cloud-native deployment tooling (nice stretch goal, not core)
- Schema registry (Avro/Protobuf schema evolution) — mention as future work only

---

## 4. Core Concepts (what you're actually building)

| Concept | Description |
|---|---|
| **Topic** | Named stream of records, split into partitions |
| **Partition** | An ordered, immutable, append-only log. Unit of parallelism. |
| **Segment** | A partition's log is split into segment files on disk (e.g., 1GB each) for efficient retention/deletion |
| **Offset** | Monotonic position of a record within a partition |
| **Producer** | Client that appends records to a partition (via key hash or round robin) |
| **Consumer** | Client that reads records sequentially from a partition, tracking its own offset |
| **Consumer Group** | Set of consumers that split partitions between them for parallel consumption |
| **Broker** | A server node hosting some set of partitions |
| **Controller** | The broker (or external process) responsible for partition leader election and metadata |
| **Replica** | A copy of a partition's log on another broker for fault tolerance |

---

## 5. Functional Requirements

### 5.1 Storage Engine (the heart of the project)
- Append-only log per partition, backed by segment files on disk.
- Each record: `[length][crc][timestamp][key][value]` binary format.
- In-memory sparse index (offset → file position) per segment for O(log n) seeks, flushed alongside data.
- Log retention policy: time-based and size-based deletion of old segments.
- Sequential disk I/O only (no random writes) — this is the single most important performance property to get right and explain in an interview.

### 5.2 Networking / Protocol
- Custom binary protocol over TCP (define your own frame format: request type, correlation ID, payload length, payload).
- Java: built with **Netty** (industry-standard, and knowing Netty is itself a strong resume signal) or raw NIO if you want more pain/more learning.
- Core APIs: `Produce`, `Fetch`, `Metadata`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit`, `OffsetFetch`.

### 5.3 Partitioning & Routing
- Producer partitions records by `hash(key) % numPartitions` (or round-robin if key is null).
- Broker exposes metadata API so clients know which broker leads which partition.

### 5.4 Consumer Groups & Rebalancing
- Consumers in the same group split partitions among themselves.
- Rebalance protocol triggered on consumer join/leave (start simple: a coordinator broker assigns partitions; range or round-robin assignment strategy).
- Offset commits stored in an internal `__consumer_offsets` topic (yes, build this the same way Kafka does — it's elegant and worth implementing).

### 5.5 Replication & Fault Tolerance
- Each partition has 1 leader + N followers (configurable replication factor).
- Followers pull from the leader continuously (like consumers pulling from a topic).
- **ISR (in-sync replica) set** tracked per partition; leader only acknowledges writes once ISR quorum has replicated (configurable `acks=0/1/all`).
- On leader failure, controller promotes a follower from the ISR set.
- Simple failure detection via heartbeats/session timeout (a minimal Raft or ZAB implementation is a strong stretch goal, but a simpler heartbeat-based controller election is acceptable for v1).

### 5.6 Delivery Guarantees
- At-least-once by default (consumer commits offset after processing).
- Idempotent producer (dedupe by producer ID + sequence number) as a stretch goal toward exactly-once.

### 5.7 Stream Processing Layer
- A small Java API for defining a processing topology reading from one topic and writing to another:
  - `stream("orders").filter(...).map(...).to("valid-orders")`
- Internally: just a consumer + producer pair with your transformation function in between, but expose it as a clean fluent API — this is what turns it from "message queue" into "stream processing engine" on paper.
- Stretch: windowed aggregations (tumbling window count/sum) using local in-memory state.

### 5.8 Observability
- JMX or a simple `/metrics` HTTP endpoint (Prometheus text format) exposing: messages/sec, bytes/sec, consumer lag per group/partition, replication lag.
- Structured logging.

---

## 6. Non-Functional Requirements

| Attribute | Target |
|---|---|
| Throughput | 100K+ msgs/sec single broker on commodity hardware (this is a realistic, benchmarkable, resume-worthy number — measure it, don't guess it) |
| Latency | Sub-10ms p99 produce latency for `acks=1` |
| Durability | No data loss on broker crash if `acks=all` and replication factor ≥ 2 |
| Ordering | Strict ordering guaranteed within a partition |
| Recovery | Broker restart recovers full log state from disk without external dependency |

---

## 7. Suggested Tech Stack (Java)

- **Core:** Java 17+, Netty for networking, java.nio for file I/O (memory-mapped files or FileChannel for the log)
- **Serialization:** Hand-rolled binary protocol (do NOT use Protobuf/Avro for v1 — writing your own framing is the whole point of the exercise)
- **Concurrency:** Java's `java.util.concurrent` — this project is a great excuse to get fluent with `ReentrantReadWriteLock`, `ConcurrentHashMap`, thread pools per connection or per partition
- **Testing:** JUnit 5 + a chaos-testing harness that kills brokers mid-test to verify replication/failover
- **Build:** Maven or Gradle
- **Benchmarking:** JMH for microbenchmarks on the log append/read path, plus a custom load-gen client for end-to-end throughput numbers
- **Metrics:** Micrometer + Prometheus format endpoint

If you go C++ instead: Boost.Asio or raw epoll for networking, mmap for log storage, and you'd skip GC-related JVM tuning discussions in favor of manual memory/lock-free structure discussions in interviews. Both are valid stories — pick based on which skill set you want to sell harder.

---

## 8. Suggested Build Phases (Milestones)

**Phase 1 — Single-node log storage engine**
Append-only segmented log, sparse index, recovery on restart, basic retention. Prove you can write and read millions of records fast with pure sequential I/O.

**Phase 2 — Single-broker pub/sub over the network**
Binary protocol, Netty server, Produce/Fetch APIs, multiple topics/partitions, a simple Java client library.

**Phase 3 — Consumer groups**
Group coordinator, partition assignment, offset commit/fetch, rebalancing on join/leave.

**Phase 4 — Replication & fault tolerance**
Multi-broker cluster, leader/follower replication, ISR tracking, controller-based leader election, failover testing.

**Phase 5 — Stream processing API + observability**
Fluent processing API, metrics endpoint, load-testing harness, README with benchmark numbers and architecture diagrams.

**Phase 6 (stretch)** — Idempotent producer / exactly-once, windowed aggregation, minimal Raft for controller election instead of heartbeat hack.

---

## 9. What Makes This "Resume-Advanced" vs. "Resume-Intermediate"

Intermediate version: single broker, in-memory queue, REST API, no replication.
Advanced version (this PRD): custom binary protocol, disk-backed segmented log with recovery, real replication with ISR and failover, consumer group rebalancing, and measured throughput/latency numbers you can quote in an interview. Phases 1–3 alone are already a strong intermediate-to-advanced project; Phases 4–5 are what push it into "distributed systems" territory.

---

## 10. Deliverables

- Public GitHub repo with clean commit history showing incremental phases (not one giant commit — this matters for how it reads to a reviewer)
- Architecture diagram (broker internals, replication flow, consumer group rebalance sequence)
- Benchmark results (throughput/latency graphs) in the README
- A short design doc per major component (log storage, replication protocol, rebalancing) — these double as interview talking points

---

## 11. Open Questions for You to Decide Next

- Java or C++ — final call?
- Single-machine multi-process cluster simulation, or actual multi-VM/Docker Compose cluster for testing replication?
- How far do you want to push the stream processing layer (simple transforms vs. windowed aggregation)?

Once you pick, we can design the actual package/module structure and start with Phase 1 (the log storage engine).
