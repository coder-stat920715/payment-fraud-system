# Real-Time Payment & Fraud Processing System

An end-to-end, production-grade reference project built with **Spring Boot 3.2**, **Apache Kafka**,
**Kafka Streams**, and **Confluent Schema Registry (Avro)** — designed as hands-on interview prep
for enterprise-level Spring/Kafka roles (e.g. Publicis Sapient Principal / Lead Engineer tracks).

---

## 1. Running it locally

```bash
docker-compose up -d
mvn generate-sources      # materializes Avro SpecificRecord classes from src/main/avro/*.avsc
mvn spring-boot:run
```

- Kafka UI: http://localhost:8080
- Schema Registry: http://localhost:8081
- App: http://localhost:8090

```bash
curl -X POST http://localhost:8090/api/v1/payments \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","merchantId":"merchant-7","amount":12500.00,"currency":"USD"}'
```

---

## 2. Package structure

```
config/     Producer/Consumer factories, topic provisioning, Streams config
producer/   Idempotent + transactional KafkaTemplates, custom Partitioner
consumer/   @KafkaListener + @RetryableTopic, rebalance listener, DLT monitor
streams/    Kafka Streams fraud-detection topology (5-min tumbling window)
schema/     (src/main/avro/*.avsc — Avro schema sources)
domain/     JPA entities + the Streams aggregation accumulator POJO
pattern/    Transactional Outbox + Idempotent Consumer (Redis SETNX)
service/    REST ingestion controller
```

---

## 3. Architecture at a glance

```
REST API → PaymentService (DB tx: payments + outbox row)
                 │
                 ▼ (AFTER_COMMIT event + scheduled poller safety net)
        OutboxEventPublisher ──► payment.initiated.events (Avro, idempotent producer)
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                                            ▼
     @KafkaListener (PaymentEventConsumer)          Kafka Streams (FraudDetectionTopology)
     manual-ack, Redis dedup, @RetryableTopic         5-min tumbling window, sum(amount)/userId
     → .DLT on exhaustion → DltMonitorConsumer         → filter > $15,000 → fraud.high-risk.alerts
```

---

## 4. Schema Evolution Strategies (Confluent Schema Registry)

Schema Registry enforces a **compatibility mode** per subject (defaults to `BACKWARD` for the
whole registry here, configurable per-topic-subject). Getting this wrong is one of the most
common causes of a full outage in a Kafka-based system, so it's worth being precise:

| Mode | Producer can... | Consumer can... | Typical use |
|---|---|---|---|
| **BACKWARD** | Use the **new** schema | Read with the **new** schema, data written with the **old** schema still parses | Upgrade **consumers first** — new consumer must tolerate old data. Add optional fields with defaults; remove fields that had defaults. |
| **FORWARD** | Use the **old** schema | Read with the **old** schema, data written with the **new** schema still parses | Upgrade **producers first** — old consumers must tolerate new data. Add fields (consumer ignores unknown ones); remove optional fields. |
| **FULL** | Either | Either, in either direction | Safest but most restrictive: only additive changes with defaults, applied to both sides, are allowed. Use when producer/consumer deploy order can't be guaranteed. |
| **NONE** | Anything | — | Registry stops protecting you. Avoid in production. |

**How Schema Registry prevents broken consumers:** every producer resolves/registers its schema's
ID against the registry before sending (the wire format is `[magic byte][4-byte schema ID][Avro
payload]` — not the full schema on every message). If a producer's new schema fails the subject's
configured compatibility check against the latest registered schema, **registration is rejected
at publish time** — the bad schema never reaches the topic, so no consumer downstream can ever be
handed a message it can't parse. This shifts a class of runtime failures ("consumer crash-loops on
a field type change") into a **build/deploy-time** failure instead.

In this project:
- `PaymentInitiatedEvent.metadata` was added as `["null", {"type":"map","values":"string"}]` with
  `"default": null` — a FORWARD/BACKWARD-safe additive change.
- `PaymentProcessedEvent.failureReason` is nullable with a default — same pattern.
- Removing a field is only safe if that field had a **default** in the schema version being
  removed (so old readers falling back to the default for a field no longer being written stay
  valid) — this is the BACKWARD side of removal.

---

## 5. Key Interview Q&A

**Q: Why does `enable.idempotence=true` require `max.in.flight.requests.per.connection <= 5`?**
The broker's idempotence dedup logic tracks the last 5 (in Kafka 3.x+) produce requests' sequence
numbers per partition to detect and drop retried duplicates while still allowing pipelining for
throughput. Beyond 5 in-flight requests, a retry could be reordered relative to unacked requests in
a way the broker's dedup window can no longer safely reconcile, risking either lost ordering or a
rejected produce.

**Q: enable.idempotence alone vs. a full Kafka transaction — what's actually different?**
Idempotence gives **exactly-once per partition, per producer session**, for retried sends —
duplicates from network-level retries are eliminated. It says nothing about **atomicity across
multiple partitions/topics**, and nothing about a **consume-transform-produce** loop (i.e. it
doesn't stop you from processing the same input record twice and producing two different outputs
if you crash between them). Transactions add: (a) atomic multi-partition/multi-topic writes, and
(b) `read_committed` isolation so downstream consumers never see writes from an aborted
transaction — which is what makes end-to-end exactly-once semantics (EOS) in a
consume-transform-produce pipeline possible (this is exactly what `processing.guarantee =
exactly_once_v2` gives Kafka Streams internally).

**Q: What is a "rebalance storm" and how do you avoid one?**
A rebalance storm is a cascade of back-to-back rebalances — often triggered when a consumer's
`max.poll.interval.ms` is exceeded (processing took too long between polls, so the group
coordinator considers it dead and kicks it out), which itself triggers ANOTHER rebalance as that
instance rejoins, which can perturb timing for other members and trigger further rebalances. Root
causes and mitigations:
- **Slow processing per poll batch** → lower `max.poll.records`, or move heavy work off the
  listener thread (but then you must handle acking correctly against async completion).
- **Long GC pauses** → JVM/heap tuning; rebalance storms often correlate with GC pauses exceeding
  `session.timeout.ms`.
- **Frequent instance churn** (autoscaling flapping, crash-looping pods) → fix the underlying
  instability; also consider **static group membership** (`group.instance.id`) so a
  temporarily-restarting instance doesn't trigger a rebalance at all within
  `session.timeout.ms`, it just resumes with its prior assignment.
- **Cooperative rebalancing** (`CooperativeStickyAssignor`, Kafka's default incremental protocol
  since 3.0 via `PartitionAssignor` config) reduces the blast radius further — only the
  partitions that actually need to move are revoked, instead of a full stop-the-world rebalance
  of every partition in the group.

**Q: How do you avoid duplicate processing during producer network retries specifically?**
This is exactly what the idempotent producer (`enable.idempotence=true`) solves at the Kafka
layer — the broker itself deduplicates retried sends by `(Producer ID, sequence number)` before
they're ever appended to the log, so a client-side retry after a timed-out-but-actually-successful
send never results in two copies of the record on the topic. This is a **producer→broker**
guarantee; it does not by itself prevent a **consumer** from processing the same already-written
record twice (that's the separate, consumer-side idempotent-consumer problem, addressed in this
project via Redis SETNX dedup).

**Q: Why manual, per-record (`MANUAL_IMMEDIATE`) acknowledgment instead of auto-commit?**
Auto-commit commits offsets on a timer, independent of whether your listener actually finished
processing a given record — a crash between "auto-commit fired" and "processing actually
completed" silently loses that record from the consumer's perspective (it'll never be redelivered,
because the offset says it was already handled). Manual-immediate ack means we only advance the
offset after our business logic (persist, publish, whatever) has actually succeeded, giving true
at-least-once semantics instead of Kafka's weaker "at-most-once under auto-commit" failure mode.

**Q: What breaks if you register `auto.register.schemas=true` in production?**
Any producer instance — including a buggy branch deploy or a local dev machine misconfigured to
point at prod — can silently register a new (possibly wrong, possibly incompatible-in-a-way-the-
compatibility-check-still-allows) schema version as the latest for a subject. In a mature
pipeline, schemas are registered explicitly via CI/CD (e.g. the Confluent Schema Registry Maven
plugin's `schema-registry:register` goal, gated behind a compatibility check in the pipeline)
BEFORE the code that uses them is deployed — this project sets `auto.register.schemas=false` for
exactly that reason.

**Q: Tumbling vs. hopping/sliding windows — when would you pick which for fraud detection?**
Tumbling windows (used here) give **non-overlapping, mutually exclusive** buckets — each event
counts toward exactly one window, which is correct for "total spend in this discrete 5-minute
period." Hopping/sliding windows overlap by design (e.g. a 5-minute window advancing every 1
minute) — useful when you want a **smoothed, continuously-updated** view of "spend over the
trailing 5 minutes" (closer to a real-time moving average), at the cost of the same event
contributing to multiple concurrent window results, and materially higher state-store and
compute overhead (many more active windows in flight at once).

**Q: Log compaction vs. time/size retention — how do you decide which topic gets which?**
Ask: "does a consumer of this topic ever need to replay OLD, superseded values for a given key, or
only ever the LATEST value per key?" If only the latest value per key matters (a changelog /
current-state topic backing a KTable, like `user-account-balances` here), compaction is correct —
it keeps storage bounded indefinitely without an arbitrary time cutoff, and a brand-new consumer
bootstrapping the KTable from scratch gets a compact, complete snapshot instead of the entire
event history. If the topic is a true append-only **event log** where history itself has value
(audit trail, replay-for-reprocessing, event sourcing), time/size retention is correct instead.

**Q: Your `@RetryableTopic` exhausts retries and a record lands on the DLT — walk through exactly
what happens end-to-end.**
1. The listener throws inside `consumePaymentInitiated`.
2. Spring Kafka's retry-topic machinery catches it, computes the next backoff delay (1s → 2s per
   the `@Backoff(delay=1000, multiplier=2.0)` config), and publishes the record (with retry-count
   headers) to `payment.initiated.events-retry-0`.
3. A dedicated internal listener container for that retry topic waits out the backoff, then
   re-delivers to the same handler logic.
4. After the configured `attempts` are exhausted (3 total: original + 2 retries here), the record
   is published to `payment.initiated.events.DLT` instead of being retried again.
5. The `@DltHandler` method (or, independently, `DltMonitorConsumer`'s own consumer group) picks it
   up for terminal handling — logging, alerting, and persisting for manual investigation/replay.
6. Throughout, the ORIGINAL partition's consumption is never blocked — records after the failing
   one keep flowing on the main topic the whole time (this is the "non-blocking" part of
   non-blocking retries, as opposed to blocking the whole partition with `RetryTemplate`-style
   in-place retry loops).

**Q: Why keep the Idempotent Consumer Redis check even though you already have an idempotent
producer AND manual-ack?**
Because those two guarantees solve two different halves of the problem, and neither covers
"consumer crashes/rebalances after processing but before its offset commit is durable" — see the
detailed walkthrough in `IdempotentConsumerService`'s Javadoc. At-least-once delivery to the
consumer is a property of Kafka's design, not a bug to be tuned away; the idempotent-consumer
pattern is how you convert "at-least-once delivered" into "effectively-once processed" from the
business logic's point of view, for operations (like charging a payment) that are not naturally
idempotent themselves.
