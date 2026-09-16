# Testing Guide — Real-Time Payment & Fraud Processing System

This guide covers **every feature** in the project end to end. Postman drives the HTTP-facing
pieces (REST API, Schema Registry REST API); everything Kafka-native (producer internals,
rebalancing, DLT, Streams windowing, compaction, Redis dedup) is verified with the CLI tools
that ship inside the `kafka` container, plus Kafka UI at `http://localhost:8080`.

**Import into Postman first:**
`File → Import` → select both `Payment-Fraud-System.postman_collection.json` and
`Payment-Fraud-System-Local.postman_environment.json`, then pick the **"Payment Fraud System -
Local"** environment in the top-right environment dropdown before running any request.

---

## 0. Setup

```bash
docker-compose up -d
docker-compose ps          # all 5 containers should be "healthy" / "Up"
mvn generate-sources        # materializes Avro classes
mvn spring-boot:run
```

Open a second terminal and get a shell inside the Kafka container for all CLI commands below:

```bash
docker exec -it kafka bash
```

Confirm topics were auto-created by the `KafkaAdmin`/`NewTopic` beans on app startup:

```bash
kafka-topics --bootstrap-server localhost:29092 --list
```
Expect: `payment.initiated.events`, `payment.processed.events`, `user-account-balances`,
`fraud.high-risk.alerts`, plus retry/DLT topics created lazily on first failure
(`payment.initiated.events-retry-0`, `-retry-1`, `payment.initiated.events.DLT`).

---

## 1. Idempotent Producer

**Goal:** confirm no duplicate records land on the topic even under retries.

1. In `application.yml`, temporarily point `spring.kafka.producer.bootstrap-servers`-equivalent
   at a wrong port, or simpler: use `tc`/toxiproxy, OR just trust the config and inspect it live:
   ```bash
   kafka-configs --bootstrap-server localhost:29092 --describe \
     --entity-type topics --entity-name payment.initiated.events
   ```
2. Fire the **"Initiate Payment"** request in Postman 20 times via the **Collection Runner**
   (Runner → select the request → Iterations: 20 → Run).
3. Count records actually on the topic and compare to 20:
   ```bash
   kafka-run-class kafka.tools.GetOffsetShell \
     --broker-list localhost:29092 --topic payment.initiated.events
   ```
   Sum the per-partition offsets — it should equal exactly the number of successful POSTs, never
   more (that's the idempotence guarantee: even if Spring Kafka's internal retry logic resent a
   record after a timeout, the broker's `(PID, sequence)` dedup means it's never double-appended).
4. Check application logs for `enable.idempotence` confirmation on producer startup (Kafka logs
   the resolved producer config at DEBUG on `org.apache.kafka.clients.producer.ProducerConfig`).

---

## 2. Transactional Producer (atomic multi-topic write)

`publishPaymentProcessedAtomically()` isn't wired to a REST endpoint by default (it's called from
downstream processing logic) — exercise it directly with a small `@SpringBootTest`, or add a
temporary test endpoint. Either way, verify atomicity like this:

1. Add a breakpoint / temporary `throw new RuntimeException("force rollback")` right after the
   **second** `transactionalKafkaTemplate.send(...)` call in `PaymentProducerService`.
2. Invoke the method (via a quick test or temp controller).
3. Confirm **neither** topic received the record:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:29092 \
     --topic payment.processed.events --from-beginning --isolation-level read_committed --timeout-ms 5000
   kafka-console-consumer --bootstrap-server localhost:29092 \
     --topic user-account-balances --from-beginning --isolation-level read_committed --timeout-ms 5000
   ```
   Both should show nothing new from that attempt — `read_committed` consumers never see records
   from an aborted transaction.
4. Remove the forced exception, re-run, and confirm **both** topics now show the record.

---

## 3. Custom Partitioner (high-value routing)

1. Send **"Initiate Payment (normal, < $10k)"** and **"Initiate HIGH-VALUE Payment (> $10k)"**
   from Postman.
2. Open Kafka UI (`http://localhost:8080`) → Topics → `payment.initiated.events` → Messages.
3. Confirm the high-value message's **Partition** column shows `0`; the normal-value message
   should show a partition determined by the userId hash (likely not 0, though it's not
   guaranteed to differ every time — vary the userId across a few low-value requests to see the
   spread across partitions 0–5, vs. every high-value request landing on 0 regardless of userId).

---

## 4. Schema Management & Evolution

Run the three **Schema Registry** folder requests in Postman, in order:

1. **List all subjects** — confirms `payment.initiated.events-value` etc. are registered (send at
   least one payment first so the subject exists).
2. **Check compatibility - SAFE additive change** → expect `{"is_compatible": true}`.
3. **Check compatibility - BREAKING change** → expect `{"is_compatible": false}`. This is the
   concrete proof that Schema Registry rejects an incompatible schema **before** any producer
   using it could ever poison the topic for existing consumers.
4. Try actually **registering** the breaking schema (not just checking compatibility) to see the
   full rejection response:
   ```bash
   curl -X POST http://localhost:8081/subjects/payment.initiated.events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"schema": "<breaking schema JSON from the Postman request body>"}'
   ```
   Expect HTTP 409 Conflict with `"error_code":409...incompatible schema"`.

---

## 5. Consumer Layer, Manual Acks & Rebalancing

1. Start **two instances** of the app on different ports:
   ```bash
   mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8091"
   # (separate terminal) default port 8090 instance already running
   ```
   Both share `group-id: payment-processing-group`, so Kafka splits the topic's 6 partitions
   between them (3 each, roughly, via the default `CooperativeStickyAssignor`).
2. Watch both apps' logs for `Partitions ASSIGNED to this instance: [...]` from
   `PaymentRebalanceListener` — confirm the partition sets are disjoint and together cover 0–5.
3. **Kill instance #2** (Ctrl+C). In instance #1's logs, watch for the rebalance: first
   `onPartitionsRevokedBeforeCommit` fires for the still-alive instance's own partitions (it
   briefly gives them up during the protocol handshake, depending on assignor), then
   `onPartitionsAssigned` fires again once the group stabilizes with instance #1 now owning
   everything.
4. Confirm processing resumed with **no gaps and no duplicates**: send 5 payments right after
   killing instance #2, confirm all 5 appear exactly once downstream (check via Kafka UI's
   consumer-group lag view — lag should return to 0).
5. Cross-check in Kafka UI → Consumers → `payment-processing-group` → see the live partition
   assignment table update in real time as you kill/restart instances.

---

## 6. Poison Pill Protection (`ErrorHandlingDeserializer`)

1. Produce a deliberately corrupt (non-Avro) record directly onto the topic:
   ```bash
   echo "not-valid-avro-bytes-at-all" | kafka-console-producer \
     --bootstrap-server localhost:29092 --topic payment.initiated.events \
     --property "parse.key=true" --property "key.separator=:" <<< "poison-key:garbage-payload"
   ```
2. Watch the app logs. **Expected:** the listener container stays alive and continues consuming
   subsequent records — you will NOT see a fatal `SerializationException` crash-loop. Depending on
   your error handler wiring, the record either logs a deserialization error and is skipped, or
   (if you've routed `DeserializationException`s into `@RetryableTopic`'s handling) it flows
   straight to the DLT.
3. Immediately send a normal payment via Postman — confirm it's processed normally right after,
   proving the poison pill didn't take down the container.

---

## 7. Non-Blocking Retries & Dead Letter Topic

1. Temporarily make `processPayment()` in `PaymentEventConsumer` throw for a specific test user:
   ```java
   if ("retry-test-user".equals(event.getUserId().toString())) {
       throw new RuntimeException("Simulated transient failure");
   }
   ```
2. Restart the app, then send a payment for `userId: "retry-test-user"` via Postman.
3. Watch the retry topics fill and drain in Kafka UI (Topics list):
   - `payment.initiated.events-retry-0` — record appears ~1s after the original failure.
   - `payment.initiated.events-retry-1` — record appears ~2s after that (exponential backoff,
     multiplier 2.0).
4. After the 3rd attempt fails, confirm the record lands on `payment.initiated.events.DLT`:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:29092 \
     --topic payment.initiated.events.DLT --from-beginning --timeout-ms 5000
   ```
5. Check app logs for both sinks firing: the `@DltHandler` log line (`"DLT: paymentId=... exhausted
   3 attempts"`) AND the independent `DltMonitorConsumer`'s `[DLT-ALERT]` log line — confirming two
   separate consumer groups both received the same DLT record.
6. Revert the temporary throw, restart, and confirm `retry-test-user` payments process normally.

---

## 8. Kafka Streams — Fraud Detection Topology

Use the **"Trigger Fraud Alert"** Postman request with the **Collection Runner**:

1. Runner → select **"Trigger Fraud Alert (5x rapid payments...)"** → Iterations: **5** → Delay:
   **0ms** → Run. This posts 5 × $3,200 = $16,000 for `userId: "fraud-test-user"`.
2. Because the window uses `Suppressed.untilWindowCloses`, the alert only emits after
   `windowSize (5 min) + grace (30s)` has elapsed from the window's start — **wait ~5.5 minutes**
   after the first of the 5 requests.
3. Watch for it in Kafka UI → Topics → `fraud.high-risk.alerts` → Messages, or via CLI:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:29092 \
     --topic fraud.high-risk.alerts --from-beginning --timeout-ms 10000
   ```
4. Confirm exactly **one** alert for `fraud-test-user`, with `totalAmount = 16000.00` and
   `triggeringPaymentIds` containing all 5 payment IDs — proof the tumbling window correctly
   aggregated all 5 events into a single bucket rather than emitting 5 separate partial alerts.
5. **Negative test:** repeat with only 3 requests ($9,600 total, under the $15,000 threshold) —
   confirm NO alert is emitted after waiting out the window.
6. Inspect the windowed state store directly to see intermediate (pre-suppression) state while the
   window is still open:
   ```bash
   kafka-topics --bootstrap-server localhost:29092 --list | grep changelog
   ```
   You'll see an internal changelog topic like
   `fraud-detection-streams-app-user-spend-per-window-store-changelog` — this backs the KTable and
   updates on every event even though the *suppressed* output stream only emits once.

---

## 9. Storage & Retention Configuration

**Time/size retention (event log):**
```bash
kafka-configs --bootstrap-server localhost:29092 --describe \
  --entity-type topics --entity-name payment.initiated.events
```
Confirm `retention.ms=604800000` (7 days) and `retention.bytes=53687091200` (50GB).

**Log compaction (`user-account-balances`):**
```bash
kafka-configs --bootstrap-server localhost:29092 --describe \
  --entity-type topics --entity-name user-account-balances
```
Confirm `cleanup.policy=compact`.

To actually *observe* compaction in action (it runs asynchronously, so this needs patience or a
forced trigger):
```bash
# Produce 3 records with the SAME key, different values
for v in 100.00 250.00 999.00; do
  echo "user-1:{\"balance\":$v}" | kafka-console-producer \
    --bootstrap-server localhost:29092 --topic user-account-balances \
    --property "parse.key=true" --property "key.separator=:"
done

# Force an immediate compaction pass by lowering the dirty-ratio trigger and
# rolling a new segment, then wait ~30s for the log cleaner thread's next cycle
kafka-configs --bootstrap-server localhost:29092 --alter \
  --entity-type topics --entity-name user-account-balances \
  --add-config min.cleanable.dirty.ratio=0.0,segment.ms=1000

sleep 35
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic user-account-balances --from-beginning --timeout-ms 5000
```
Expect to eventually see only the **last** value (999.00) for `user-1`, not all three — the log
cleaner has compacted away the superseded records.

---

## 10. Transactional Outbox Pattern

1. Send **"Initiate Payment"** via Postman, capture the returned `paymentId`.
2. Immediately query Postgres directly to see the outbox row (starts `PENDING`, then flips to
   `PUBLISHED` within ~2s once `OutboxEventPublisher`'s scheduled relay or the `AFTER_COMMIT`
   listener fires):
   ```bash
   docker exec -it postgres psql -U payment_app -d payment_fraud_db \
     -c "SELECT id, event_type, status, created_at, published_at FROM outbox ORDER BY created_at DESC LIMIT 5;"
   ```
3. Confirm the matching `payments` row exists too — same transaction, both committed together:
   ```bash
   docker exec -it postgres psql -U payment_app -d payment_fraud_db \
     -c "SELECT id, user_id, amount, status FROM payments ORDER BY created_at DESC LIMIT 5;"
   ```
4. **Crash-recovery test** (proves the scheduled-poller safety net, not just the fast path):
   temporarily comment out the `applicationEventPublisher.publishEvent(...)` call in
   `PaymentService` (simulating "app crashed after DB commit, before the in-process event fired"),
   restart, send a payment. The row will sit `PENDING` until the next `@Scheduled(fixedDelay =
   2000)` sweep in `OutboxEventPublisher.relayPendingOutboxEvents()` picks it up anyway — confirm
   it still reaches `PUBLISHED` within ~2 seconds, just via the poller instead of the event
   listener.

---

## 11. Idempotent Consumer Pattern (Redis SETNX)

1. Send a payment via Postman, note the `paymentId`.
2. Manually re-publish the **exact same** `paymentId` to the topic (simulating a redelivery after
   a rebalance) using `kafka-console-producer` with a raw Avro-encoded payload, OR simpler:
   temporarily add a debug endpoint that calls `kafkaTemplate.send(...)` twice with the same
   `PaymentInitiatedEvent` object.
3. Check Redis directly for the dedup key:
   ```bash
   docker exec -it redis redis-cli GET "payment-event:<paymentId>"
   docker exec -it redis redis-cli TTL "payment-event:<paymentId>"
   ```
   Expect value `1` and a TTL close to 86400s (24h).
4. Confirm in app logs that the second delivery logged
   `"Duplicate delivery detected for paymentId=... skipping reprocessing"` rather than reprocessing.
5. **Failure-path test:** force `processPayment()` to throw for one specific paymentId, confirm
   the Redis key is removed (`IdempotentConsumerService.unmark()`) so the subsequent
   `@RetryableTopic` retry is correctly treated as a genuine first attempt, not a false-positive
   duplicate:
   ```bash
   docker exec -it redis redis-cli GET "payment-event:<that-paymentId>"   # should be (nil) between attempts
   ```

---

## 12. Actuator / Observability Sanity Check

Run the **Actuator** folder in Postman:
- `GET /actuator/health` → `status: "UP"`, with nested `db`, `redis`, `kafka` (if the Kafka health
  indicator is on the classpath) all `UP`.
- `GET /actuator/metrics/kafka.consumer.records.consumed.total` → confirms Micrometer is capturing
  live consumer throughput, useful evidence during a live interview demo of the rebalance test.

---

## Quick Reference: all CLI commands used above

```bash
# Topics
kafka-topics --bootstrap-server localhost:29092 --list
kafka-topics --bootstrap-server localhost:29092 --describe --topic payment.initiated.events

# Config inspection
kafka-configs --bootstrap-server localhost:29092 --describe --entity-type topics --entity-name <topic>

# Offsets (message count proxy)
kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic <topic>

# Consume (human-readable, only works cleanly for non-Avro / when piped through the Avro console consumer)
kafka-console-consumer --bootstrap-server localhost:29092 --topic <topic> --from-beginning

# Produce raw bytes (for poison-pill test)
kafka-console-producer --bootstrap-server localhost:29092 --topic <topic>

# Consumer groups
kafka-consumer-groups --bootstrap-server localhost:29092 --list
kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group payment-processing-group
```

For Avro-aware console consumption (decodes the wire format instead of showing raw bytes), use
Confluent's `kafka-avro-console-consumer` if available in your image, or just read messages via
Kafka UI, which decodes Avro automatically using the configured Schema Registry.
