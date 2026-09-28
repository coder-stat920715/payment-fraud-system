package com.souptik.paymentfraud.pattern.idempotent;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * IDEMPOTENT CONSUMER PATTERN, implemented with a Redis SETNX-equivalent
 * check (Redis's `SET key value NX EX <ttl>` in a single atomic command).
 *
 * Why we still need this even with an idempotent PRODUCER and
 * MANUAL_IMMEDIATE acks: Kafka's delivery guarantee to a consumer is
 * fundamentally AT-LEAST-ONCE. Concretely, this exact sequence is possible
 * and is NOT prevented by anything on the producer side:
 *
 *   1. Consumer polls a batch, processes paymentId=P123 successfully
 *      (writes to DB, calls downstream authorization, etc.)
 *   2. Consumer calls acknowledgment.acknowledge() to commit the offset...
 *   3. ...but the process crashes / the broker connection drops / a
 *      rebalance is triggered BEFORE that commit is durably recorded by the
 *      broker.
 *   4. On restart (or on the new partition owner after rebalance), the
 *      consumer resumes from the LAST COMMITTED offset, which is still
 *      BEFORE P123 - so P123 is delivered and processed a second time.
 *
 * This is unavoidable with at-least-once delivery + any side effect that
 * isn't itself naturally idempotent (e.g. "charge card" or "send SMS" are
 * NOT naturally idempotent operations). The fix is to make the CONSUMER's
 * processing idempotent explicitly: before doing real work, atomically check
 * "have I seen this exact business key before" and skip if so.
 *
 * Redis SETNX (via RedisTemplate.opsForValue().setIfAbsent(...)) is a good
 * fit because the check-and-set is a single atomic operation on the Redis
 * server - there's no race window between "check" and "set" the way there
 * would be with a naive SELECT-then-INSERT against a DB from application code.
 */
@Service
@RequiredArgsConstructor
public class IdempotentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumerService.class);

    private final StringRedisTemplate redisTemplate;

    /**
     * TTL bounds how long we remember a processed key. It should comfortably
     * exceed the maximum realistic re-delivery window (consumer restart time,
     * rebalance settling time, retry-topic backoff chain duration) but not be
     * "forever", to bound Redis memory. 24h is a reasonable default for a
     * payments use case where the retry/backoff chain tops out around
     * (1s + 2s + 4s) seconds and DLT/manual replay could plausibly happen
     * within the same business day.
     */
    private static final Duration DEDUP_TTL = Duration.ofHours(24);

    /**
     * Atomically marks `dedupKey` as processed IF AND ONLY IF it was not
     * already marked. Returns true if this call is the one that "won" (i.e.
     * this is genuinely the first time we're seeing this key) - the caller
     * should proceed with processing. Returns false if the key was already
     * present - the caller should skip processing (it's a duplicate delivery).
     */
    public boolean markProcessedIfAbsent(String dedupKey) {
        Boolean wasAbsent = redisTemplate.opsForValue().setIfAbsent(dedupKey, "1", DEDUP_TTL);
        boolean firstTime = Boolean.TRUE.equals(wasAbsent);
        if (!firstTime) {
            log.debug("Idempotency check: key={} already processed, skipping", dedupKey);
        }
        return firstTime;
    }

    /**
     * Rolls back the "processed" marker. Called when processing THROWS after
     * we've already claimed the key - without this, a legitimate retry
     * (e.g. via @RetryableTopic) would be incorrectly skipped as a
     * "duplicate" even though the original attempt never actually completed.
     *
     * Trade-off to flag in an interview: there is a small window between
     * markProcessedIfAbsent() succeeding and the eventual unmark() on failure
     * where a CONCURRENT duplicate delivery (e.g. from a rebalance handing
     * the same still-in-flight record to another consumer, which can't
     * actually happen within one partition/group but could across a
     * differently-configured replay tool) would be incorrectly treated as
     * "already handled". This is why Redis dedup is paired with the DB-level
     * `processed_messages` ledger + normal DB unique constraints as defense
     * in depth for anything where a duplicate side effect would be costly.
     */
    public void unmark(String dedupKey) {
        redisTemplate.delete(dedupKey);
    }
}
