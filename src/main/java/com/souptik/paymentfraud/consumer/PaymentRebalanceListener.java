package com.souptik.paymentfraud.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom rebalance listener demonstrating graceful handling of partition
 * revocation - the #1 source of subtle bugs and duplicate-processing
 * incidents in real Kafka consumer applications.
 *
 * WHY this matters: when a consumer group rebalances (a new instance joins,
 * an instance is considered dead by the group coordinator, or partition
 * count changes), every affected consumer loses ownership of some or all of
 * its assigned partitions. Any in-memory state keyed by partition (local
 * aggregation buffers, batched-write caches, etc.) held by THIS consumer for
 * a partition it is about to lose becomes orphaned if not flushed - the next
 * owner of that partition starts fresh from the last COMMITTED offset, so
 * unflushed local state silently disappears (or worse, gets reprocessed
 * inconsistently).
 *
 * Sequencing guarantee Kafka gives us: onPartitionsRevokedBeforeCommit is
 * called BEFORE the consumer's offsets are auto-committed by the framework
 * and before the partitions are reassigned - this is our last guaranteed
 * chance to flush buffered state and commit offsets for exactly what we
 * actually processed.
 */
@Component
public class PaymentRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentRebalanceListener.class);

    /**
     * Example in-memory buffer keyed by partition - stands in for whatever
     * batched/aggregated state a real listener might accumulate between
     * manual commits (e.g. a local fraud-score cache, a batch-insert buffer).
     */
    private final Map<TopicPartition, List<Object>> partitionBuffers = new ConcurrentHashMap<>();

    public void bufferForPartition(TopicPartition partition, Object item) {
        partitionBuffers.computeIfAbsent(partition, p -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(item);
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("Partitions ASSIGNED to this instance: {}", partitions);
        // Optional: warm any partition-scoped local state / caches here
        // (e.g. pre-load recent balances for the users on these partitions).
    }

    /**
     * Called AFTER the container has already committed the offsets it knows
     * about for the revoked partitions (framework-managed manual-immediate
     * acks are already durable at this point). Good place for logging/metrics
     * and releasing any partition-scoped resources that don't need to
     * participate in the commit itself.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("Partitions REVOKED (post-commit) from this instance: {}", partitions);
        partitions.forEach(partitionBuffers::remove);
    }

    /**
     * THE critical hook: fires before the container's own offset commit for
     * the revoked partitions. We flush any buffered state for exactly these
     * partitions here so nothing is lost, and so what we flush is consistent
     * with what gets committed.
     */
    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            List<Object> buffered = partitionBuffers.get(partition);
            if (buffered != null && !buffered.isEmpty()) {
                log.info("Flushing {} buffered items for revoked partition {} before rebalance completes",
                        buffered.size(), partition);
                flush(partition, buffered);
            }
        }
        // NOTE: we deliberately do NOT call consumer.commitSync() here for
        // manually-committed offsets that the listener has already
        // acknowledged - Spring's container handles that. This hook is for
        // flushing APPLICATION state, not for taking over offset management.
    }

    private void flush(TopicPartition partition, List<Object> buffered) {
        // Stand-in for a real flush: batch DB write, cache eviction, metrics
        // publish, etc. Must be fast - a slow flush here delays the whole
        // group's rebalance (a "rebalance storm" risk if this routinely
        // takes longer than session.timeout.ms across many instances).
        buffered.clear();
    }
}
