package com.publicissapient.paymentfraud.producer;

import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Custom Partitioner that isolates high-value transactions (> $10,000) onto a
 * single, dedicated partition (partition 0).
 *
 * WHY route high-value transactions to a dedicated partition at all?
 *   1. Consumer-side prioritization: a dedicated consumer thread/instance can
 *      subscribe with a tighter poll loop / lower max.poll.records just for
 *      partition-0, giving high-value fraud checks lower processing latency
 *      without slowing down the bulk of "normal" traffic.
 *   2. Monitoring/alerting: you can attach partition-scoped consumer-lag
 *      alerts specifically to partition 0 as a proxy for "are we keeping up
 *      with the transactions that matter most".
 *   3. Ordering guarantee preserved: all HIGH-VALUE events funnel through one
 *      partition, so if you need a strict global order of large transactions
 *      (e.g. for sequential fraud-scoring state), you get it for free.
 *
 * TRADE-OFF (worth raising in an interview): routing everything to a single
 * partition creates a hot partition - it does not scale with consumer
 * parallelism for that specific traffic segment. This is a deliberate
 * trade-off of throughput for ordering + prioritization; in a real system
 * you'd watch that partition's throughput and consider a small dedicated
 * "high-value" topic instead if volume grows.
 *
 * For all other records (normal traffic, and any non-PaymentInitiatedEvent
 * value type on a shared topic), we fall back to Kafka's default sticky/
 * round-robin-ish keyed partitioning by hashing the key, exactly like the
 * built-in DefaultPartitioner would.
 */
public class HighValueTransactionPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(HighValueTransactionPartitioner.class);

    private static final int HIGH_VALUE_PARTITION = 0;
    private static final BigDecimal HIGH_VALUE_THRESHOLD = BigDecimal.valueOf(10_000);

    @Override
    public void configure(Map<String, ?> configs) {
        // No dynamic config needed today; threshold is intentionally a
        // compile-time constant to avoid this hot-path method doing config
        // lookups per record. Could be wired from ProducerConfig if it needs
        // to be tunable per-environment.
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                          Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        BigDecimal amount = extractAmount(value);

        if (amount != null && amount.compareTo(HIGH_VALUE_THRESHOLD) > 0) {
            log.debug("Routing high-value transaction (amount={}) for key={} to dedicated partition {}",
                    amount, key, HIGH_VALUE_PARTITION);
            // Guard against a topic that (mis)configured with fewer partitions
            // than expected - never return an out-of-range partition index.
            return HIGH_VALUE_PARTITION < numPartitions ? HIGH_VALUE_PARTITION : defaultPartition(keyBytes, numPartitions);
        }

        return defaultPartition(keyBytes, numPartitions);
    }

    /**
     * Extracts the decimal `amount` field regardless of whether the value
     * arrives as the generated SpecificRecord (PaymentInitiatedEvent) or a
     * GenericRecord (e.g. in tests, or if a schema-agnostic producer path is
     * ever introduced).
     */
    private BigDecimal extractAmount(Object value) {
        try {
            if (value instanceof PaymentInitiatedEvent event) {
                Object rawAmount = event.getAmount();
                return toBigDecimal(rawAmount);
            }
            if (value instanceof GenericRecord record && record.hasField("amount")) {
                return toBigDecimal(record.get("amount"));
            }
        } catch (Exception e) {
            log.warn("Could not extract amount for partitioning decision, falling back to default partitioning", e);
        }
        return null;
    }

    private BigDecimal toBigDecimal(Object rawAmount) {
        if (rawAmount instanceof BigDecimal bd) {
            return bd;
        }
        if (rawAmount instanceof ByteBuffer buffer) {
            // Avro `decimal` logical type over `bytes` arrives as unscaled
            // two's-complement bytes; scale (2) matches the .avsc definition.
            ByteBuffer duplicate = buffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return new BigDecimal(new java.math.BigInteger(bytes), 2);
        }
        return null;
    }

    /**
     * Mirrors Kafka's default keyed-partitioning behaviour (murmur2 hash of
     * the key bytes, modulo partition count) for every record that isn't
     * routed to the dedicated high-value partition.
     */
    private int defaultPartition(byte[] keyBytes, int numPartitions) {
        if (keyBytes == null) {
            // No key -> Kafka's default would use the sticky partitioner for
            // batching efficiency. We approximate with round-robin via a
            // simple random spread since we don't have access to the
            // StickyPartitionCache used internally by the default partitioner.
            return (int) (System.nanoTime() % numPartitions + numPartitions) % numPartitions;
        }
        int hash = org.apache.kafka.common.utils.Utils.murmur2(keyBytes);
        return (Math.abs(hash) % numPartitions);
    }

    @Override
    public void close() {
        // no resources to release
    }
}
