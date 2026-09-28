package com.souptik.paymentfraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declarative topic provisioning via Spring's KafkaAdmin auto-configuration:
 * any NewTopic @Bean present in the context is automatically created (or
 * reconciled - e.g. partition count increases) against the broker on startup.
 *
 * Two distinct retention strategies are demonstrated, matched to how each
 * topic's data is actually consumed:
 *
 *   - payment.initiated.events / payment.processed.events: classic EVENT LOG.
 *     We care about every historical event (audit, replay, reprocessing), so
 *     we use time+size-bounded deletion (cleanup.policy=delete).
 *
 *   - user-account-balances: a CURRENT-STATE topic (the "changelog" style
 *     topic backing a KTable). We only ever care about the LATEST balance per
 *     userId key, so log compaction is the right retention strategy -
 *     Kafka retains only the most recent record per key indefinitely,
 *     discarding superseded values in the background.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    @Value("${app.kafka.topics.payment-processed}")
    private String paymentProcessedTopic;

    @Value("${app.kafka.topics.user-account-balances}")
    private String userAccountBalancesTopic;

    @Value("${app.kafka.topics.high-risk-alerts}")
    private String highRiskAlertsTopic;

    @Value("${app.kafka.partitions.default}")
    private int defaultPartitions;

    /**
     * Raw transaction event log: retained 7 days OR until the partition hits
     * 50GB, whichever comes first (retention.bytes is a PER-PARTITION limit,
     * not a total-topic limit - worth calling out explicitly in an interview).
     */
    @Bean
    public NewTopic paymentInitiatedTopic() {
        return TopicBuilder.name(paymentInitiatedTopic)
                .partitions(defaultPartitions)
                .replicas(1) // set to 3 in a real multi-broker cluster
                .config("cleanup.policy", "delete")
                .config("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000))   // 7 days
                .config("retention.bytes", String.valueOf(50L * 1024 * 1024 * 1024)) // 50GB per partition
                .config("min.insync.replicas", "1") // set to 2 in production with replicas=3
                .build();
    }

    @Bean
    public NewTopic paymentProcessedTopic() {
        return TopicBuilder.name(paymentProcessedTopic)
                .partitions(defaultPartitions)
                .replicas(1)
                .config("cleanup.policy", "delete")
                .config("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000))
                .config("retention.bytes", String.valueOf(50L * 1024 * 1024 * 1024))
                .build();
    }

    /**
     * Compacted topic backing the "latest balance per user" KTable. Notice
     * there is NO retention.ms / retention.bytes here - compaction alone
     * governs cleanup: Kafka's log cleaner thread periodically rewrites each
     * segment, keeping only the last record per key (a record with a null
     * value acts as a "tombstone" that deletes the key entirely after
     * delete.retention.ms).
     */
    @Bean
    public NewTopic userAccountBalancesTopic() {
        return TopicBuilder.name(userAccountBalancesTopic)
                .partitions(defaultPartitions)
                .replicas(1)
                .compact() // shorthand for .config("cleanup.policy", "compact")
                .config("min.cleanable.dirty.ratio", "0.5")  // trigger compaction once 50% of the segment is "dirty"
                .config("delete.retention.ms", String.valueOf(24L * 60 * 60 * 1000)) // tombstones kept 24h so lagging consumers still see the delete
                .config("segment.ms", String.valueOf(10L * 60 * 1000)) // roll segments every 10 min so recent writes become eligible for compaction sooner
                .build();
    }

    @Bean
    public NewTopic highRiskAlertsTopic() {
        return TopicBuilder.name(highRiskAlertsTopic)
                .partitions(defaultPartitions)
                .replicas(1)
                .config("cleanup.policy", "delete")
                .config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000)) // keep alerts longer - 30 days, for audit/compliance
                .build();
    }
}
