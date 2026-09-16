package com.publicissapient.paymentfraud.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer-side configuration.
 *
 * Two distinct ProducerFactory/KafkaTemplate pairs are configured on purpose:
 *
 *   1. A plain idempotent, non-transactional template for high-throughput,
 *      single-topic publishing (e.g. PaymentInitiatedEvent ingestion) where
 *      we don't need cross-topic atomicity.
 *
 *   2. A transactional template (with a KafkaTransactionManager) for flows
 *      that must atomically write to MULTIPLE topics (or read-process-write
 *      consume-then-produce flows) - e.g. publishing PaymentProcessedEvent
 *      AND a user-account-balances update together, all-or-nothing.
 *
 * Interview note: idempotence (enable.idempotence=true) guarantees exactly-once
 * delivery PER PARTITION for retried sends. Transactions go further: they give
 * you atomicity ACROSS multiple partitions/topics (and, combined with
 * read_committed consumers, exactly-once processing end-to-end).
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${app.kafka.transactional-id-prefix}")
    private String transactionalIdPrefix;

    // -------------------------------------------------------------------
    // 1) Idempotent, non-transactional producer
    // -------------------------------------------------------------------
    private Map<String, Object> baseIdempotentProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", false);

        // ---- Idempotence & durability guarantees ----
        // Broker assigns each producer a PID + per-partition sequence number;
        // duplicate retries of the same (PID, sequence) are silently dropped
        // by the broker, so a network blip that causes a client-side retry
        // NEVER results in a duplicate record on the topic.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // acks=all: leader waits for the full ISR set to replicate before ack.
        // Without this, idempotence only protects against duplicates - it does
        // NOT protect against acknowledged-then-lost data if the leader dies
        // before followers catch up.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry essentially forever - safe ONLY because idempotence is on.
        // Without idempotence, aggressive retries would create duplicates.
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // Kafka guarantees no message reordering with idempotence enabled for
        // up to 5 in-flight requests per connection (broker-side dedup buffer
        // covers this window). >5 would risk reordering on retry.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "com.publicissapient.paymentfraud.producer.HighValueTransactionPartitioner");
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(baseIdempotentProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // -------------------------------------------------------------------
    // 2) Transactional producer - for atomic multi-topic writes
    // -------------------------------------------------------------------
    @Bean
    public ProducerFactory<String, Object> transactionalProducerFactory() {
        Map<String, Object> props = baseIdempotentProducerConfigs();
        // Setting a transactional.id automatically implies enable.idempotence=true
        // and upgrades the producer to a "transactional producer". Each app
        // instance MUST have a unique, STABLE transactional.id across restarts
        // (Spring appends a per-thread suffix to this prefix automatically via
        // DefaultKafkaProducerFactory when transactionIdPrefix is set) so that
        // the broker can fence off zombie producer instances after a restart.
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix(transactionalIdPrefix);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    /**
     * Registers the transactional producer factory with Spring's declarative
     * @Transactional support. This lets a service method annotated
     * @Transactional("kafkaTransactionManager") atomically publish to several
     * topics - either all sends are visible to read_committed consumers, or
     * none are (on rollback, the transaction is aborted and the records are
     * never exposed).
     */
    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }
}
