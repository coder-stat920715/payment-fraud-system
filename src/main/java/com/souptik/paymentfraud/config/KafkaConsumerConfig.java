package com.souptik.paymentfraud.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer-side configuration.
 *
 * Key design decisions:
 *
 * 1. ErrorHandlingDeserializer wraps KafkaAvroDeserializer ("poison pill"
 *    protection). If a record's bytes fail to deserialize (corrupted
 *    payload, schema-registry outage, or a producer that wrote non-Avro
 *    bytes to this topic by mistake), the raw exception is CAUGHT at the
 *    deserializer level and stashed as a DeserializationException header on
 *    the ConsumerRecord instead of being thrown out of poll(). Without this,
 *    a single bad record would repeatedly crash the listener container on
 *    every re-poll (since the offset was never committed) - a full outage
 *    caused by one bad message.
 *
 * 2. AckMode.MANUAL_IMMEDIATE - offsets are committed only after our
 *    business logic explicitly calls Acknowledgment.acknowledge(), and that
 *    commit happens synchronously/immediately rather than being deferred to
 *    the next poll. This trades a little throughput for a stronger
 *    "at-least-once, and only after successful processing" guarantee.
 *
 * 3. setConcurrency(3) - spins up 3 consumer threads in this instance, each
 *    bound to a subset of the topic's partitions via the normal consumer
 *    group rebalance protocol. Concurrency should never exceed the topic's
 *    partition count (excess threads simply sit idle).
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- Poison-pill protection ---
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);

        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", true);   // deserialize into generated SpecificRecord classes, not GenericRecord

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15_000);

        // Only see records from COMMITTED transactions - required to make
        // the exactly-once semantics of our transactional producer actually
        // mean something on the read side.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Primary listener container factory: manual-immediate acks, 3 concurrent
     * consumer threads, and a DefaultErrorHandler with a short fixed backoff
     * for genuinely transient exceptions that occur BEFORE we hand off to
     * @RetryableTopic (e.g. a listener method throwing before it even
     * inspects the payload). @RetryableTopic-annotated listeners layer their
     * own retry/backoff policy on top of this at the framework level.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // 3 threads -> up to 3 partitions consumed in parallel by THIS
        // instance. Total effective parallelism across the consumer group is
        // bounded by (number of app instances x concurrency), capped at the
        // topic's partition count.
        factory.setConcurrency(3);

        ContainerProperties containerProps = factory.getContainerProperties();
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // A short, bounded retry for infrastructure-level hiccups at the
        // container level. Business-logic retries with exponential backoff +
        // DLT routing are handled per-listener via @RetryableTopic instead.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));

        return factory;
    }
}
