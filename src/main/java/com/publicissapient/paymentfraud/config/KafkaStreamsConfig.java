package com.publicissapient.paymentfraud.config;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams configuration. Registered under the exact bean name
 * KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME so that
 * @EnableKafkaStreams picks it up automatically to build the
 * StreamsBuilderFactoryBean consumed by FraudDetectionTopology.
 */
@Configuration
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("schema.registry.url", schemaRegistryUrl);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde.class);

        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024);

        // exactly_once_v2: the topology's reads from the source topic,
        // writes to internal changelog/repartition topics, and writes to the
        // output (HighRiskAlertEvent) topic are all wrapped in a single
        // Kafka transaction PER task-commit-interval. This is what prevents
        // double-counting a user's spend in the windowed aggregation if a
        // stream thread crashes and restarts mid-window.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/payment-fraud");

        // Analogous protection to the consumer's ErrorHandlingDeserializer -
        // a single malformed record read from the source topic logs and
        // SKIPS rather than crashing the whole stream thread. In production
        // you'd typically route the skipped record to a topology-level DLT
        // instead (via a custom DeserializationExceptionHandler) - shown here
        // with LogAndContinue for simplicity/clarity.
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(props);
    }
}
