package com.souptik.paymentfraud;

/**
 * Reference outline for testing FraudDetectionTopology with
 * org.apache.kafka.streams.TopologyTestDriver (kafka-streams-test-utils).
 *
 * Kept as an outline/comment (rather than a fully wired test) because
 * exercising the real topology needs a StreamsBuilder wired the same way
 * the @Bean method builds it (including the SpecificAvroSerde pointed at a
 * MockSchemaRegistryClient) - worth building out fully once the project is
 * checked into a real repo with `mvn generate-sources` run to materialize
 * the Avro classes. The key techniques to demonstrate in an interview:
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
 * props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
 * props.put("schema.registry.url", "mock://test-registry");
 *
 * StreamsBuilder builder = new StreamsBuilder();
 * // ... build the same topology as FraudDetectionTopology ...
 * Topology topology = builder.build();
 *
 * try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {
 *     TestInputTopic<String, PaymentInitiatedEvent> input = testDriver.createInputTopic(
 *             "payment.initiated.events", new StringSerializer(), avroSerializer);
 *     TestOutputTopic<String, HighRiskAlertEvent> output = testDriver.createOutputTopic(
 *             "fraud.high-risk.alerts", new StringDeserializer(), avroDeserializer);
 *
 *     // Advance test time explicitly to control tumbling-window boundaries -
 *     // this is the #1 technique for deterministic window testing:
 *     input.pipeInput("user-1", paymentOf(6000), Instant.parse("2026-01-01T00:00:00Z"));
 *     input.pipeInput("user-1", paymentOf(9500), Instant.parse("2026-01-01T00:01:00Z"));
 *     // total = 15500 > 15000 threshold, still within the same 5-min window
 *
 *     // advance past windowSize + grace to force the suppressed KTable to emit:
 *     testDriver.advanceWallClockTime(Duration.ofMinutes(6));
 *
 *     assertThat(output.readValuesToList()).hasSize(1);
 * }
 * }</pre>
 */
class FraudDetectionTopologyTest {
    // See class-level Javadoc - implement using the pattern above once Avro
    // classes are generated (`mvn generate-sources`) and a MockSchemaRegistryClient
    // is wired via `mock://` scheme (provided by kafka-schema-registry-client's test jar).
}
