package com.publicissapient.paymentfraud.streams;

import com.publicissapient.paymentfraud.avro.HighRiskAlertEvent;
import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import com.publicissapient.paymentfraud.avro.RiskLevel;
import com.publicissapient.paymentfraud.domain.SpendAccumulator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Real-time fraud detection topology.
 *
 * Pipeline:
 *   PaymentInitiatedEvent stream (keyed by userId)
 *       -> group by userId
 *       -> 5-minute TUMBLING window, sum(amount) + collect paymentIds
 *       -> materialized as a windowed KTable (state store: "user-spend-per-window-store")
 *       -> filter windows where total > $15,000
 *       -> map to HighRiskAlertEvent
 *       -> publish to fraud.high-risk.alerts (Avro)
 *
 * WHY TUMBLING (not hopping/sliding)? We want DISTINCT, non-overlapping
 * 5-minute buckets of spend per user - each payment counts toward exactly
 * ONE window. A hopping window would double-count the same payment across
 * multiple overlapping windows, which is wrong for a "how much did this user
 * spend in this 5-minute period" fraud signal.
 *
 * WHY suppress-until-window-close (Suppressed.untilWindowCloses)? Without
 * it, the KTable emits an UPDATE record on every single new payment within
 * the window (Kafka Streams' default "eager" emission), which would fire the
 * high-risk alert filter repeatedly as the running total crosses the
 * threshold multiple times and would flood the alerts topic. Suppressing
 * until the window's grace period elapses gives us exactly ONE final,
 * correct result per user per window - at the cost of alert latency (you
 * wait up to windowSize + grace before an alert fires). This is the classic
 * Kafka Streams "correctness vs latency" trade-off worth discussing in an
 * interview - a real fraud system might instead emit BOTH an early
 * (unsuppressed) "possible risk" signal for fast triage and a suppressed
 * "confirmed window total" event for audit/compliance.
 */
@Configuration
public class FraudDetectionTopology {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionTopology.class);

    @Value("${app.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    @Value("${app.kafka.topics.high-risk-alerts}")
    private String highRiskAlertsTopic;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${app.kafka.fraud.window-size-minutes:5}")
    private long windowSizeMinutes;

    @Value("${app.kafka.fraud.window-grace-seconds:30}")
    private long windowGraceSeconds;

    @Value("${app.kafka.fraud.risk-threshold-usd:15000}")
    private long riskThresholdUsd;

    @Bean
    public KStream<String, PaymentInitiatedEvent> fraudDetectionStream(StreamsBuilder streamsBuilder) {

        Serde<String> stringSerde = Serdes.String();
        Serde<PaymentInitiatedEvent> paymentSerde = buildAvroSerde(false);
        Serde<HighRiskAlertEvent> alertSerde = buildAvroSerde(false);
        Serde<SpendAccumulator> accumulatorSerde = new JsonSerde<>(SpendAccumulator.class);

        BigDecimal riskThreshold = BigDecimal.valueOf(riskThresholdUsd);
        Duration windowSize = Duration.ofMinutes(windowSizeMinutes);
        Duration grace = Duration.ofSeconds(windowGraceSeconds);

        KStream<String, PaymentInitiatedEvent> paymentStream =
                streamsBuilder.stream(paymentInitiatedTopic, Consumed.with(stringSerde, paymentSerde));

        // ---- 5-minute TUMBLING window, keyed by userId (the record key) ----
        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(windowSize, grace);

        KTable<Windowed<String>, SpendAccumulator> windowedSpendTable = paymentStream
                .groupByKey(Grouped.with(stringSerde, paymentSerde))
                .windowedBy(tumblingWindow)
                .aggregate(
                        SpendAccumulator::new,
                        (userId, event, accumulator) ->
                                accumulator.add(toBigDecimal(event.getAmount()), event.getPaymentId().toString()),
                        Materialized.<String, SpendAccumulator, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>
                                        as("user-spend-per-window-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(accumulatorSerde)
                )
                // Emit exactly once per window, only after the window is
                // truly closed (windowSize + grace elapsed) - see class-level
                // Javadoc for the correctness/latency trade-off this implies.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // ---- Filter to only windows exceeding the risk threshold ----
        KStream<String, HighRiskAlertEvent> highRiskAlerts = windowedSpendTable
                .toStream()
                .filter((windowedUserId, accumulator) ->
                        accumulator != null && accumulator.getTotalAmount().compareTo(riskThreshold) > 0)
                .map((windowedUserId, accumulator) -> {
                    String userId = windowedUserId.key();
                    Window window = windowedUserId.window();

                    // NOTE on logical types: the .avsc declares windowStart/windowEnd
                    // as `long` with logicalType=timestamp-millis, and amount as
                    // `bytes` with logicalType=decimal. The generated SpecificRecord
                    // only exposes these as native Instant/BigDecimal types if the
                    // Avro compiler's logical-type conversions are registered
                    // (avro-maven-plugin's <enableDecimalLogicalType> + a
                    // SpecificData.get().addLogicalTypeConversion(...) call at
                    // startup). To keep this reference project buildable without
                    // that extra wiring, we set the UNDERLYING primitive types
                    // directly here (epoch millis / raw bytes) - functionally
                    // identical on the wire, just without the ergonomic Java type.
                    HighRiskAlertEvent alert = HighRiskAlertEvent.newBuilder()
                            .setUserId(userId)
                            .setTotalAmount(toByteBuffer(accumulator.getTotalAmount()))
                            .setWindowStart(window.start())
                            .setWindowEnd(window.end())
                            .setAlertGeneratedTimestamp(System.currentTimeMillis())
                            .setTriggeringPaymentIds(accumulator.getPaymentIds())
                            .setRiskLevel(classifyRisk(accumulator.getTotalAmount(), riskThreshold))
                            .build();

                    log.warn("HIGH RISK ALERT: userId={} totalSpend={} window=[{} - {}]",
                            userId, accumulator.getTotalAmount(), window.start(), window.end());

                    return KeyValue.pair(userId, alert);
                });

        highRiskAlerts.to(highRiskAlertsTopic, Produced.with(stringSerde, alertSerde));

        return paymentStream;
    }

    private RiskLevel classifyRisk(BigDecimal total, BigDecimal threshold) {
        BigDecimal doubleThreshold = threshold.multiply(BigDecimal.valueOf(2));
        if (total.compareTo(doubleThreshold) >= 0) {
            return RiskLevel.CRITICAL;
        }
        BigDecimal oneAndHalf = threshold.multiply(BigDecimal.valueOf(1.5));
        if (total.compareTo(oneAndHalf) >= 0) {
            return RiskLevel.HIGH;
        }
        return RiskLevel.MEDIUM;
    }

    /** Converts the Avro `decimal` logical-type bytes field into a usable BigDecimal (scale=2, matching the .avsc). */
    private BigDecimal toBigDecimal(Object rawAmount) {
        if (rawAmount instanceof BigDecimal bd) {
            return bd;
        }
        ByteBuffer buffer = ((ByteBuffer) rawAmount).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new BigDecimal(new BigInteger(bytes), 2);
    }

    private ByteBuffer toByteBuffer(BigDecimal amount) {
        return ByteBuffer.wrap(amount.setScale(2, java.math.RoundingMode.HALF_UP).unscaledValue().toByteArray());
    }

    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> buildAvroSerde(boolean isKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("specific.avro.reader", true);
        serde.configure(config, isKey);
        return serde;
    }
}