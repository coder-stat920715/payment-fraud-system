package com.publicissapient.paymentfraud.producer;

import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import com.publicissapient.paymentfraud.avro.PaymentProcessedEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * Producer-facing service used by the ingestion REST layer.
 *
 * Demonstrates two distinct delivery models:
 *   - publishPaymentInitiated(): fire via the plain idempotent template.
 *     Single topic, single record -> idempotence + acks=all is sufficient.
 *   - publishPaymentProcessedAtomically(): a read-process-write style flow
 *     that must update TWO topics (the processed-event topic AND the
 *     user-account-balances compacted topic) atomically. Wrapped in a Kafka
 *     transaction via {@link KafkaTransactionManager}, driven declaratively
 *     with @Transactional("kafkaTransactionManager").
 */
@Service
@RequiredArgsConstructor
public class PaymentProducerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Qualifier("transactionalKafkaTemplate")
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

    @Value("${app.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    @Value("${app.kafka.topics.payment-processed}")
    private String paymentProcessedTopic;

    @Value("${app.kafka.topics.user-account-balances}")
    private String userBalancesTopic;

    /**
     * Non-transactional, idempotent publish. The broker-side idempotence
     * check means that if this CompletableFuture times out client-side and
     * Spring Kafka's retry logic resends, the broker recognizes the retried
     * (PID, sequence) pair and discards the duplicate - the topic will still
     * only ever contain ONE copy of this event.
     */
    public CompletableFuture<SendResult<String, Object>> publishPaymentInitiated(PaymentInitiatedEvent event) {
        // Key by userId: guarantees all of a user's payment events land on
        // the same partition and are consumed IN ORDER - required for the
        // Kafka Streams windowed aggregation to produce correct per-user totals.
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(paymentInitiatedTopic, event.getUserId().toString(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish PaymentInitiatedEvent paymentId={} after exhausting retries",
                        event.getPaymentId(), ex);
                // In production: emit a metric + optionally persist to a local
                // fallback store for manual replay. Do NOT silently swallow.
            } else {
                log.info("Published PaymentInitiatedEvent paymentId={} partition={} offset={}",
                        event.getPaymentId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
        return future;
    }

    /**
     * Atomic multi-topic publish using a Kafka transaction.
     *
     * Both sends below either BOTH become visible to read_committed consumers
     * or NEITHER does - there is no possibility of a downstream consumer
     * seeing the processed-event without the corresponding balance update,
     * even if this JVM crashes mid-way (the transaction coordinator will
     * abort the dangling transaction once its transaction.timeout.ms elapses).
     *
     * Spring detects the active Kafka transaction (started by the
     * "kafkaTransactionManager" @Transactional boundary) and automatically
     * enlists kafkaTemplate.send() calls made on this thread into it, AS LONG
     * AS they go through a KafkaTemplate backed by the SAME transactional
     * ProducerFactory (transactionalKafkaTemplate here).
     */
    @Transactional("kafkaTransactionManager")
    public void publishPaymentProcessedAtomically(PaymentProcessedEvent processedEvent, Object balanceUpdate) {
        transactionalKafkaTemplate.send(paymentProcessedTopic, processedEvent.getUserId().toString(), processedEvent);
        transactionalKafkaTemplate.send(userBalancesTopic, processedEvent.getUserId().toString(), balanceUpdate);

        log.info("Atomically published PaymentProcessedEvent + balance update for paymentId={}",
                processedEvent.getPaymentId());

        // If any exception is thrown anywhere in this method (including from
        // a synchronous downstream call you add later), Spring rolls back the
        // Kafka transaction and NEITHER record ever becomes visible.
    }
}
