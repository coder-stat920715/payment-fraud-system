package com.publicissapient.paymentfraud.consumer;

import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import com.publicissapient.paymentfraud.pattern.idempotent.IdempotentConsumerService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Primary consumer for PaymentInitiatedEvent.
 *
 * Layered resiliency strategy:
 *
 *   1. ErrorHandlingDeserializer (configured on the ConsumerFactory) already
 *      protects us from POISON-PILL bytes - a record that fails Avro
 *      deserialization never even reaches this method; it's routed straight
 *      to the DLT by Spring's DeserializationException-aware error handling
 *      inside @RetryableTopic's machinery.
 *
 *   2. @RetryableTopic - for exceptions thrown from WITHIN this method
 *      (business-logic failures: a downstream call timing out, a transient
 *      DB error, etc.), Spring Kafka transparently creates
 *      "payment.initiated.events-retry-0", "-retry-1", "-retry-2" topics
 *      (non-blocking: the main partition keeps moving, retries happen on
 *      separate topics/timers) with EXPONENTIAL BACKOFF, and after
 *      exhausting attempts, forwards the record to
 *      "payment.initiated.events.DLT".
 *
 *   3. Manual, per-record offset acknowledgment (AckMode.MANUAL_IMMEDIATE on
 *      the container factory) - we only ack after the idempotent
 *      dedup-and-process logic has fully completed.
 *
 *   4. Idempotent Consumer Pattern (Redis SETNX) - guards against
 *      re-delivery on consumer restart before the last ack was durable
 *      (classic "processed but not yet committed" window in at-least-once
 *      delivery).
 */
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final IdempotentConsumerService idempotentConsumerService;
    private final PaymentRebalanceListener rebalanceListener;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),        // 1s, then 2s (3rd attempt is the original + 2 retries = "3 attempts" total)
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE, // -retry-0, -retry-1 ...
            dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class },                       // don't retry on, say, IllegalArgumentException bugs you'd rather fail fast on - tune per domain
            listenerContainerFactory = "kafkaListenerContainerFactory"
    )
    @KafkaListener(
            topics = "${app.kafka.topics.payment-initiated}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentInitiated(
            @Payload PaymentInitiatedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        String dedupKey = "payment-event:" + event.getPaymentId();

        // Idempotent Consumer Pattern: SETNX-style check BEFORE doing any
        // real work. If another instance (or an earlier, un-acked delivery
        // of this exact record after a rebalance) already processed this
        // paymentId, we skip straight to acknowledging and return - this is
        // what turns Kafka's at-least-once delivery into effectively-once
        // PROCESSING from the business logic's point of view.
        boolean firstTimeProcessing = idempotentConsumerService.markProcessedIfAbsent(dedupKey);
        if (!firstTimeProcessing) {
            log.info("Duplicate delivery detected for paymentId={} (topic-partition={}-{}, offset={}) - skipping reprocessing",
                    event.getPaymentId(), topic, partition, offset);
            acknowledgment.acknowledge();
            return;
        }

        try {
            log.info("Processing PaymentInitiatedEvent paymentId={} userId={} amount={} (partition={}, offset={})",
                    event.getPaymentId(), event.getUserId(), event.getAmount(), partition, offset);

            // Buffer into the rebalance-aware holder so that IF a rebalance
            // happens mid-batch, onPartitionsRevokedBeforeCommit can flush
            // exactly what this partition has accumulated so far.
            rebalanceListener.bufferForPartition(new TopicPartition(topic, partition), event);

            processPayment(event);

            acknowledgment.acknowledge();

        } catch (Exception ex) {
            // Roll back the "already processed" marker so a legitimate retry
            // (via @RetryableTopic) is allowed to actually reprocess instead
            // of being skipped as a false-positive duplicate.
            idempotentConsumerService.unmark(dedupKey);
            log.error("Error processing paymentId={}, will be retried by @RetryableTopic", event.getPaymentId(), ex);
            throw ex; // rethrow so @RetryableTopic's machinery catches it and schedules the next retry-topic hop
        }
    }

    private void processPayment(PaymentInitiatedEvent event) {
        // Domain logic placeholder: fraud pre-check, persistence, downstream
        // authorization call, etc. Kept out of this class to keep the
        // consumer focused on Kafka plumbing (SRP).
    }

    /**
     * Dedicated listener for the Dead Letter Topic. In production this would
     * typically page/alert (PagerDuty, Slack webhook) and persist the failed
     * record for manual replay/investigation rather than silently logging.
     */
    @DltHandler
    public void handleDlt(
            @Payload PaymentInitiatedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS) Integer attempts,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {

        log.error("DLT: paymentId={} from originalTopic={} exhausted {} attempts. Last error: {}",
                event.getPaymentId(), originalTopic, attempts, exceptionMessage);

        // e.g. persist to an `incident_review` table + fire an alert.
    }
}
