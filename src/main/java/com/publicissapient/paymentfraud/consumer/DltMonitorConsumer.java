package com.publicissapient.paymentfraud.consumer;

import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A SEPARATE, dedicated @KafkaListener on the Dead Letter Topic, running
 * under its OWN consumer group ("payment-dlt-monitoring-group").
 *
 * Why have this in addition to the @DltHandler inside PaymentEventConsumer?
 *   - @DltHandler (in PaymentEventConsumer) is the framework-wired sink that
 *     @RetryableTopic forwards exhausted records to as part of the SAME
 *     logical consumer group - good for immediate, synchronous handling
 *     right where the retry topology is defined.
 *   - THIS class represents an independent monitoring/alerting subscriber -
 *     e.g. a lightweight process (or, in a real system, a separate
 *     microservice) that ONLY cares about observing DLT volume and firing
 *     alerts, decoupled from the retry topology's own group. Multiple teams
 *     (on-call/SRE dashboards, fraud-ops tooling) can each attach their own
 *     independent consumer group to the same DLT without interfering with
 *     each other or with the primary retry flow.
 */
@Component
public class DltMonitorConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltMonitorConsumer.class);

    @KafkaListener(
            topics = "${app.kafka.topics.payment-initiated-dlt}",
            groupId = "payment-dlt-monitoring-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void monitorDlt(
            @Payload PaymentInitiatedEvent event,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS) Integer attempts,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            Acknowledgment acknowledgment) {

        // In production: increment a Micrometer counter tagged by
        // originalTopic + exception type, and push to an alerting channel
        // once the rate crosses a threshold (a single DLT record is normal;
        // a burst indicates a systemic issue - e.g. a downstream dependency
        // outage - worth paging on).
        log.error("[DLT-ALERT] paymentId={} originalTopic={} attempts={} lastError={}",
                event.getPaymentId(), originalTopic, attempts, exceptionMessage);

        acknowledgment.acknowledge();
    }
}
