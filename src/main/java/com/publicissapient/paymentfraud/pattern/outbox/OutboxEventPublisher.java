package com.publicissapient.paymentfraud.pattern.outbox;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Relays committed outbox rows to Kafka via TWO complementary mechanisms:
 *
 *   1. FAST PATH - @TransactionalEventListener(phase = AFTER_COMMIT): fires
 *      in-process, milliseconds after the business transaction commits.
 *      Gives near-real-time publish latency in the common case.
 *
 *   2. SAFETY-NET PATH - a @Scheduled poller that periodically sweeps for
 *      any outbox rows still PENDING (e.g. because the app crashed AFTER the
 *      DB commit but BEFORE the in-process AFTER_COMMIT listener ran - an
 *      in-memory ApplicationEvent does not survive a JVM crash, unlike the
 *      already-committed outbox row itself). This is what actually makes the
 *      pattern durable end-to-end; the transactional-event-listener path is
 *      a latency optimization on top of it, not a replacement for it.
 *
 * In a larger production system, step 2 would typically be replaced by a
 * Debezium CDC connector tailing the outbox table's WAL and publishing to
 * Kafka directly - removing the polling relay (and its DB load) entirely.
 * Both approaches are valid; the poller is shown here because it requires no
 * extra infrastructure and is easy to reason about for an interview.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onOutboxEventCommitted(OutboxEventCommittedSignal signal) {
        publishIfPending(signal.outboxEventId());
    }

    /**
     * Runs every 2 seconds and publishes any row still PENDING - covers both
     * the crash-after-commit scenario above and any transient Kafka
     * publish failure from the fast path (which we deliberately do NOT
     * retry inline in onOutboxEventCommitted, to keep that listener fast and
     * side-effect-free beyond the initial attempt).
     */
    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void relayPendingOutboxEvents() {
        List<OutboxEvent> pending =
                outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxEvent.OutboxStatus.PENDING, PageRequest.of(0, 100));

        for (OutboxEvent event : pending) {
            publish(event);
        }
    }

    private void publishIfPending(UUID outboxEventId) {
        Optional<OutboxEvent> maybeEvent = outboxRepository.findById(outboxEventId);
        maybeEvent.filter(e -> e.getStatus() == OutboxEvent.OutboxStatus.PENDING)
                .ifPresent(this::publish);
    }

    private void publish(OutboxEvent event) {
        try {
            // NOTE: in this simplified reference implementation we publish
            // the JSON mirror payload directly. In a strict Avro-everywhere
            // system, this relay would deserialize `payload` into the
            // matching generated Avro SpecificRecord before sending, so the
            // bytes on the topic are true Avro + validated against Schema
            // Registry, exactly like the direct-producer path.
            kafkaTemplate.send(event.getKafkaTopic(), event.getKafkaKey(), event.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            markPublished(event.getId());
                        } else {
                            log.error("Outbox relay failed to publish outboxEventId={}, will retry on next poll",
                                    event.getId(), ex);
                            // Left as PENDING deliberately - the next scheduled
                            // sweep will retry it. A FAILED status is reserved
                            // for rows exceeding a max-attempt count (not shown)
                            // that should be alerted on instead of retried forever.
                        }
                    });
        } catch (Exception e) {
            log.error("Unexpected error relaying outboxEventId={}", event.getId(), e);
        }
    }

    @Transactional
    private void markPublished(UUID outboxEventId) {
        outboxRepository.findById(outboxEventId).ifPresent(event -> {
            event.setStatus(OutboxEvent.OutboxStatus.PUBLISHED);
            event.setPublishedAt(Instant.now());
            outboxRepository.save(event);
        });
    }
}
