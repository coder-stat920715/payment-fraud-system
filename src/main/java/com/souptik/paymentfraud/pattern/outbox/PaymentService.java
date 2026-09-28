package com.souptik.paymentfraud.pattern.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.souptik.paymentfraud.domain.Payment;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Demonstrates the TRANSACTIONAL OUTBOX PATTERN.
 *
 * Problem it solves: "update the DB" and "publish to Kafka" are two
 * different systems. Without this pattern you have an inherent dual-write
 * problem - e.g. if the DB commit succeeds but the app crashes before the
 * Kafka send, the event is silently lost; if you flip the order and publish
 * first, you can publish an event for a DB write that then fails to commit.
 * There's no cheap way to make "commit to Postgres" and "publish to Kafka"
 * a single atomic operation without XA/2PC (which Kafka doesn't support
 * well, and which hurts availability/throughput even where it's possible).
 *
 * The outbox pattern sidesteps this entirely: we only ever need ONE ACID
 * transaction, against ONE database. The Kafka publish is decoupled into an
 * asynchronous relay step that reads already-committed outbox rows.
 */
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final EntityManager entityManager;
    private final OutboxRepository outboxRepository;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    /**
     * The business write (INSERT into `payments`) and the outbox write
     * (INSERT into `outbox`) happen inside the SAME @Transactional method -
     * i.e. the same database transaction. Either both rows are committed, or
     * (on any exception) neither is.
     */
    @Transactional
    public UUID initiatePayment(String userId, String merchantId, BigDecimal amount, String currency) {

        UUID paymentId = UUID.randomUUID();

        Payment payment = Payment.builder()
                .id(paymentId)
                .userId(userId)
                .merchantId(merchantId)
                .amount(amount)
                .currency(currency)
                .status("INITIATED")
                .createdAt(Instant.now())
                .build();
        entityManager.persist(payment);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("Payment")
                .aggregateId(paymentId.toString())
                .eventType("PaymentInitiatedEvent")
                .payload(toJson(payment))
                .kafkaTopic(paymentInitiatedTopic)
                .kafkaKey(userId)
                .status(OutboxEvent.OutboxStatus.PENDING)
                .createdAt(Instant.now())
                .build();
        entityManager.persist(outboxEvent);

        // Published NOW, but @TransactionalEventListener(phase = AFTER_COMMIT)
        // guarantees the actual handler only runs after THIS transaction has
        // durably committed - so we never signal "go publish" for a payment
        // that could still be rolled back later in this same method (if we
        // added more logic below that throws).
        applicationEventPublisher.publishEvent(new OutboxEventCommittedSignal(outboxEvent.getId()));

        return paymentId;
    }

    @SneakyThrows
    private String toJson(Payment payment) {
        Map<String, Object> mirror = new HashMap<>();
        mirror.put("paymentId", payment.getId().toString());
        mirror.put("userId", payment.getUserId());
        mirror.put("merchantId", payment.getMerchantId());
        mirror.put("amount", payment.getAmount());
        mirror.put("currency", payment.getCurrency());
        mirror.put("initiatedTimestamp", payment.getCreatedAt().toEpochMilli());
        return objectMapper.writeValueAsString(mirror);
    }
}
