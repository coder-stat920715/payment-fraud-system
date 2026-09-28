package com.souptik.paymentfraud.pattern.outbox;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity mapping to the `outbox` table. Written in the SAME local
 * transaction as the business entity change (see PaymentService) - this is
 * the entire mechanism that makes the Transactional Outbox pattern work:
 * a single ACID database transaction guarantees the business row and this
 * outbox row are either both committed or both rolled back, with no
 * distributed transaction / 2PC required across the DB and Kafka.
 */
@Entity
@Table(name = "outbox")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private UUID id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;

    @Column(columnDefinition = "jsonb")
    private String payload; // JSON-serialized Avro-compatible payload (or a JSON mirror of the event)

    private String kafkaTopic;
    private String kafkaKey;

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    private Instant createdAt;
    private Instant publishedAt;

    public enum OutboxStatus {
        PENDING, PUBLISHED, FAILED
    }
}
