package com.publicissapient.paymentfraud.pattern.outbox;

import java.util.UUID;

/**
 * A lightweight, in-process ApplicationEvent published right after the
 * business transaction commits (see PaymentService). It carries just the
 * outbox row's id - the actual payload is re-read from the DB by the relay
 * to guarantee we publish exactly what was durably committed, not a
 * possibly-stale in-memory copy.
 */
public record OutboxEventCommittedSignal(UUID outboxEventId) {
}
