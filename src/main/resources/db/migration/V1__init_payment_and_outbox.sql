-- Business table: system of record for a payment.
CREATE TABLE payments (
    id              UUID PRIMARY KEY,
    user_id         VARCHAR(64)     NOT NULL,
    merchant_id     VARCHAR(64)     NOT NULL,
    amount          NUMERIC(18,2)   NOT NULL,
    currency        VARCHAR(3)      NOT NULL DEFAULT 'USD',
    status          VARCHAR(32)     NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Transactional Outbox table.
-- WHY: writing to this table happens in the SAME local DB transaction as the
-- `payments` insert above, so it is impossible to commit the business change
-- without also durably recording the fact that an event must be published.
-- A separate relay (poller or Debezium CDC) then reads this table and
-- publishes to Kafka, achieving atomicity across a DB write + a Kafka publish
-- without needing distributed (XA) transactions.
CREATE TABLE outbox (
    id                UUID PRIMARY KEY,
    aggregate_type    VARCHAR(64)   NOT NULL,   -- e.g. "Payment"
    aggregate_id      VARCHAR(64)   NOT NULL,   -- e.g. payment.id
    event_type        VARCHAR(64)   NOT NULL,   -- e.g. "PaymentInitiatedEvent"
    payload           JSONB         NOT NULL,
    kafka_topic       VARCHAR(128)  NOT NULL,
    kafka_key         VARCHAR(128)  NOT NULL,
    status            VARCHAR(16)   NOT NULL DEFAULT 'PENDING',  -- PENDING | PUBLISHED | FAILED
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    published_at      TIMESTAMPTZ
);

-- The relay polls exactly this shape of query very frequently - index it.
CREATE INDEX idx_outbox_status_created_at ON outbox (status, created_at) WHERE status = 'PENDING';

-- Idempotent-consumer processed-message ledger (belt-and-braces alongside the
-- Redis SETNX check - Redis gives low-latency dedup, this table gives a
-- durable audit trail survivable across a full Redis flush).
CREATE TABLE processed_messages (
    message_key     VARCHAR(128)  PRIMARY KEY,
    topic           VARCHAR(128)  NOT NULL,
    processed_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);
