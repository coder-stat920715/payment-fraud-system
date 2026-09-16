package com.publicissapient.paymentfraud.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Intermediate aggregation value held in the Kafka Streams state store while
 * a 5-minute tumbling window is open for a given userId.
 *
 * This is deliberately a plain POJO (serialized with a JSON Serde at the
 * aggregation step - see FraudDetectionTopology) rather than an Avro type:
 * it is internal topology/state-store plumbing that never crosses a public
 * topic boundary, so it doesn't need schema-registry governance. Only the
 * final HighRiskAlertEvent - the thing other services actually consume -
 * is Avro.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpendAccumulator {

    private BigDecimal totalAmount = BigDecimal.ZERO;
    private List<String> paymentIds = new ArrayList<>();

    public SpendAccumulator add(BigDecimal amount, String paymentId) {
        this.totalAmount = this.totalAmount.add(amount);
        this.paymentIds.add(paymentId);
        return this;
    }
}
