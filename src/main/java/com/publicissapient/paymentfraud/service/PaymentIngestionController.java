package com.publicissapient.paymentfraud.service;

import com.publicissapient.paymentfraud.pattern.outbox.PaymentService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/**
 * Thin ingestion API. Delegates to PaymentService, which demonstrates the
 * transactional outbox pattern (DB write + outbox row in one local
 * transaction; the OutboxEventPublisher relay then gets it onto Kafka).
 *
 * This is the recommended path for anything that ALSO needs a durable
 * system-of-record row (which is most real payment flows). The direct
 * KafkaTemplate path in PaymentProducerService remains useful for
 * pure event-only flows that don't need a corresponding DB write.
 */
@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentIngestionController {

    private final PaymentService paymentService;

    public record InitiatePaymentRequest(
            @NotBlank String userId,
            @NotBlank String merchantId,
            @Positive BigDecimal amount,
            @NotBlank String currency) {
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> initiatePayment(@RequestBody InitiatePaymentRequest request) {
        UUID paymentId = paymentService.initiatePayment(
                request.userId(), request.merchantId(), request.amount(), request.currency());

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "paymentId", paymentId,
                        "status", "INITIATED",
                        "note", "Event will be published to Kafka asynchronously via the transactional outbox relay"
                ));
    }
}
