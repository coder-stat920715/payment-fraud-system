package com.publicissapient.paymentfraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Real-Time Payment & Fraud Processing System.
 *
 * @EnableKafka           - activates @KafkaListener processing.
 * @EnableKafkaStreams    - activates the auto-configured StreamsBuilderFactoryBean
 *                          backing our KafkaStreamsConfig topology bean.
 * @EnableRetry            - backs the outbox relay's retry semantics and any
 *                          @Retryable service-level methods.
 * @EnableScheduling       - backs the OutboxRelayScheduler poller (fallback
 *                          publisher for the transactional outbox pattern).
 */
@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
@EnableRetry
@EnableScheduling
public class PaymentFraudApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentFraudApplication.class, args);
    }
}
