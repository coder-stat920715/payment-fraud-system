package com.publicissapient.paymentfraud;

import com.publicissapient.paymentfraud.avro.PaymentInitiatedEvent;
import com.publicissapient.paymentfraud.avro.PaymentMethod;
import com.publicissapient.paymentfraud.producer.HighValueTransactionPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HighValueTransactionPartitionerTest {

    private static final String TOPIC = "payment.initiated.events";

    private Cluster buildCluster(int numPartitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new PartitionInfo(TOPIC, i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("test-cluster", Collections.singletonList(node), partitions,
                Collections.emptySet(), Collections.emptySet());
    }

    private PaymentInitiatedEvent eventWithAmount(BigDecimal amount) {
        return PaymentInitiatedEvent.newBuilder()
                .setPaymentId("p-1")
                .setUserId("user-1")
                .setMerchantId("merchant-1")
                .setAmount(ByteBuffer.wrap(amount.unscaledValue().toByteArray()))
                .setCurrency("USD")
                .setPaymentMethod(PaymentMethod.CARD)
                .setInitiatedTimestamp(System.currentTimeMillis())
                .build();
    }

    @Test
    void routesHighValueTransactionToPartitionZero() {
        HighValueTransactionPartitioner partitioner = new HighValueTransactionPartitioner();
        Cluster cluster = buildCluster(6);

        PaymentInitiatedEvent highValueEvent = eventWithAmount(new BigDecimal("15000.00"));

        int partition = partitioner.partition(TOPIC, "user-1", "user-1".getBytes(),
                highValueEvent, null, cluster);

        assertThat(partition).isEqualTo(0);
    }

    @Test
    void lowValueTransactionUsesDefaultKeyedPartitioning() {
        HighValueTransactionPartitioner partitioner = new HighValueTransactionPartitioner();
        Cluster cluster = buildCluster(6);

        PaymentInitiatedEvent lowValueEvent = eventWithAmount(new BigDecimal("50.00"));

        int partition = partitioner.partition(TOPIC, "user-1", "user-1".getBytes(),
                lowValueEvent, null, cluster);

        assertThat(partition).isBetween(0, 5);
        // Deterministic: same key should always map to the same partition.
        int partitionAgain = partitioner.partition(TOPIC, "user-1", "user-1".getBytes(),
                lowValueEvent, null, cluster);
        assertThat(partitionAgain).isEqualTo(partition);
    }
}
