package com.example.orders.calc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("solution")
class PartitionMathTest {

    @Test
    void matchesKafkaBuiltInPartitioner() {
        for (String key : List.of("order-981", "order-417", "TR", "customer-0001")) {
            for (int partitions : new int[] {3, 6, 12}) {
                int expected = BuiltInPartitioner.partitionForKey(key.getBytes(StandardCharsets.UTF_8), partitions);
                assertEquals(expected, PartitionMath.partitionFor(key, partitions));
            }
        }
    }

    @Test
    void histogramKeepsEveryRecord() {
        assertEquals(KeySets.RECORDS, Arrays.stream(PartitionMath.histogram(KeySets.countries(), 6)).sum());
    }

    @Test
    void uniformHashDoesNotMeanUniformTraffic() {
        int[] counts = PartitionMath.histogram(KeySets.bigCustomer(), 6);
        int max = Arrays.stream(counts).max().orElseThrow();
        assertTrue(max > KeySets.RECORDS / 2, "büyük müşterinin partition'ı toplamin yarısından fazla olmalı");
    }

    @Test
    void increasingPartitionCountMovesKeys() {
        double moved = PartitionMath.movedKeyPercent(KeySets.orderIds(), 3, 6);
        assertTrue(moved > 30 && moved < 70, "3 -> 6 geçişinde key'lerin yaklaşık yarısı yer değiştirir: " + moved);
    }
}
