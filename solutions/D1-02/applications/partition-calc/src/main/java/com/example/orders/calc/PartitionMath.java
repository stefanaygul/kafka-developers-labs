package com.example.orders.calc;

import java.util.List;
import java.util.Random;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

/**
 * Lab D1-02: Bu dosyada çalışıyorsun.
 * Kafka'nın varsayılan partitioner'i key'li kayıtlar için hangi formülü kullanıyor?
 */
public final class PartitionMath {

    /** Key'siz kayıtlarda bir batch'in kaç kayıt taşıdığını varsayıyoruz. */
    private static final int STICKY_BATCH = 1_000;

    private PartitionMath() {
    }

    /**
     * Bir key'in hangi partition'a gideceğini döndürür.
     */
    public static int partitionFor(String key, int partitions) {
        try (StringSerializer serializer = new StringSerializer()) {
            byte[] keyBytes = serializer.serialize("orders", key);
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions;
        }
    }

    /**
     * Kayıtları partition'lara dağıtır ve partition başına kayıt sayısını döndürür.
     * Null key'li kayıtlar stickyPartitions ile dağıtılır; o kısım hazır.
     */
    public static int[] histogram(List<String> keys, int partitions) {
        int[] counts = new int[partitions];
        int[] sticky = stickyPartitions(keys.size(), partitions);
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key == null) {
                counts[sticky[i]]++;
                continue;
            }
            counts[partitionFor(key, partitions)]++;
        }
        return counts;
    }

    /**
     * Partition sayısı değişince key'lerin yüzde kaçının başka partition'a gittiğini döndürür.
     * Yüzde 0 ile 100 arasındadır.
     */
    public static double movedKeyPercent(List<String> distinctKeys, int fromPartitions, int toPartitions) {
        if (distinctKeys.isEmpty()) {
            return 0.0;
        }
        long moved = distinctKeys.stream()
                .filter(key -> partitionFor(key, fromPartitions) != partitionFor(key, toPartitions))
                .count();
        return 100.0 * moved / distinctKeys.size();
    }

    /**
     * Sticky partitioner'ın sadeleştirilmiş modeli: bir batch dolana kadar aynı partition,
     * sonra başka bir partition. Tohum sabit olduğu için herkes aynı sonucu görür.
     */
    private static int[] stickyPartitions(int records, int partitions) {
        Random random = new Random(42);
        int[] assignment = new int[records];
        int current = random.nextInt(partitions);
        for (int i = 0; i < records; i++) {
            if (i > 0 && i % STICKY_BATCH == 0) {
                current = random.nextInt(partitions);
            }
            assignment[i] = current;
        }
        return assignment;
    }
}
