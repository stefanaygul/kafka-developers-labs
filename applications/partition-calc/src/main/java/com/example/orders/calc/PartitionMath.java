package com.example.orders.calc;

import java.util.List;
import java.util.Random;

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
        // TODO D1-02-1: Key'i Kafka'nın yaptığı gibi önce bayt dizisine çevir (StringSerializer).
        //   Sonra Utils sınıfındaki murmur2 ve toPositive metotlarıyla hash'i hesapla,
        //   partition sayısına göre modunu al. Slayt 20'deki formül ile aynı olmalı.
        return 0;
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
            // TODO D1-02-2: Bu key'in partition'ını partitionFor ile bul ve o partition'ın
            //   sayacını bir artır.
        }
        return counts;
    }

    /**
     * Partition sayısı değişince key'lerin yüzde kaçının başka partition'a gittiğini döndürür.
     * Yüzde 0 ile 100 arasındadır.
     */
    public static double movedKeyPercent(List<String> distinctKeys, int fromPartitions, int toPartitions) {
        // TODO D1-02-3: Her key için partitionFor sonucunu iki partition sayısıyla karşılaştır.
        //   Sonucu farklı olan key sayısını toplam key sayısına oranla, yüzdeye çevir.
        return 0.0;
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
