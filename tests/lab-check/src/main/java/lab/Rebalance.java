package lab;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Aynı senaryoyu üç protokolle ölçer: iki consumer çalışırken üçüncüsü katılır.
 * Spring Boot 3.5'in kafka-clients 3.9'u KIP-848'i desteklemez, bu ölçüm 4.x client kullanır.
 *
 * Asıl fark süre değil, kaç partition'ın durduğudur. Süre bu küçük senaryoda heartbeat
 * aralığına bağlıdır (classic 3 sn, KIP-848 5 sn) ve her koşuda değişir.
 */
final class Rebalance {

    private record Mode(String label, String protocol, String assignor) {
    }

    private record Measurement(long completedMs, int stopped, long longestOutageMs) {
    }

    private static final int NEW_MEMBER_SHARE = 2;

    private static final List<Mode> MODES = List.of(
            new Mode("eager (classic, Range)", "classic", "org.apache.kafka.clients.consumer.RangeAssignor"),
            new Mode("cooperative (classic)", "classic", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"),
            new Mode("KIP-848 (consumer)", "consumer", null));

    private Rebalance() {
    }

    static int measure() {
        System.out.printf("%-24s %20s %20s %24s%n", "Protokol", "Atama bitti (ms)", "Duran partition", "En uzun kesinti (ms)");
        for (Mode mode : MODES) {
            Measurement result = run(mode);
            System.out.printf("%-24s %20d %20d %24d%n", mode.label(), result.completedMs(), result.stopped(),
                    result.longestOutageMs());
        }
        System.out.println();
        System.out.println("Duran partition sayısı protokolün farkıdır. Süreler heartbeat aralığına bağlıdır, her koşuda değişir.");
        return 0;
    }

    static void printComparison(Report report) {
        for (Mode mode : MODES) {
            Measurement result = run(mode);
            report.ok(mode.label() + ": " + result.stopped() + " partition durdu, en uzun kesinti "
                    + result.longestOutageMs() + " ms, atama " + result.completedMs() + " ms'de bitti");
        }
    }

    private static Measurement run(Mode mode) {
        String topic = Env.unique("rebalance");
        String group = Env.unique("rebalance-group");
        try (Kafka kafka = new Kafka()) {
            kafka.createOrdersLikeTopic(topic);
            AtomicBoolean running = new AtomicBoolean(true);
            State state = new State();
            List<Thread> threads = new ArrayList<>();
            try {
                threads.add(member(mode, topic, group, running, state, false));
                threads.add(member(mode, topic, group, running, state, false));
                awaitBalanced(kafka, group);
                // Rastgele bekleme: ölçüm heartbeat fazına denk gelmesin.
                App.sleep(ThreadLocalRandom.current().nextLong(3000));
                state.reset();
                long joinedAt = System.currentTimeMillis();
                threads.add(member(mode, topic, group, running, state, true));
                long deadline = joinedAt + 40_000;
                while (state.newMemberPartitions.get() < NEW_MEMBER_SHARE && System.currentTimeMillis() < deadline) {
                    App.sleep(20);
                }
                long completed = System.currentTimeMillis() - joinedAt;
                if (state.newMemberPartitions.get() < NEW_MEMBER_SHARE) {
                    throw new CheckFailed(mode.label() + ": yeni üye payını almadı.");
                }
                App.sleep(1000);
                return new Measurement(completed, state.stopped.get(), state.longestOutage.get());
            } finally {
                running.set(false);
                for (Thread thread : threads) {
                    try {
                        thread.join(8000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                kafka.deleteTopics(List.of(topic));
                kafka.deleteGroups(List.of(group));
            }
        }
    }

    /** İlk atama bitmeden ölçüm başlarsa geçiş halindeki partition'lar yanlış sayılır. */
    private static void awaitBalanced(Kafka kafka, String group) {
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                var members = kafka.admin().describeConsumerGroups(List.of(group)).describedGroups().get(group)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS).members();
                if (members.size() == 2
                        && members.stream().allMatch(m -> m.assignment().topicPartitions().size() == 3)) {
                    return;
                }
            } catch (Exception ignored) {
                // Group henüz oluşmadı.
            }
            App.sleep(200);
        }
        throw new CheckFailed("İki consumer dengeye gelmedi: " + group);
    }

    private static final class State {
        final Map<TopicPartition, Long> revokedAt = new ConcurrentHashMap<>();
        final AtomicInteger stopped = new AtomicInteger();
        final AtomicInteger newMemberPartitions = new AtomicInteger();
        final AtomicLong longestOutage = new AtomicLong();

        void reset() {
            revokedAt.clear();
            stopped.set(0);
            longestOutage.set(0);
        }

        void revoked(Collection<TopicPartition> partitions) {
            long now = System.currentTimeMillis();
            partitions.forEach(partition -> revokedAt.put(partition, now));
            stopped.addAndGet(partitions.size());
        }

        void assigned(Collection<TopicPartition> partitions) {
            long now = System.currentTimeMillis();
            for (TopicPartition partition : partitions) {
                Long since = revokedAt.remove(partition);
                if (since != null) {
                    longestOutage.accumulateAndGet(now - since, Math::max);
                }
            }
        }
    }

    private static Thread member(Mode mode, String topic, String group, AtomicBoolean running, State state,
            boolean newMember) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.BOOTSTRAP);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, mode.protocol());
        if (mode.assignor() != null) {
            config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, mode.assignor());
        }
        Thread thread = new Thread(() -> {
            try (KafkaConsumer<byte[], byte[]> consumer =
                    new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        state.revoked(partitions);
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        state.assigned(partitions);
                        if (newMember) {
                            state.newMemberPartitions.addAndGet(partitions.size());
                        }
                    }
                });
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(100));
                }
            }
        }, "rebalance-" + mode.protocol());
        thread.start();
        return thread;
    }
}
