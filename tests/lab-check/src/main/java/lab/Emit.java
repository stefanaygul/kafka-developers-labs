package lab;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Deterministik test verisi ve host tarafı bağlantı denemesi. */
final class Emit {

    private static final String[][] CUSTOMERS = {
        {"Ayşe", "Ankara"}, {"Mehmet", "İzmir"}, {"Zeynep", "İstanbul"}, {"Can", "Bursa"}, {"Elif", "Antalya"}};

    private Emit() {
    }

    static int command(List<String> args) {
        if (args.isEmpty()) {
            throw new CheckFailed("Eksik argüman.", "./lab.sh emit customers|late-events|poison [topic]");
        }
        try (Kafka kafka = new Kafka()) {
            switch (args.get(0)) {
                case "customers" -> customers(kafka);
                case "late-events" -> lateEvents(kafka);
                case "poison" -> poison(kafka, args.size() > 1 ? args.get(1) : "orders");
                default -> throw new CheckFailed("Bilinmeyen veri: " + args.get(0));
            }
        }
        return 0;
    }

    /** KTable için compacted topic ve 50 müşteri. Zaman damgası eski: siparişlerden önce işlensin. */
    static void customers(Kafka kafka) {
        customers(kafka, "customers");
        System.out.println("customers topic'i hazır: 50 müşteri yazıldı (compacted, 6 partition)");
    }

    static void customers(Kafka kafka, String topic) {
        kafka.createTopic(topic, 6, (short) 3, Map.of("cleanup.policy", "compact", "min.insync.replicas", "2"));
        long timestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
        for (int i = 1; i <= 50; i++) {
            String[] person = CUSTOMERS[i % CUSTOMERS.length];
            String id = "customer-" + i;
            String json = String.format("{\"customerId\":\"%s\",\"name\":\"%s\",\"city\":\"%s\"}", id, person[0], person[1]);
            kafka.send(new ProducerRecord<>(topic, null, timestamp, Kafka.bytes(id), Kafka.bytes(json)));
        }
    }

    /** Slayt 69'daki tablo: A, C, B, D, E. Her çalıştırmada yeni bir window kullanılır. */
    static int lateEvents(Kafka kafka) {
        Instant anchor = nextAnchor();
        System.out.println("Window: " + anchor + " - " + anchor.plus(5, ChronoUnit.MINUTES) + ", grace 2 dk");
        sendLateEvents(kafka, "orders", "customer-1", anchor, true);
        return 0;
    }

    /**
     * Hepsi aynı key ile yazılır. Stream time task başınadır: farklı partition'lara dağılsalar
     * repartition topic'ine varış sırası garanti olmaz ve sonuç rastgele değişir.
     */
    static void sendLateEvents(Kafka kafka, String topic, String customer, Instant anchor, boolean verbose) {
        Object[][] events = {{"A", 1}, {"C", 6}, {"B", 3}, {"D", 8}, {"E", 4}};
        for (Object[] event : events) {
            String id = "late-" + event[0] + "-" + anchor.getEpochSecond();
            Instant time = anchor.plus((Integer) event[1], ChronoUnit.MINUTES);
            kafka.sendJson(topic, customer, Kafka.orderJson(id, id, customer, 100, time.toString()));
            if (verbose) {
                System.out.println("  " + event[0] + " event time " + time);
            }
            App.sleep(250);
        }
    }

    /** Stream time geri gitmez: her çalıştırma öncekinden ileri bir window seçer. */
    private static Instant nextAnchor() {
        Path state = Env.ROOT.resolve(".lab-state/late-events-anchor");
        Instant now = Instant.now();
        Instant anchor = Instant.ofEpochSecond(now.getEpochSecond() / 300 * 300);
        try {
            if (Files.exists(state)) {
                Instant previous = Instant.parse(Files.readString(state).trim()).plus(10, ChronoUnit.MINUTES);
                anchor = previous.isAfter(anchor) ? previous : anchor;
            }
            if (anchor.isAfter(now.plus(45, ChronoUnit.MINUTES))) {
                throw new CheckFailed("Window'lar şimdiki zamanın çok ilerisine geçti: " + anchor,
                        "Broker bir saatten ileri zaman damgasını reddeder. Yeni bir application-id ile başla: "
                                + "./lab.sh run order-streams --spring.kafka.streams.application-id=order-streams-2 ve "
                                + ".lab-state/late-events-anchor dosyasını sil.");
            }
            Files.createDirectories(state.getParent());
            Files.writeString(state, anchor.toString());
        } catch (java.io.IOException e) {
            throw new CheckFailed("Durum dosyası yazılamadı: " + e.getMessage());
        }
        return anchor;
    }

    static void poison(Kafka kafka, String topic) {
        var metadata = kafka.send(topic, "poison-key", Kafka.bytes("{\"bozuk\": json değil"));
        System.out.println("Bozuk kayıt yazıldı: " + topic + " partition=" + metadata.partition()
                + " offset=" + metadata.offset());
    }

    /** Host'taki gerçek bir client'in gözünden: hangi partition'a yazılabiliyor? */
    static int probe() {
        String topic = Env.unique("probe");
        int failed = 0;
        try (Kafka kafka = new Kafka()) {
            kafka.createOrdersLikeTopic(topic);
            App.sleep(1000);
            for (Node node : kafka.brokers()) {
                System.out.println("Broker " + node.id() + " kendini şöyle tanıtıyor: " + node.host() + ":" + node.port());
            }
            TopicDescription description = kafka.describeTopic(topic);
            Properties config = Kafka.producerConfig();
            config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8000);
            config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000);
            try (KafkaProducer<byte[], byte[]> producer =
                    new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer())) {
                List<String> results = new ArrayList<>();
                for (TopicPartitionInfo info : description.partitions()) {
                    String leader = info.leader() == null ? "yok" : info.leader().host() + ":" + info.leader().port();
                    try {
                        producer.send(new ProducerRecord<>(topic, info.partition(), null, Kafka.bytes("probe")))
                                .get(10, TimeUnit.SECONDS);
                        results.add("  P" + info.partition() + " leader=" + leader + " -> YAZILDI");
                    } catch (Exception e) {
                        failed++;
                        Throwable cause = e.getCause() == null ? e : e.getCause();
                        results.add("  P" + info.partition() + " leader=" + leader + " -> YAZILAMADI: "
                                + cause.getClass().getSimpleName());
                    }
                }
                results.forEach(System.out::println);
            }
            kafka.deleteTopics(List.of(topic));
        }
        System.out.println(failed == 0 ? "Tüm partition'lara host'tan yazılabiliyor."
                : failed + " partition'a yazılamadı. Bu partition'ların leader adresini host çözebiliyor mu?");
        return failed == 0 ? 0 : 1;
    }

    static Duration seconds(long value) {
        return Duration.ofSeconds(value);
    }
}
