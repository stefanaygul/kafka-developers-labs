package lab;

import static lab.Checks.expect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;

/** Gün 1 kontrolleri. */
final class Day1 {

    private static final ObjectMapper JSON = new ObjectMapper();

    private Day1() {
    }

    // ---------- D1-01 ----------
    static void firstMessage(Report report) {
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D1-01");
            report.step("KRaft quorum");
            try {
                QuorumInfo quorum = kafka.admin().describeMetadataQuorum().quorumInfo()
                        .get(8, java.util.concurrent.TimeUnit.SECONDS);
                expect(quorum.leaderId() >= 0, "Aktif controller yok.", "docker logs kafka-1 çıktısına bak.");
                expect(quorum.voters().size() == 3, "Beklenen 3 voter, görünen " + quorum.voters().size(), null);
                report.ok("Aktif controller " + quorum.leaderId() + ", 3 voter");
            } catch (CheckFailed e) {
                throw e;
            } catch (Exception e) {
                throw new CheckFailed("Quorum okunamadı: " + e.getMessage());
            }

            report.step("orders topic'i");
            TopicDescription orders = kafka.describeTopic("orders");
            expect(orders != null, "Topic eksik: orders", "README Adım 3: kafka-topics.sh --create");
            expect(orders.partitions().size() == 6,
                    "Yanlış partition sayısı: " + orders.partitions().size() + " (beklenen 6)", "Topic'i silip 6 partition ile aç.");
            int replicas = orders.partitions().get(0).replicas().size();
            expect(replicas == 3, "Yanlış replication factor: " + replicas + " (beklenen 3)", "Topic'i silip RF 3 ile aç.");
            String minIsr = kafka.topicConfig("orders", "min.insync.replicas");
            expect("2".equals(minIsr), "Yanlış min.insync.replicas: " + minIsr + " (beklenen 2)",
                    "Topic'i --config min.insync.replicas=2 ile aç.");
            report.ok("6 partition, RF 3, min.insync.replicas=2");

            report.step("Üretilen kayıtlar");
            List<ConsumerRecord<byte[], byte[]>> records = kafka.readAll("orders", 5000, Duration.ofSeconds(10));
            long keyed = records.stream().filter(r -> r.key() != null).count();
            long keyless = records.size() - keyed;
            expect(keyed >= 3, "Key'li kayıt yok ya da az: " + keyed + " (en az 3 bekleniyor)", "README Adım 4.");
            expect(keyless >= 1, "Key'siz kayıt bulunamadı.", "README Adım 5: parse.key olmadan üret.");
            Map<String, Set<Integer>> partitionsByKey = new HashMap<>();
            records.stream().filter(r -> r.key() != null).forEach(r ->
                    partitionsByKey.computeIfAbsent(Kafka.text(r.key()), k -> new HashSet<>()).add(r.partition()));
            partitionsByKey.forEach((key, partitions) -> expect(partitions.size() == 1,
                    "Aynı key birden fazla partition'da: " + key + " " + partitions,
                    "Topic'in partition sayısı değiştirilmiş olabilir."));
            report.ok(keyed + " key'li, " + keyless + " key'siz kayıt; her key tek partition'da");

            report.step("Consumer group");
            Map<Integer, Long> committed = kafka.committed("d1-01-grup", "orders");
            expect(!committed.isEmpty(), "Consumer group atanmamış: d1-01-grup için commit edilmiş offset yok.",
                    "README Adım 7: --group d1-01-grup ile tüket.");
            report.ok("d1-01-grup " + committed.size() + " partition için offset commit etmiş");

            report.step("Uçtan uca doğrulama");
            String topic = Env.unique("d1-01");
            try {
                kafka.createOrdersLikeTopic(topic);
                Set<Integer> seen = new HashSet<>();
                for (int i = 0; i < 5; i++) {
                    seen.add(kafka.send(topic, "order-1", Kafka.bytes("deneme-" + i)).partition());
                }
                int read = kafka.readAll(topic, 10, Duration.ofSeconds(8)).size();
                expect(read == 5, "Beklenen mesaj tüketilmedi: 5 yazıldı, " + read + " okundu.", null);
                expect(seen.size() == 1, "Aynı key farklı partition'lara gitti: " + seen, null);
                report.ok("5 kayıt yazıldı ve okundu, hepsi aynı partition'da");
            } finally {
                kafka.deleteTopics(List.of(topic));
            }
        }
    }

    // ---------- D1-02 ----------
    static void partitionCalc(Report report) {
        report.step("Derleme");
        App.build("applications/partition-calc");
        report.ok("partition-calc derlendi");

        report.step("TODO D1-02-1: formül");
        String jar = Env.ROOT.resolve("applications/partition-calc/target/partition-calc.jar").toString();
        for (String key : List.of("order-981", "order-417", "TR", "customer-0001")) {
            for (int partitions : new int[] {3, 6, 12}) {
                String out = calc(jar, "--key", key, "--partitions", String.valueOf(partitions)).trim();
                int expected = Utils.toPositive(Utils.murmur2(Kafka.bytes(key))) % partitions;
                expect(out.equals("partition=" + expected),
                        "Key " + key + ", " + partitions + " partition için beklenen " + expected + ", çıktı: " + out,
                        "TODO D1-02-1: PartitionMath.partitionFor içinde murmur2 ve toPositive kullan.");
            }
        }
        report.ok("Hesap Kafka formülüyle aynı");

        report.step("Gerçek broker ile karşılaştırma");
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D1-02");
            String topic = Env.unique("d1-02");
            try {
                kafka.createOrdersLikeTopic(topic);
                for (String key : List.of("order-981", "order-417", "order-503")) {
                    RecordMetadata metadata = kafka.send(topic, key, Kafka.bytes("x"));
                    String out = calc(jar, "--key", key, "--partitions", "6").trim();
                    expect(out.equals("partition=" + metadata.partition()),
                            "Broker partition " + metadata.partition() + " dedi, hesap: " + out, null);
                }
                report.ok("Broker'ın seçtiği partition ile hesap aynı");
            } finally {
                kafka.deleteTopics(List.of(topic));
            }
        }

        report.step("TODO D1-02-2 ve 3: histogram ve taşınma");
        String machine = calc(jar, "--machine");
        for (String line : machine.lines().filter(l -> l.startsWith("histogram")).toList()) {
            String[] parts = line.split(" ", 4);
            long sum = java.util.Arrays.stream(parts[3].replaceAll("[\\[\\] ]", "").split(","))
                    .mapToLong(Long::parseLong).sum();
            expect(sum == 30_000, "Histogram eksik: " + parts[1] + " için toplam " + sum + " (beklenen 30000)",
                    "TODO D1-02-2: histogram döngüsünde sayacı artır.");
        }
        report.ok("Tüm setlerde 30000 kayıt sayıldı");
        String moved = machine.lines().filter(l -> l.startsWith("moved")).findFirst().orElse("");
        double percent = Double.parseDouble(moved.substring(moved.lastIndexOf(' ') + 1));
        expect(percent > 30 && percent < 70, "Taşınan key oranı beklenmedik: %" + percent,
                "TODO D1-02-3: iki partition sayısındaki sonucu karşılaştır.");
        report.ok("3'ten 6'ya geçişte key'lerin %" + percent + " kadarı yer değiştirdi");
    }

    private static String calc(String jar, String... args) {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-jar", jar));
        command.addAll(List.of(args));
        Shell.Result result = Shell.capture(Duration.ofSeconds(30), command);
        expect(result.ok(), "partition-calc hata verdi: " + result.output(), null);
        return result.output();
    }

    // ---------- D1-03 ----------
    static void producerMeasure(Report report) {
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D1-03");
            report.step("Ölçüm topic'i");
            TopicDescription perf = kafka.describeTopic("orders-perf");
            expect(perf != null, "Topic eksik: orders-perf", "README Adım 1.");
            expect(perf.partitions().size() == 6 && perf.partitions().get(0).replicas().size() == 3,
                    "orders-perf 6 partition ve RF 3 olmalı.", "Topic'i silip doğru ayarla aç.");
            report.ok("orders-perf hazır");

            report.step("Çalışan ölçüm");
            Shell.Result run = Shell.kafkaCli(Duration.ofSeconds(60), "kafka-producer-perf-test.sh",
                    "--bootstrap-server", "kafka-1:19092", "--topic", "orders-perf", "--num-records", "2000",
                    "--record-size", "200", "--throughput", "-1",
                    "--command-property", "acks=all", "linger.ms=0", "compression.type=none");
            expect(run.output().contains("records sent"), "Perf test sonuç üretmedi: " + run.output(), null);
            report.ok("kafka-producer-perf-test çalışıyor");

            report.step("Idempotence uyumluluk kuralı");
            Shell.Result conflict = Shell.kafkaCli(Duration.ofSeconds(30), "kafka-producer-perf-test.sh",
                    "--bootstrap-server", "kafka-1:19092", "--topic", "orders-perf", "--num-records", "10",
                    "--record-size", "200", "--throughput", "-1",
                    "--command-property", "acks=1", "enable.idempotence=true");
            expect(conflict.output().contains("Must set acks to all"),
                    "acks=1 ile enable.idempotence=true reddedilmeliydi.", null);
            report.ok("acks=1 ve enable.idempotence=true ConfigException veriyor; bu ölçüm değil, yapılandırma hatası");
        }

        report.step("OLCUM.md");
        Path file = Env.ROOT.resolve(Labs.get("D1-03").directory()).resolve("OLCUM.md");
        String text = read(file);
        expect(!text.contains("___"), "OLCUM.md tamamlanmamış: boş alanlar (___) duruyor.",
                "Kendi ölçümlerini tabloya yaz.");
        report.ok("Ölçüm tablosu dolu");
    }

    static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new CheckFailed("Dosya okunamadı: " + file);
        }
    }

    // ---------- D1-04 ----------
    static void firstSpringApp(Report report) {
        report.step("TODO D1-04-3 ve 4: application.yml");
        String yml = read(Env.ROOT.resolve("applications/order-consumer/src/main/resources/application.yml"));
        expect(yml.contains("localhost:9092") && yml.contains("localhost:9094") && yml.contains("localhost:9096"),
                "bootstrap-servers üç broker'ı içermiyor.", "TODO D1-04-3.");
        expect(yml.matches("(?s).*group-id:\\s*order-processor.*"), "group-id order-processor değil.", "TODO D1-04-4.");
        report.ok("Consumer ayarları tamam");

        report.step("Derleme");
        App.build("applications/order-producer", "applications/order-consumer");
        report.ok("İki uygulama derlendi");

        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D1-04");
            String topic = Env.unique("orders");
            String group = Env.unique("group");
            kafka.createOrdersLikeTopic(topic);
            try (App consumerA = App.start("order-consumer", "--app.topic=" + topic,
                            "--spring.kafka.consumer.group-id=" + group, "--app.instance=A");
                    App producer = App.start("order-producer", "--server.port=0", "--app.topic=" + topic)) {
                report.step("Uygulamalar");
                consumerA.awaitStarted();
                producer.awaitStarted();
                String url = "http://localhost:" + producer.httpPort() + "/orders";
                expect(consumerA.await("REBALANCE instance=A event=ASSIGNED", Duration.ofSeconds(25)) != null,
                        "Consumer group atanmamış: A instance'ına partition gelmedi.",
                        "TODO D1-04-5: onOrder metodunu @KafkaListener ile bağla ve app.topic placeholder'ını kullan.");
                report.ok("Producer ve consumer başladı, partition'lar atandı");

                report.step("TODO D1-04-1 ve 2: üretim ve teslim sonucu");
                String eventId = eventId(App.post(url + "?orderId=order-check-1"));
                Matcher ack = producer.await("ACK eventId=" + eventId + " .*partition=(\\d+) offset=(\\d+)",
                        Duration.ofSeconds(15));
                if (ack == null) {
                    boolean recorded = !kafka.readAll(topic, 5, Duration.ofSeconds(3)).isEmpty();
                    throw new CheckFailed(recorded
                            ? "Producer acknowledgement eksik: kayıt Kafka'da ama ACK logu yok."
                            : "Producer acknowledgement eksik: kayıt Kafka'ya hiç ulaşmadı.",
                            recorded ? "TODO D1-04-2: whenComplete içinde onAck çağır."
                                    : "TODO D1-04-1: kafkaTemplate.send ile gönder.");
                }
                report.ok("ACK alındı: partition=" + ack.group(1) + " offset=" + ack.group(2));

                report.step("TODO D1-04-5: tüketim");
                Matcher consumed = consumerA.await("CONSUMED instance=A groupId=" + group + " partition="
                        + ack.group(1) + " offset=" + ack.group(2) + " .*eventId=" + eventId, Duration.ofSeconds(15));
                expect(consumed != null, "Beklenen mesaj tüketilmedi: eventId=" + eventId,
                        "TODO D1-04-5: Topic adını sabit yazdıysan check kendi topic'ini dinletemez; ${app.topic} kullan.");
                report.ok("Consumer aynı partition ve offset'i logladı");

                report.step("Key davranışı");
                Set<String> partitions = new HashSet<>();
                for (int i = 0; i < 5; i++) {
                    String id = eventId(App.post(url + "?orderId=order-same-key"));
                    Matcher m = producer.await("ACK eventId=" + id + " .*partition=(\\d+)", Duration.ofSeconds(10));
                    expect(m != null, "ACK gelmedi: " + id, null);
                    partitions.add(m.group(1));
                }
                int expected = Utils.toPositive(Utils.murmur2(Kafka.bytes("order-same-key"))) % 6;
                expect(partitions.equals(Set.of(String.valueOf(expected))),
                        "Key partition'ı belirlemiyor: görülen " + partitions + ", beklenen " + expected,
                        "TODO D1-04-1: send çağrısında key parametresini ver.");
                report.ok("Aynı key 5 kez partition " + expected + "'e gitti");

                report.step("Rebalance");
                try (App consumerB = App.start("order-consumer", "--app.topic=" + topic,
                        "--spring.kafka.consumer.group-id=" + group, "--app.instance=B")) {
                    expect(consumerB.await("REBALANCE instance=B event=ASSIGNED", Duration.ofSeconds(40)) != null,
                            "İkinci instance'a partition gelmedi.", null);
                    awaitMembers(kafka, group, 2);
                    report.ok("İki instance gruba girdi, partition'lar bölüşüldü");
                }
                awaitMembers(kafka, group, 1);
                report.ok("İkinci instance çıkınca partition'lar tek instance'a döndü");
                consumerA.matching("event=ASSIGNED").stream().reduce((a, b) -> b).ifPresent(line ->
                        report.info("Son atama: " + line.substring(line.indexOf("REBALANCE"))));
            } finally {
                kafka.deleteTopics(List.of(topic));
                kafka.deleteGroups(List.of(group));
            }
        }
    }

    static String eventId(HttpResponse<String> response) {
        expect(response.statusCode() / 100 == 2, "HTTP " + response.statusCode() + ": " + response.body(), null);
        return json(response.body()).get("eventId").asText();
    }

    static JsonNode json(String text) {
        try {
            return JSON.readTree(text);
        } catch (IOException e) {
            throw new CheckFailed("JSON okunamadı: " + text);
        }
    }

    /** Grubun beklenen üye sayısına ulaşıp partition'ları eşit bölüşmesini bekler. */
    static void awaitMembers(Kafka kafka, String group, int members) {
        long deadline = System.currentTimeMillis() + 30_000;
        String last = "";
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsumerGroupDescription description = kafka.admin().describeConsumerGroups(List.of(group))
                        .describedGroups().get(group).get(5, java.util.concurrent.TimeUnit.SECONDS);
                Collection<MemberDescription> all = description.members();
                int assigned = all.stream().mapToInt(m -> m.assignment().topicPartitions().size()).sum();
                last = all.size() + " üye, " + assigned + " partition, durum " + description.groupState();
                if (all.size() == members && assigned == 6) {
                    return;
                }
            } catch (Exception e) {
                last = e.getMessage();
            }
            App.sleep(500);
        }
        throw new CheckFailed("Consumer group beklenen duruma gelmedi: " + last + " (beklenen " + members + " üye)");
    }
}
