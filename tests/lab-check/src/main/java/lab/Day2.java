package lab;

import static lab.Checks.expect;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;

/** Gün 2 kontrolleri. */
final class Day2 {

    private Day2() {
    }

    // ---------- D2-01 ----------
    static void cluster(Report report) {
        report.step("TODO D2-01-1: compose-broken.yml");
        Path broken = Env.ROOT.resolve(Labs.get("D2-01").directory()).resolve("compose-broken.yml");
        String text = Day1.read(broken);
        expect(text.contains("EXTERNAL://localhost:9094"),
                "kafka-2'nin EXTERNAL adresi hâlâ yanlış: host'tan çözülemeyen bir ad advertise ediliyor.",
                "TODO D2-01-1: EXTERNAL listener host'tan erişilen adresi tanıtmalı.");
        report.ok("Düzeltme dosyada var");

        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D2-01");
            report.step("Advertised listener'lar");
            Map<Integer, String> expected = Map.of(1, "localhost:9092", 2, "localhost:9094", 3, "localhost:9096");
            for (Node node : kafka.brokers()) {
                String actual = node.host() + ":" + node.port();
                expect(actual.equals(expected.get(node.id())),
                        "Broker " + node.id() + " kendini " + actual + " olarak tanıtıyor, beklenen "
                                + expected.get(node.id()),
                        "Düzelttikten sonra uygula: ./lab.sh up D2-01 --broken");
            }
            report.ok("Üç broker da host'tan erişilebilir adresi tanıtıyor");

            report.step("Docker ağı içinden erişim");
            for (String host : List.of("kafka-1", "kafka-2", "kafka-3")) {
                Shell.Result result = Shell.kafkaCli(Duration.ofSeconds(30), "kafka-broker-api-versions.sh",
                        "--bootstrap-server", host + ":19092");
                expect(result.ok(), "INTERNAL listener yanıt vermiyor: " + host + ":19092", null);
            }
            report.ok("INTERNAL listener'lar çalışıyor");

            report.step("Host'tan her partition'a yazma");
            String topic = Env.unique("d2-01");
            try {
                kafka.createOrdersLikeTopic(topic);
                App.sleep(500);
                TopicDescription description = kafka.describeTopic(topic);
                for (TopicPartitionInfo info : description.partitions()) {
                    kafka.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, info.partition(), null,
                            Kafka.bytes("x")));
                }
                report.ok("6 partition'ın hepsine yazıldı");
            } finally {
                kafka.deleteTopics(List.of(topic));
            }

            report.step("orders topic'i");
            TopicDescription orders = kafka.describeTopic("orders");
            expect(orders != null, "Topic eksik: orders", "README Adım 5.");
            expect(orders.partitions().get(0).replicas().size() == 3,
                    "Yanlış replication factor: " + orders.partitions().get(0).replicas().size(), null);
            expect("2".equals(kafka.topicConfig("orders", "min.insync.replicas")),
                    "min.insync.replicas topic düzeyinde 2 değil.", "--config min.insync.replicas=2 ile ver.");
            report.ok("orders: RF 3, min.insync.replicas=2");
            StringBuilder leaders = new StringBuilder();
            orders.partitions().forEach(p -> leaders.append("P").append(p.partition()).append("->")
                    .append(p.leader() == null ? "yok" : p.leader().id()).append(" "));
            report.info("Leader dağılımı: " + leaders.toString().trim());
        }
    }

    // ---------- D2-02 ----------
    static void producerDelivery(Report report) {
        report.step("Derleme");
        App.build("applications/order-producer");
        report.ok("order-producer derlendi");
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D2-02");
            String topic = Env.unique("orders");
            String missing = Env.unique("olmayan-topic");
            Path failedFile = Env.ROOT.resolve("target/lab-check/" + Env.unique("failed") + ".jsonl");
            kafka.createOrdersLikeTopic(topic);
            String tiny = Env.unique("orders-tiny");
            kafka.createTopic(tiny, 6, (short) 3, Map.of("max.message.bytes", "64"));
            try (App producer = App.start("order-producer", "--server.port=0", "--app.topic=" + topic,
                    "--app.failed-events-file=" + failedFile)) {
                producer.awaitStarted();
                String url = "http://localhost:" + producer.httpPort() + "/orders";

                report.step("TODO D2-02-4: onaylı gönderim");
                HttpResponse<String> confirmed = App.post(url + "/confirmed?orderId=order-ok");
                expect(confirmed.statusCode() == 201, "Beklenen 201, gelen " + confirmed.statusCode() + ": "
                        + confirmed.body(), "TODO D2-02-4: future'ı bekle ve RecordMetadata döndür.");
                JsonNode body = Day1.json(confirmed.body());
                expect(body.has("partition") && body.has("offset"), "Yanıtta partition ve offset yok.", null);
                report.ok("201: partition=" + body.get("partition") + " offset=" + body.get("offset"));

                report.step("TODO D2-02-2: teslim logu");
                String eventId = body.get("eventId").asText();
                Matcher ack = producer.await("ACK eventId=" + eventId + " .*timestamp=(\\d+) ackMs=(\\d+)",
                        Duration.ofSeconds(5));
                expect(ack != null, "ACK satırında timestamp ve ackMs yok.", "TODO D2-02-2.");
                report.ok("timestamp ve ackMs loglanıyor (ackMs=" + ack.group(2) + ")");

                report.step("TODO D2-02-3: asenkron hata (broker reddediyor)");
                HttpResponse<String> queued = App.post(url + "?orderId=order-too-large&topic=" + tiny);
                expect(queued.statusCode() == 202, "Asenkron çağrı 202 dönmeliydi: " + queued.statusCode(), null);
                String tinyEvent = Day1.json(queued.body()).get("eventId").asText();
                Matcher returned = producer.await("SEND_RETURNED eventId=" + tinyEvent + " .*sendReturnedMs=(\\d+)",
                        Duration.ofSeconds(3));
                expect(returned != null, "SEND_RETURNED logu yok.", null);
                report.ok("send() " + returned.group(1) + " ms içinde hatasız döndü, HTTP 202: bu broker onayı değil");
                expect(producer.await("NACK eventId=" + tinyEvent + " .*RecordTooLarge", Duration.ofSeconds(15)) != null,
                        "Broker reddi callback'te görülmedi.", "TODO D1-04-2: hata dalında onFailure çağrılmalı.");
                report.ok("Broker sonradan reddetti: RecordTooLargeException callback'te geldi");
                JsonNode stats = Day1.json(App.get(url + "/stats").body());
                expect(stats.get("failed").asLong() >= 1, "failed sayacı artmadı.", "TODO D2-02-3.");
                expect(Files.exists(failedFile) && Day1.read(failedFile).contains("order-too-large"),
                        "Başarısız event kalıcı olarak kaydedilmedi.", "TODO D2-02-3: failedEvents.record çağır.");
                report.ok("Hata yakalandı: sayaç artti ve event kalıcı dosyaya yazıldı");

                report.step("TODO D2-02-1: senkron hata (olmayan topic)");
                expect(!kafka.topicExists(missing), "Test topic'i beklenmedik şekilde var.", null);
                HttpResponse<String> sync = App.post(url + "?orderId=order-missing&topic=" + missing);
                expect(sync.statusCode() == 202, "send() fırlattı ve yakalanmadı: HTTP " + sync.statusCode(),
                        "TODO D2-02-1: KafkaException'ı yakala ve failedFuture'a çevir.");
                String missingEvent = Day1.json(sync.body()).get("eventId").asText();
                expect(producer.await("NACK eventId=" + missingEvent + " .*(UnknownTopicOrPartition|Timeout)Exception", Duration.ofSeconds(5)) != null,
                        "Olmayan topic için NACK logu gelmedi.", "TODO D2-02-1.");
                Matcher blocked = producer.await("SEND_RETURNED eventId=" + missingEvent + " .*sendReturnedMs=(\\d+)",
                        Duration.ofSeconds(3));
                expect(blocked != null && Long.parseLong(blocked.group(1)) >= 4000,
                        "send() max.block.ms kadar bloklanmalıydı.", null);
                report.ok("send() " + blocked.group(1) + " ms bloklandı (max.block.ms), hata senkron geldi");

                report.step("Hatanın çağırana iletilmesi");
                HttpResponse<String> failed = App.post(url + "/confirmed?orderId=order-fail-2&topic=" + missing);
                expect(failed.statusCode() == 503, "Beklenen 503, gelen " + failed.statusCode(), "TODO D2-02-4.");
                report.ok("Onaysız gönderim 503 döndü: " + Day1.json(failed.body()).get("error").asText());
            } finally {
                kafka.deleteTopics(List.of(topic, tiny));
                try {
                    Files.deleteIfExists(failedFile);
                } catch (java.io.IOException ignored) {
                    // Temizlik.
                }
            }
        }
    }

    // ---------- D2-03 ----------
    static void manualCommit(Report report) {
        report.step("TODO D2-03-1: AckMode");
        String yml = Day1.read(Env.ROOT.resolve(
                "applications/order-processor/src/main/resources/application-idempotency.yml"));
        expect(yml.matches("(?s).*ack-mode:\\s*manual_immediate.*"), "ack-mode manual_immediate değil.", "TODO D2-03-1.");
        report.ok("ack-mode: manual_immediate");

        report.step("Derleme");
        App.build("applications/order-processor");
        report.ok("order-processor derlendi");

        try (Kafka kafka = new Kafka(); Connection db = database()) {
            Checks.requireCluster(report, kafka, "D2-03");
            String topic = Env.unique("orders");
            String group = Env.unique("group");
            String customer = Env.unique("customer");
            String eventId = UUID.randomUUID().toString();
            String orderId = "crash-" + UUID.randomUUID();
            String[] args = {"--app.topic=" + topic, "--app.group=" + group};
            kafka.createOrdersLikeTopic(topic);
            try {
                report.step("Crash: DB commit oldu, offset onaylanmadı");
                RecordMetadata metadata = kafka.sendJson(topic, orderId,
                        Kafka.orderJson(eventId, orderId, customer, 100, Instant.now().toString()));
                try (App first = App.start("order-processor", args)) {
                    Integer exit = first.awaitExit(Duration.ofSeconds(45));
                    expect(exit != null, "Uygulama crash olmadı. Son satırlar:" + first.tail(8),
                            "TODO D2-03-2: crashInjector çağrısına dokunma.");
                    expect(exit == 137, "Beklenen çıkış kodu 137, gelen " + exit + ":" + first.tail(8), null);
                }
                expect(count(db, "SELECT count(*) FROM processed_events WHERE event_id = ?", eventId) == 1,
                        "processed_events içinde bu event yok.", "TODO D2-03-3: aynı transaction'da işaret yaz.");
                report.ok("Süreç öldü (137), DB'de event var");
                long committed = kafka.committed(group, topic).getOrDefault(metadata.partition(), 0L);
                expect(committed <= metadata.offset(), "Offset commit edilmiş: " + committed, "TODO D2-03-2.");
                report.ok("Offset commit edilmedi: commit=" + committed + ", kayıt offset=" + metadata.offset());

                report.step("Yeniden başlatma: aynı event tekrar gelmeli");
                try (App second = App.start("order-processor", args)) {
                    Matcher duplicate = second.await("DUPLICATE .*eventId=" + eventId, Duration.ofSeconds(60));
                    if (duplicate == null) {
                        long stats = count(db, "SELECT order_count FROM customer_stats WHERE customer_id = ?", customer);
                        throw new CheckFailed(stats > 1
                                ? "İş sonucu iki kez yazıldı: order_count=" + stats + " (beklenen 1)."
                                : "Tekrar teslimat görülmedi. Son satırlar:" + second.tail(8),
                                "TODO D2-03-4: 0 satırda markRedelivered çağır, DUPLICATE döndür.");
                    }
                    report.ok("Kayıt tekrar teslim edildi ve DUPLICATE olarak tanındı");

                    long orderCount = count(db, "SELECT order_count FROM customer_stats WHERE customer_id = ?", customer);
                    expect(orderCount == 1, "İş sonucu değişti: order_count=" + orderCount + " (beklenen 1).", "TODO D2-03-4.");
                    long redelivered = count(db, "SELECT redelivery_count FROM processed_events WHERE event_id = ?", eventId);
                    expect(redelivered == 1, "redelivery_count=" + redelivered + " (beklenen 1).", "TODO D2-03-4.");
                    report.ok("order_count=1, redelivery_count=1: tekrar var, zarar yok");

                    report.step("Offset ilerlemesi");
                    long deadline = System.currentTimeMillis() + 15_000;
                    long now = 0;
                    while (System.currentTimeMillis() < deadline) {
                        now = kafka.committed(group, topic).getOrDefault(metadata.partition(), 0L);
                        if (now == metadata.offset() + 1) {
                            break;
                        }
                        App.sleep(300);
                    }
                    expect(now == metadata.offset() + 1, "Offset ilerlemedi: " + now + " (beklenen "
                            + (metadata.offset() + 1) + ").", "TODO D2-03-2: ack.acknowledge() çağır.");
                    report.ok("Başarılı işlemden sonra offset " + now + " oldu");
                }
            } finally {
                kafka.deleteTopics(List.of(topic));
                kafka.deleteGroups(List.of(group));
            }
        } catch (SQLException e) {
            throw new CheckFailed("Veritabanı hatası: " + e.getMessage(), "PostgreSQL ayakta mi? ./lab.sh up D2-03");
        }
    }

    static Connection database() {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(Env.jdbcUrl(), "orders", "orders");
        } catch (ClassNotFoundException | SQLException e) {
            throw new CheckFailed("PostgreSQL'e bağlanılamadı (" + Env.jdbcUrl() + "): " + e.getMessage(),
                    "Ortamı başlat: ./lab.sh up D2-03");
        }
    }

    static long count(Connection db, String sql, String... params) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                statement.setString(i + 1, params[i]);
            }
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getLong(1) : 0;
            }
        }
    }

    // ---------- D2-04 ----------
    static void retryDlt(Report report) {
        report.step("Derleme");
        App.build("applications/order-processor");
        report.ok("order-processor derlendi");
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D2-04");
            String topic = Env.unique("orders");
            String notifications = Env.unique("order-notifications");
            String group = Env.unique("group");
            String dlt = topic + "-dlt";
            kafka.createOrdersLikeTopic(topic);
            try (App app = App.start("order-processor", "--spring.profiles.active=retry", "--app.topic=" + topic,
                    "--app.notification-topic=" + notifications, "--app.retry-group=" + group,
                    "--app.notification-group=" + group + "-n")) {
                app.awaitStarted();
                awaitAssignment(kafka, group);
                scenarioA(report, kafka, app, topic, dlt);
                scenarioB(report, kafka, app, topic, dlt);
                scenarioC(report, kafka, app, notifications);
                dltMustBeConfirmed(report, kafka, app, topic, dlt, group);
            } finally {
                kafka.deleteTopicsWithPrefix(topic);
                kafka.deleteTopicsWithPrefix(notifications);
                kafka.deleteGroups(List.of(group, group + "-n", group + "-dlt-inspector"));
            }
        }
    }

    private static void awaitAssignment(Kafka kafka, String group) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                var description = kafka.admin().describeConsumerGroups(List.of(group)).describedGroups().get(group)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (!description.members().isEmpty()
                        && description.members().stream().anyMatch(m -> !m.assignment().topicPartitions().isEmpty())) {
                    return;
                }
            } catch (Exception ignored) {
                // Group henüz yok.
            }
            App.sleep(300);
        }
        throw new CheckFailed("Consumer group atanmamış: " + group);
    }

    private static void scenarioA(Report report, Kafka kafka, App app, String topic, String dlt) {
        report.step("Senaryo A: geçici hata, blocking retry");
        RecordMetadata transientRecord = kafka.sendJson(topic, "k-transient", Kafka.orderJson("o-1", "fail-transient", 10));
        expect(app.await("PAID attempt=3 partition=" + transientRecord.partition() + " offset="
                        + transientRecord.offset() + " ", Duration.ofSeconds(15)) != null,
                "Geçici hata retry edilmedi: 3. denemede başarı bekleniyordu.",
                "TODO D2-04-2: DefaultErrorHandler'a FixedBackOff(1000L, 3) ver.");
        report.ok("İki hatadan sonra 3. denemede başarılı; DLT'ye gitmedi");

        RecordMetadata failing = kafka.sendJson(topic, "k-block", Kafka.orderJson("o-2", "fail-permanent", 10));
        RecordMetadata behind = kafka.sendJson(topic, "k-block", Kafka.orderJson("o-3", "customer-1", 10));
        expect(failing.partition() == behind.partition(), "Aynı key farklı partition'a gitti.", null);
        Matcher paid = app.await("PAID attempt=1 partition=" + behind.partition() + " offset=" + behind.offset()
                + " at=(\\S+)", Duration.ofSeconds(20));
        expect(paid != null, "Arkadaki kayıt işlenmedi: partition ilerlemiyor.", "TODO D2-04-1: recoverer bağla.");
        long attempts = app.count("ATTEMPT attempt=\\d+ partition=" + failing.partition() + " offset=" + failing.offset() + " ");
        expect(attempts == 4, "Beklenen 4 deneme (1 + 3 retry), görülen " + attempts, "TODO D2-04-2.");
        Instant first = attemptTime(app, failing, 1);
        long blockedMs = Duration.between(first, Instant.parse(paid.group(1))).toMillis();
        expect(blockedMs >= 2500, "Arkadaki kayıt beklemedi: " + blockedMs + " ms", null);
        report.ok("4 deneme yapıldı; arkadaki kayıt " + blockedMs + " ms bekledi: partition blocking retry'da durur");

        ConsumerRecord<byte[], byte[]> record = awaitDlt(kafka, dlt, failing.offset(), failing.partition());
        expect(record != null, "DLT header eksik ya da kayıt DLT'de değil.", "TODO D2-04-1.");
        expect(topic.equals(header(record, "kafka_dlt-original-topic")), "DLT header eksik: original-topic", null);
        expect(header(record, "kafka_dlt-exception-fqcn") != null, "DLT header eksik: exception-fqcn", null);
        report.ok("DLT: kaynak topic, partition ve offset header'ları doğru");

        RecordMetadata invalid = kafka.sendJson(topic, "k-invalid", Kafka.orderJson("o-4", "fail-invalid", 10));
        expect(awaitDlt(kafka, dlt, invalid.offset(), invalid.partition()) != null,
                "Kalıcı hata DLT'ye gitmedi.", "TODO D2-04-2.");
        long invalidAttempts = app.count("ATTEMPT attempt=\\d+ partition=" + invalid.partition() + " offset=" + invalid.offset() + " ");
        expect(invalidAttempts == 1, "Kalıcı hata " + invalidAttempts + " kez denendi (beklenen 1).",
                "TODO D2-04-2: addNotRetryableExceptions ile PaymentValidationException ekle.");
        report.ok("Kalıcı hata retry edilmeden DLT'ye gitti");
    }

    private static Instant attemptTime(App app, RecordMetadata record, int attempt) {
        Matcher matcher = app.await("ATTEMPT attempt=" + attempt + " partition=" + record.partition() + " offset="
                + record.offset() + " .*at=(\\S+)", Duration.ofSeconds(1));
        expect(matcher != null, "Deneme logu bulunamadı.", null);
        return Instant.parse(matcher.group(1));
    }

    private static void scenarioB(Report report, Kafka kafka, App app, String topic, String dlt) {
        report.step("Senaryo B: deserialization hatası");
        byte[] poison = Kafka.bytes("{\"bozuk\": json değil");
        RecordMetadata bad = kafka.send(topic, "k-poison", poison);
        RecordMetadata good = kafka.sendJson(topic, "k-poison", Kafka.orderJson("o-5", "customer-1", 10));
        boolean flowed = app.await("PAID attempt=1 partition=" + good.partition() + " offset=" + good.offset() + " ",
                Duration.ofSeconds(15)) != null;
        expect(flowed, "Bozuk kayıt partition'ı kilitledi: arkadaki kayıt işlenmedi.",
                "TODO D2-04-3: value-deserializer'ı ErrorHandlingDeserializer ile sarmala.");
        report.ok("Bozuk kaydın arkasındaki kayıt işlendi");
        long listenerAttempts = app.count("ATTEMPT attempt=\\d+ partition=" + bad.partition() + " offset=" + bad.offset() + " ");
        expect(listenerAttempts == 0, "Bozuk kayıt listener'a ulaşmamalıydı.", null);
        report.ok("Listener bozuk kaydı hiç görmedi: bu iş hatası değil, deserialization hatası");
        ConsumerRecord<byte[], byte[]> record = awaitDlt(kafka, dlt, bad.offset(), bad.partition());
        expect(record != null, "Bozuk kayıt DLT'ye gitmedi.", "dltTemplate ve recoverer'ı kontrol et.");
        expect(java.util.Arrays.equals(record.value(), poison), "DLT'deki bayt orijinal bayt değil.", null);
        String fqcn = header(record, "kafka_dlt-exception-cause-fqcn") + " " + header(record, "kafka_dlt-exception-fqcn");
        expect(fqcn.contains("Deserialization"), "DLT header'ı deserialization hatası göstermiyor: " + fqcn, null);
        report.ok("Orijinal bayt DLT'de korundu, hata sınıfı: DeserializationException");
    }

    private static void scenarioC(Report report, Kafka kafka, App app, String notifications) {
        report.step("Senaryo C: non-blocking retry");
        kafka.sendJson(notifications, "k-notify", Kafka.orderJson("n-1", "fail-notify", 10));
        App.sleep(200);
        kafka.sendJson(notifications, "k-notify", Kafka.orderJson("n-2", "customer-1", 10));
        Matcher ok = app.await("NOTIFIED topic=" + Pattern.quote(notifications) + " orderId=n-2", Duration.ofSeconds(10));
        expect(ok != null, "Arkadaki bildirim işlenmedi.", "TODO D2-04-4: @RetryableTopic ekle.");
        Matcher toDlt = app.await("NOTIFY_DLT topic=" + Pattern.quote(notifications) + "-dlt orderId=n-1", Duration.ofSeconds(20));
        expect(toDlt != null, "Bildirim retry topic'lerinden geçip DLT'ye ulaşmadı.",
                "TODO D2-04-4: @RetryableTopic ve index son eki.");
        for (String suffix : List.of("-retry-0", "-retry-1", "-retry-2", "-dlt")) {
            expect(kafka.topicExists(notifications + suffix), "Topic eksik: " + notifications + suffix,
                    "topicSuffixingStrategy index değerine ayarlı mi?");
        }
        report.ok("retry-0, retry-1, retry-2 ve dlt topic'leri oluştu");

        List<Instant> times = new ArrayList<>();
        for (String line : app.matching("NOTIFY_ATTEMPT .*orderId=n-1 ")) {
            Matcher matcher = Pattern.compile("at=(\\S+)").matcher(line);
            if (matcher.find()) {
                times.add(Instant.parse(matcher.group(1)));
            }
        }
        expect(times.size() == 4, "Beklenen 4 deneme, görülen " + times.size(), null);
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < times.size(); i++) {
            gaps.add(Duration.between(times.get(i - 1), times.get(i)).toMillis());
        }
        expect(gaps.get(0) >= 900 && gaps.get(1) >= 1800 && gaps.get(2) >= 3600, "Backoff beklenen gibi değil: " + gaps, null);
        report.ok("Denemeler arası süre (ms): " + gaps);

        int notified = indexOf(app, "NOTIFIED .*orderId=n-2");
        int dead = indexOf(app, "NOTIFY_DLT .*orderId=n-1");
        expect(notified < dead, "Sıra değişmedi: n-2, n-1'in sonucundan önce işlenmeliydi.", null);
        report.ok("Aynı key'de sonra gelen kayıt önce bitti: non-blocking retry sırayı bozar");
    }

    private static int indexOf(App app, String regex) {
        List<String> all = app.matching(".");
        Pattern pattern = Pattern.compile(regex);
        for (int i = 0; i < all.size(); i++) {
            if (pattern.matcher(all.get(i)).find()) {
                return i;
            }
        }
        return Integer.MAX_VALUE;
    }

    /** DLT'ye yazılamazsa kayıt kurtarılmış sayılmamalı: offset ilerlememeli. */
    private static void dltMustBeConfirmed(Report report, Kafka kafka, App app, String topic, String dlt, String group) {
        report.step("DLT yazma doğrulaması");
        kafka.deleteTopics(List.of(dlt));
        long gone = System.currentTimeMillis() + 15_000;
        while (kafka.topicExists(dlt) && System.currentTimeMillis() < gone) {
            App.sleep(200);
        }
        RecordMetadata record = kafka.sendJson(topic, "k-no-dlt", Kafka.orderJson("o-6", "fail-invalid", 10));
        App.sleep(12_000);
        long committed = kafka.committed(group, topic).getOrDefault(record.partition(), 0L);
        expect(committed <= record.offset(),
                "DLT yokken offset ilerledi: kayıt kaybedildi (commit=" + committed + ").",
                "TODO D2-04-1: setFailIfSendResultIsError(true) ve setWaitForSendResultTimeout.");
        report.ok("DLT yokken offset ilerlemedi: commit=" + committed + ", kayıt offset=" + record.offset());

        kafka.createOrdersLikeTopic(dlt);
        expect(awaitDlt(kafka, dlt, record.offset(), record.partition(), 40) != null,
                "DLT geri gelince kayıt yazılmadı.", null);
        long deadline = System.currentTimeMillis() + 15_000;
        long advanced = committed;
        while (System.currentTimeMillis() < deadline && advanced <= record.offset()) {
            advanced = kafka.committed(group, topic).getOrDefault(record.partition(), 0L);
            App.sleep(300);
        }
        expect(advanced > record.offset(), "DLT yazıldıktan sonra offset ilerlemedi.", null);
        report.ok("DLT geri gelince kayıt yazıldı ve offset " + advanced + " oldu");
    }

    private static ConsumerRecord<byte[], byte[]> awaitDlt(Kafka kafka, String dlt, long offset, int partition) {
        return awaitDlt(kafka, dlt, offset, partition, 15);
    }

    private static ConsumerRecord<byte[], byte[]> awaitDlt(Kafka kafka, String dlt, long offset, int partition, int seconds) {
        long deadline = System.currentTimeMillis() + seconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (kafka.topicExists(dlt)) {
                for (ConsumerRecord<byte[], byte[]> record : kafka.readAll(dlt, 500, Duration.ofSeconds(3))) {
                    Long originalOffset = number(record, "kafka_dlt-original-offset");
                    Long originalPartition = number(record, "kafka_dlt-original-partition");
                    if (originalOffset != null && originalOffset == offset
                            && originalPartition != null && originalPartition == partition) {
                        return record;
                    }
                }
            }
            App.sleep(500);
        }
        return null;
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : Kafka.text(header.value());
    }

    private static Long number(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(header.value());
        return header.value().length == Integer.BYTES ? (long) buffer.getInt() : buffer.getLong();
    }

    // ---------- D2-05 ----------
    static void lagRebalanceFailover(Report report) {
        report.step("Derleme");
        App.build("applications/order-consumer");
        report.ok("order-consumer derlendi");
        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D2-05");
            String topic = Env.unique("orders");
            String group = Env.unique("group");
            kafka.createOrdersLikeTopic(topic);
            try {
                for (int i = 0; i < 300; i++) {
                    kafka.sendJson(topic, "order-" + i, Kafka.orderJson("order-" + i, "customer-1", 10));
                }
                String[] common = {"--app.topic=" + topic, "--spring.kafka.consumer.group-id=" + group,
                    "--spring.kafka.consumer.max-poll-records=10", "--app.slow-ms=100"};
                try (App first = App.start("order-consumer", with(common, "--app.instance=A"))) {
                    report.step("TODO D2-05-1: yavaş consumer ve lag");
                    first.awaitStarted();
                    expect(first.await("CONSUMED", Duration.ofSeconds(20)) != null, "Hiç kayıt tüketilmedi.",
                            "Önce D1-04'u tamamla: ./lab.sh check D1-04");
                    App.sleep(4000);
                    long sampleOne = kafka.totalLag(group, topic);
                    expect(sampleOne > 150, "Lag oluşmadı (" + sampleOne + "): consumer yavaşlamıyor.",
                            "TODO D2-05-1: slowMs kadar bekle.");
                    App.sleep(3000);
                    long sampleTwo = kafka.totalLag(group, topic);
                    expect(sampleTwo < sampleOne, "Lag azalmıyor: " + sampleOne + " -> " + sampleTwo, null);
                    report.ok("Ölçülen lag: " + sampleOne + " -> " + sampleTwo + " (3 sn arayla); eğim saniyede yaklaşık "
                            + (sampleOne - sampleTwo) / 3 + " kayıt");

                    report.step("Rebalance ölçümü");
                    try (App second = App.start("order-consumer", with(common, "--app.instance=B"))) {
                        expect(second.await("REBALANCE instance=B event=ASSIGNED", Duration.ofSeconds(45)) != null,
                                "İkinci instance gruba giremedi.", null);
                        Day1.awaitMembers(kafka, group, 2);
                        first.matching("event=ASSIGNED .*pause=\\d+ms").stream().reduce((a, b) -> b).ifPresentOrElse(
                                line -> report.ok("Ölçülen duraklama: " + line.substring(line.indexOf("pause="))),
                                () -> report.ok("İkinci instance gruba girdi"));
                    }
                }
                report.step("Protokol karşılaştırması (KIP-848, Kafka 4.x client)");
                Rebalance.printComparison(report);

                failover(report, kafka, topic);
            } finally {
                kafka.deleteTopics(List.of(topic));
                kafka.deleteGroups(List.of(group));
            }
        }
    }

    private static String[] with(String[] base, String extra) {
        List<String> all = new ArrayList<>(List.of(base));
        all.add(extra);
        return all.toArray(String[]::new);
    }

    private static void failover(Report report, Kafka kafka, String topic) {
        report.step("Failover: kafka-2 durduruluyor");
        long start = System.currentTimeMillis();
        Shell.capture(Duration.ofSeconds(40), "docker", "stop", "kafka-2");
        try {
            long deadline = System.currentTimeMillis() + 30_000;
            boolean moved = false;
            while (System.currentTimeMillis() < deadline && !moved) {
                moved = kafka.describeTopic(topic).partitions().stream().allMatch(p ->
                        p.leader() != null && p.leader().id() != 2 && p.isr().stream().noneMatch(n -> n.id() == 2));
                App.sleep(250);
            }
            expect(moved, "Leader ve ISR değişmedi.", null);
            report.ok("Yeni leader'lar seçildi, ISR kısaldı (" + (System.currentTimeMillis() - start) + " ms, stop dahil)");
            RecordMetadata metadata = kafka.sendJson(topic, "after-failover", Kafka.orderJson("o-failover", "customer-1", 10));
            report.ok("acks=all yazma devam etti: partition=" + metadata.partition() + " offset=" + metadata.offset());
        } finally {
            Shell.capture(Duration.ofSeconds(30), "docker", "start", "kafka-2");
        }
        report.step("Failover: kafka-2 geri geliyor");
        long deadline = System.currentTimeMillis() + 60_000;
        boolean restored = false;
        while (System.currentTimeMillis() < deadline && !restored) {
            try {
                restored = kafka.describeTopic(topic).partitions().stream().allMatch(p -> p.isr().size() == 3);
            } catch (CheckFailed ignored) {
                restored = false;
            }
            App.sleep(500);
        }
        expect(restored, "ISR 60 sn içinde 3'e dönmedi.", "docker logs kafka-2 çıktısına bak.");
        report.ok("ISR yeniden 3 broker");
    }
}
