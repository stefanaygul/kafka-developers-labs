package lab;

import static lab.Checks.expect;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Gün 3 kontrolleri. */
final class Day3 {

    private static final String V1 = "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.shop\",\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"}]}";
    private static final String BREAKING = "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.shop\",\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"TRY\"},"
            + "{\"name\":\"warehouseId\",\"type\":\"string\"}]}";

    private Day3() {
    }

    // ---------- D3-01 ----------
    static void streams(Report report) {
        report.step("Topology testleri (TopologyTestDriver)");
        Shell.Result tests = Shell.capture(Duration.ofMinutes(4), Shell.mvnw("-q", "-Psolution-tests",
                "-pl", "applications/order-streams", "test"));
        if (!tests.ok()) {
            String output = tests.output();
            expect(!output.contains("COMPILATION ERROR"), "Derleme başarısız:" + errors(output), "Hatanın gösterdiği satırı düzelt.");
            expect(!output.contains("onlyHighValueOrdersReachOutputTopic"), "Bölüm A çalışmıyor.", "TODO D3-01-1.");
            expect(!output.contains("ordersAreEnrichedWithCustomerAfterRepartition"), "Bölüm B çalışmıyor.",
                    "TODO D3-01-2 ve 3: selectKey, repartition ve join.");
            expect(!output.contains("graceAcceptsLateEventAndDropsTooLateEvent"), "Bölüm C çalışmıyor.", "TODO D3-01-4.");
            throw new CheckFailed("Topology testleri başarısız:" + errors(output));
        }
        report.ok("Filtre, join ve window testleri geçti");

        report.step("Derleme");
        App.build("applications/order-streams");

        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D3-01");
            String orders = Env.unique("orders");
            String customers = Env.unique("customers");
            String high = Env.unique("orders-high");
            String enriched = Env.unique("orders-enriched");
            String counts = Env.unique("order-counts");
            String appId = Env.unique("order-streams");
            String lateCustomer = Env.unique("customer-late");
            kafka.createOrdersLikeTopic(orders);
            Emit.customers(kafka, customers);
            try (App app = App.start("order-streams", "--spring.kafka.streams.application-id=" + appId,
                    "--app.orders-topic=" + orders, "--app.customers-topic=" + customers,
                    "--app.high-value-topic=" + high, "--app.enriched-topic=" + enriched,
                    "--app.counts-topic=" + counts)) {
                report.step("Gerçek cluster: uygulama");
                expect(app.await("State transition from REBALANCING to RUNNING", Duration.ofSeconds(60)) != null,
                        "Streams uygulaması RUNNING durumuna gelmedi:" + app.tail(10), null);
                report.ok("Streams uygulaması RUNNING");
                expect(app.await("Sub-topology: 1", Duration.ofSeconds(5)) != null,
                        "describe() çıktısında ikinci sub-topology yok: repartition oluşmamış.", "TODO D3-01-2.");
                report.ok("describe() iki sub-topology gösteriyor: araya repartition girdi");

                report.step("Bölüm A ve B: gerçek topic'ler");
                String now = Instant.now().toString();
                kafka.sendJson(orders, "o-low", Kafka.orderJson("e-low", "o-low", "customer-1", 500, now));
                kafka.sendJson(orders, "o-high", Kafka.orderJson("e-high", "o-high", "customer-1", 15000, now));
                List<ConsumerRecord<byte[], byte[]>> highRecords = await(kafka, high, 1, 25);
                expect(highRecords.size() == 1 && "o-high".equals(Kafka.text(highRecords.get(0).key())),
                        "orders-high topic'inde beklenen tek kayıt yok: " + highRecords.size(), "TODO D3-01-1.");
                report.ok("Yalnızca eşik üzerindeki sipariş orders-high'a düştü");
                List<ConsumerRecord<byte[], byte[]>> enrichedRecords = await(kafka, enriched, 2, 25);
                expect(enrichedRecords.size() == 2, "Zenginleştirilmiş kayıt sayısı " + enrichedRecords.size() + " (beklenen 2)", "TODO D3-01-3.");
                expect("customer-1".equals(Kafka.text(enrichedRecords.get(0).key())), "Key customerId değil.", "TODO D3-01-2.");
                expect(Kafka.text(enrichedRecords.get(0).value()).contains("\"city\""), "Müşteri bilgisi eklenmemiş.", "TODO D3-01-3.");
                report.ok("Siparişler müşteri bilgisiyle zenginleşti, key customerId oldu");

                report.step("İç topic'ler");
                for (String internal : List.of(appId + "-orders-by-customer-repartition",
                        appId + "-order-counts-store-changelog")) {
                    expect(kafka.topicExists(internal), "İç topic eksik: " + internal, null);
                }
                report.ok("repartition ve changelog topic'lerini Streams kendisi oluşturdu");

                report.step("Bölüm C: window ve geç event (slayt 69)");
                // Broker bir saatten ileri zaman damgasını reddeder (log.message.timestamp.after.max.ms).
                Instant anchor = Instant.ofEpochSecond(Instant.now().getEpochSecond() / 300 * 300);
                Emit.sendLateEvents(kafka, orders, lateCustomer, anchor, false);
                String firstWindow = lateCustomer + "@" + anchor;
                String secondWindow = lateCustomer + "@" + anchor.plus(5, ChronoUnit.MINUTES);
                expect(app.await("Skipping record for expired window", Duration.ofSeconds(20)) != null,
                        "Grace dışındaki E düşürülmedi.", "TODO D3-01-4: TimeWindows.ofSizeAndGrace.");
                report.ok("E (grace dışı) düşürüldü: 'Skipping record for expired window'");
                Map<String, Long> latest = new HashMap<>();
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> record : kafka.readAll(counts, 500, Duration.ofSeconds(3))) {
                        latest.put(Kafka.text(record.key()), ByteBuffer.wrap(record.value()).getLong());
                    }
                    if (Long.valueOf(2).equals(latest.get(firstWindow)) && Long.valueOf(2).equals(latest.get(secondWindow))) {
                        break;
                    }
                    App.sleep(500);
                }
                expect(Long.valueOf(2).equals(latest.get(firstWindow)),
                        "İlk window beklenen 2 değil: " + latest.get(firstWindow), "A ve grace içindeki B sayılmalı.");
                expect(Long.valueOf(2).equals(latest.get(secondWindow)),
                        "İkinci window beklenen 2 değil: " + latest.get(secondWindow), "C ve D sayılmalı.");
                report.ok("İlk window=2 (A, B), ikinci window=2 (C, D): B grace içinde kabul edildi");
            } finally {
                kafka.deleteTopics(List.of(orders, customers, high, enriched, counts));
                kafka.deleteTopicsWithPrefix(appId);
                kafka.deleteGroups(List.of(appId));
            }
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> await(Kafka kafka, String topic, int expected, int seconds) {
        long deadline = System.currentTimeMillis() + seconds * 1000L;
        List<ConsumerRecord<byte[], byte[]>> records = List.of();
        while (System.currentTimeMillis() < deadline) {
            records = kafka.readAll(topic, 100, Duration.ofSeconds(3));
            if (records.size() >= expected) {
                return records;
            }
            App.sleep(500);
        }
        return records;
    }

    private static String errors(String output) {
        return output.lines().filter(line -> line.contains("ERROR")).limit(6)
                .reduce("", (a, b) -> a + "\n          " + (b.length() > 160 ? b.substring(0, 160) : b));
    }

    // ---------- D3-02 ----------
    static void avro(Report report) {
        report.step("Schema Registry");
        String registry = Env.schemaRegistryUrl();
        expect(Infra.portOpen(Env.schemaRegistryPort()), "Schema Registry kapalı: " + registry, "Ortamı başlat: ./lab.sh up D3-02");
        HttpResponse<String> health = App.get(registry + "/subjects");
        expect(health.statusCode() == 200, "Bu port Schema Registry değil: " + registry,
                "Port başka bir uygulamada olabilir: SCHEMA_REGISTRY_PORT ile değiştir.");
        report.ok("Schema Registry yanıt veriyor: " + registry);

        report.step("Derleme");
        App.build("applications/order-avro");
        report.ok("order-avro derlendi: Avro sınıfı üretildi");

        try (Kafka kafka = new Kafka()) {
            Checks.requireCluster(report, kafka, "D3-02");
            String topic = Env.unique("orders-avro");
            String group = Env.unique("group");
            String subject = topic + "-value";
            try {
                report.step("v1 ve compatibility modu");
                JsonNode v1 = Day1.json(schemaPost(registry + "/subjects/" + subject + "/versions", V1).body());
                expect(v1.has("id"), "v1 kaydedilemedi: " + v1, null);
                HttpResponse<String> mode = request("PUT", registry + "/config/" + subject, "{\"compatibility\":\"BACKWARD\"}");
                expect(mode.statusCode() == 200, "Compatibility modu ayarlanamadı: " + mode.body(), null);
                String configured = Day1.json(App.get(registry + "/config/" + subject).body()).get("compatibilityLevel").asText();
                expect("BACKWARD".equals(configured), "Subject modu BACKWARD değil: " + configured, null);
                report.ok("Subject " + subject + ": v1 id=" + v1.get("id") + ", mod BACKWARD");

                try (App app = App.start("order-avro", "--server.port=0", "--app.topic=" + topic, "--app.group=" + group)) {
                    app.awaitStarted();
                    int port = app.httpPort();

                    report.step("TODO D3-02-1 ve 2: Avro ile yazma");
                    HttpResponse<String> created = App.post("http://localhost:" + port + "/avro/orders?orderId=order-avro-1");
                    expect(created.statusCode() == 201, "Avro yazma başarısız (" + created.statusCode() + "): " + created.body(),
                            "TODO D3-02-1: serializer ayarları. TODO D3-02-2: Order.newBuilder ve send.");
                    report.ok("Kayıt Avro olarak yazıldı");
                    expect(app.await("AVRO_CONSUMED class=com.shop.Order .*orderId=order-avro-1", Duration.ofSeconds(20)) != null,
                            "Kayıt Order nesnesine çözülmedi:" + app.tail(6),
                            "TODO D3-02-1: specific.avro.reader true olmalı.");
                    report.ok("Consumer kaydı com.shop.Order olarak okudu: specific.avro.reader çalışıyor");
                }

                report.step("TODO D3-02-3: uyumlu değişiklik");
                JsonNode versions = Day1.json(App.get(registry + "/subjects/" + subject + "/versions").body());
                expect(versions.size() == 2, "Beklenen 2 versiyon, görünen " + versions,
                        "TODO D3-02-3: order.avsc'ye default'lu currency alanı ekle.");
                JsonNode latest = Day1.json(App.get(registry + "/subjects/" + subject + "/versions/latest").body());
                expect(latest.get("schema").asText().contains("currency"), "v2'de currency alanı yok.", "TODO D3-02-3.");
                expect(latest.get("schema").asText().contains("\"default\":\"TRY\""), "currency default'u TRY değil.", "TODO D3-02-3.");
                report.ok("v2 kaydedildi: id=" + latest.get("id") + ", currency default TRY");

                report.step("Mesaj biçimi");
                List<ConsumerRecord<byte[], byte[]>> records = kafka.readAll(topic, 5, Duration.ofSeconds(8));
                expect(!records.isEmpty(), "Topic'te kayıt yok.", null);
                ByteBuffer value = ByteBuffer.wrap(records.get(0).value());
                byte magic = value.get();
                int schemaId = value.getInt();
                expect(magic == 0, "Magic byte 0 değil: " + magic, null);
                expect(schemaId == latest.get("id").asInt(), "Mesajdaki schema id " + schemaId + ", beklenen " + latest.get("id"), null);
                expect(App.get(registry + "/schemas/ids/" + schemaId).statusCode() == 200, "Schema id bulunamadı: " + schemaId, null);
                report.ok("İlk 5 bayt: magic=0, schema id=" + schemaId + " (GET /schemas/ids ile doğrulandı)");

                report.step("Kırıcı değişiklik");
                HttpResponse<String> test = schemaPost(registry + "/compatibility/subjects/" + subject + "/versions/latest?verbose=true", BREAKING);
                expect(!Day1.json(test.body()).get("is_compatible").asBoolean(), "Kırıcı schema uyumlu görüldü: " + test.body(), null);
                HttpResponse<String> rejected = schemaPost(registry + "/subjects/" + subject + "/versions", BREAKING);
                expect(rejected.statusCode() == 409, "Schema evrimi beklenmedik şekilde kabul edildi: HTTP "
                        + rejected.statusCode() + " " + rejected.body(), null);
                report.ok("Default'suz zorunlu alan reddedildi: HTTP 409");
                String message = Day1.json(rejected.body()).get("message").asText();
                report.info(message.length() > 180 ? message.substring(0, 180) + "..." : message);
                int after = Day1.json(App.get(registry + "/subjects/" + subject + "/versions").body()).size();
                expect(after == 2, "Reddedilen schema versiyon oluşturdu: " + after, null);
                report.ok("Versiyon sayısı değişmedi");
            } finally {
                request("DELETE", registry + "/subjects/" + subject, null);
                request("DELETE", registry + "/subjects/" + subject + "?permanent=true", null);
                kafka.deleteTopics(List.of(topic));
                kafka.deleteGroups(List.of(group));
            }
        }
    }

    private static HttpResponse<String> schemaPost(String url, String schema) {
        String body = "{\"schema\":" + quote(schema) + "}";
        return request("POST", url, body);
    }

    private static HttpResponse<String> request(String method, String url, String body) {
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
        return App.http(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .method(method, publisher).build());
    }

    private static String quote(String text) {
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    // ---------- D3-03 ----------
    static void workshop(Report report) {
        report.step("SABLON.md");
        Path file = Env.ROOT.resolve(Labs.get("D3-03").directory()).resolve("SABLON.md");
        String text = Day1.read(file);
        expect(!text.contains("___"), "SABLON.md tamamlanmamış: boş alanlar (___) duruyor.",
                "Her kararı bir cümleyle gerekçelendir.");
        report.ok("Şablon dolu. Değerlendirme eğitmende: ölçüt gerekçedir.");
    }
}
