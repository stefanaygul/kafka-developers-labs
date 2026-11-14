package lab;

import static lab.Checks.expect;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** D2-06: olay sayımı ve otomatik tatbikat. */
final class Incident {

    private static final Path BASELINE = Env.ROOT.resolve(".lab-state/incident-baseline");

    private Incident() {
    }

    static int command(String action) throws IOException, SQLException {
        try (Connection db = Day2.database()) {
            switch (action) {
                case "start" -> {
                    long[] now = counts(db);
                    Files.createDirectories(BASELINE.getParent());
                    Files.writeString(BASELINE, now[0] + " " + now[1] + " " + now[2]);
                    System.out.println("Başlangıç kaydedildi: processed=" + now[0] + " order_count=" + now[1]
                            + " redelivery=" + now[2]);
                    return 0;
                }
                case "verify" -> {
                    if (!Files.exists(BASELINE)) {
                        throw new CheckFailed("Başlangıç kaydı yok.", "Önce: ./lab.sh incident start");
                    }
                    String[] base = Files.readString(BASELINE).trim().split(" ");
                    long[] now = counts(db);
                    long processed = now[0] - Long.parseLong(base[0]);
                    long business = now[1] - Long.parseLong(base[1]);
                    long redelivered = now[2] - Long.parseLong(base[2]);
                    System.out.println("İşlenen benzersiz event : " + processed);
                    System.out.println("İş sonucu (order_count) : " + business);
                    System.out.println("Tekrar teslimat         : " + redelivered);
                    System.out.println(processed == business
                            ? "Her event iş sonucuna tam bir kez yansıdı."
                            : "UYUMSUZLUK: iş sonucu ile event sayısı farklı.");
                    System.out.println("Not: Sayıların tutması tek başına 'sıfır kayıp' kanıtı değildir.");
                    System.out.println("     Producer'ın ack aldığı sayıyla da karşılaştır: /orders/stats");
                    return processed == business ? 0 : 1;
                }
                default -> throw new CheckFailed("Bilinmeyen eylem: " + action, "./lab.sh incident start|verify");
            }
        }
    }

    private static long[] counts(Connection db) throws SQLException {
        return new long[] {
            Day2.count(db, "SELECT count(*) FROM processed_events"),
            Day2.count(db, "SELECT coalesce(sum(order_count), 0) FROM customer_stats"),
            Day2.count(db, "SELECT coalesce(sum(redelivery_count), 0) FROM processed_events")};
    }

    /** Rapor dolu mu, ardından aynı olay otomatik tatbik edilir ve sayılar doğrulanır. */
    static void check(Report report) {
        report.step("RAPOR.md");
        Path file = Env.ROOT.resolve(Labs.get("D2-06").directory()).resolve("RAPOR.md");
        String text = Day1.read(file);
        expect(!text.contains("___"), "RAPOR.md tamamlanmamış: boş alanlar (___) duruyor.", "Kendi bulgularını yaz.");
        report.ok("Rapor dolu");

        report.step("Derleme");
        App.build("applications/order-producer", "applications/order-processor");
        report.ok("Uygulamalar derlendi");

        try (Kafka kafka = new Kafka(); Connection db = Day2.database()) {
            Checks.requireCluster(report, kafka, "D2-06");
            drill(report, kafka, db);
        } catch (SQLException e) {
            throw new CheckFailed("Veritabanı hatası: " + e.getMessage());
        }
    }

    private static void drill(Report report, Kafka kafka, Connection db) throws SQLException {
        report.step("Tatbikat: 3 consumer, sürekli üretim");
        String topic = Env.unique("orders");
        String group = Env.unique("group");
        kafka.createOrdersLikeTopic(topic);
        long[] before = counts(db);
        List<App> apps = new ArrayList<>();
        try {
            for (String instance : List.of("A", "B", "C")) {
                apps.add(App.start("order-processor", "--app.topic=" + topic, "--app.group=" + group,
                        "--app.instance=" + instance));
            }
            App producer = App.start("order-producer", "--server.port=0", "--app.topic=" + topic);
            apps.add(producer);
            producer.awaitStarted();
            String url = "http://localhost:" + producer.httpPort() + "/orders";
            apps.get(0).awaitStarted();
            Day1.awaitMembers(kafka, group, 3);
            report.ok("3 instance gruba girdi");

            var load = App.post(url + "/load?perSecond=50&seconds=24");
            expect(load.statusCode() == 202, "Yük başlatılamadı: HTTP " + load.statusCode() + " " + load.body()
                    + ". Producer logu: " + producer.logFile(), null);
            App.sleep(6000);

            report.step("Arıza 1: broker düşüyor");
            Shell.capture(Duration.ofSeconds(40), "docker", "stop", "kafka-2");
            report.ok("kafka-2 durduruldu");
            App.sleep(6000);

            report.step("Arıza 2: consumer commit edemeden ölüyor");
            String crashOrder = "crash-" + java.util.UUID.randomUUID();
            expect(App.post(url + "?orderId=" + crashOrder + "&customerId=customer-incident").statusCode() == 202,
                    "Crash kaydı yazılamadı.", null);
            App crashed = null;
            long crashDeadline = System.currentTimeMillis() + 20_000;
            while (crashed == null && System.currentTimeMillis() < crashDeadline) {
                crashed = apps.subList(0, 3).stream().filter(app -> !app.alive()).findFirst().orElse(null);
                App.sleep(100);
            }
            expect(crashed != null, "Hiçbir instance crash olmadı.", "OrderListener'da crashInjector çağrısı duruyor mu?");
            report.ok("Bir instance DB commit sonrası, offset onayından önce oldu");

            long deadline = System.currentTimeMillis() + 40_000;
            while (System.currentTimeMillis() < deadline
                    && Day1.json(App.get(url + "/stats").body()).get("loadRunning").asBoolean()) {
                App.sleep(500);
            }
            App.sleep(3000);

            report.step("Toparlanma");
            Shell.capture(Duration.ofSeconds(30), "docker", "start", "kafka-2");
            long lagDeadline = System.currentTimeMillis() + 60_000;
            long lag = -1;
            while (System.currentTimeMillis() < lagDeadline) {
                lag = kafka.totalLag(group, topic);
                if (lag == 0) {
                    break;
                }
                App.sleep(500);
            }
            expect(lag == 0, "Lag sıfırlanmadı: " + lag, null);
            report.ok("Lag sıfır");

            JsonNode stats = Day1.json(App.get(url + "/stats").body());
            long acked = stats.get("acked").asLong();
            long failed = stats.get("failed").asLong();
            long[] after = counts(db);
            long processed = after[0] - before[0];
            long business = after[1] - before[1];
            long redelivered = after[2] - before[2];
            report.info("ack=" + acked + " failed=" + failed + " işlenen=" + processed + " iş sonucu=" + business
                    + " tekrar teslimat=" + redelivered);
            expect(acked > 0, "Producer hiç ack almadı.", null);
            expect(processed >= acked, "Ack alan event işlenmedi: ack=" + acked + ", işlenen=" + processed, null);
            expect(redelivered >= 1, "Tekrar teslimat görülmedi: crash kaydı yeniden gelmeliydi.", "TODO D2-03-2.");
            report.ok("Crash kaydı tekrar teslim edildi: " + redelivered + " tekrar");
            expect(processed == business, "İş sonucu ile event sayısı uyumsuz: " + processed + " / " + business,
                    "Idempotency çalışmıyor: D2-03.");
            report.ok("Ack alan her event işlendi; iş sonucu event sayısıyla aynı");
        } finally {
            Shell.capture(Duration.ofSeconds(30), "docker", "start", "kafka-2");
            apps.forEach(App::close);
            kafka.deleteTopics(List.of(topic));
            kafka.deleteGroups(List.of(group));
        }
    }
}
