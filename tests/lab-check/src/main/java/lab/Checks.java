package lab;

import java.util.List;

/** check komutunun giriş noktasi. */
final class Checks {

    private Checks() {
    }

    static int run(String id) {
        Labs.Lab lab = Labs.get(id);
        Report report = new Report(lab.id() + " · " + lab.title());
        List<String> todos = Todos.open(lab.id());
        if (!todos.isEmpty()) {
            report.step("Açık TODO'lar (iş bittiyse yorum satırını silebilirsin)");
            todos.forEach(report::warn);
        }
        try {
            switch (lab.id()) {
                case "D1-01" -> Day1.firstMessage(report);
                case "D1-02" -> Day1.partitionCalc(report);
                case "D1-03" -> Day1.producerMeasure(report);
                case "D1-04" -> Day1.firstSpringApp(report);
                case "D2-01" -> Day2.cluster(report);
                case "D2-02" -> Day2.producerDelivery(report);
                case "D2-03" -> Day2.manualCommit(report);
                case "D2-04" -> Day2.retryDlt(report);
                case "D2-05" -> Day2.lagRebalanceFailover(report);
                case "D2-06" -> Incident.check(report);
                case "D3-01" -> Day3.streams(report);
                case "D3-02" -> Day3.avro(report);
                case "D3-03" -> Day3.workshop(report);
                default -> throw new CheckFailed("Bu lab için kontrol tanımlı değil: " + lab.id());
            }
            return report.success();
        } catch (CheckFailed failure) {
            return report.failure(failure);
        }
    }

    /** Container çalışıyor diye başarılı saymaz: broker gerçekten yanıt vermeli. */
    static void requireCluster(Report report, Kafka kafka, String labId) {
        report.step("Kafka cluster");
        if (!Infra.portOpen(9092) && !Infra.portOpen(9094) && !Infra.portOpen(9096)) {
            throw new CheckFailed("Kafka broker'larına ulaşılamıyor (9092, 9094, 9096).",
                    "Ortamı başlat: ./lab.sh up " + labId);
        }
        int brokers = kafka.brokers().size();
        if (brokers != 3) {
            throw new CheckFailed("Beklenen 3 broker, görünen " + brokers + ".", "./lab.sh status ile hangisinin düştüğüne bak.");
        }
        report.ok("3 broker ayakta");
    }

    static void expect(boolean condition, String failure, String hint) {
        if (!condition) {
            throw new CheckFailed(failure, hint);
        }
    }
}
