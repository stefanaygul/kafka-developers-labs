package lab;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Lab kataloğu: hangi lab hangi altyapıyı ister. */
final class Labs {

    record Lab(String id, String title, String directory, boolean postgres, boolean schema) {
        String readme() {
            return directory + "/README.md";
        }
    }

    private static final Map<String, Lab> ALL = new LinkedHashMap<>();

    static {
        add("D1-01", "İlk mesaj", "exercises/day1/D1-01-ilk-mesaj", false, false);
        add("D1-02", "Partition dağılımı", "exercises/day1/D1-02-partition-dagilimi", false, false);
        add("D1-03", "Producer ölçümü", "exercises/day1/D1-03-producer-olcum", false, false);
        add("D1-04", "İlk Spring uygulaması", "exercises/day1/D1-04-ilk-spring-uygulamasi", false, false);
        add("D2-01", "KRaft cluster", "exercises/day2/D2-01-kraft-cluster", false, false);
        add("D2-02", "Producer teslim sonucu", "exercises/day2/D2-02-producer-teslim", false, false);
        add("D2-03", "Manuel commit ve idempotency", "exercises/day2/D2-03-manuel-commit-idempotency", true, false);
        add("D2-04", "Retry ve DLT", "exercises/day2/D2-04-retry-dlt", false, false);
        add("D2-05", "Lag, rebalance, failover", "exercises/day2/D2-05-lag-rebalance-failover", false, false);
        add("D2-06", "Production provası", "exercises/day2/D2-06-production-provasi", true, false);
        add("D3-01", "Kafka Streams", "exercises/day3/D3-01-kafka-streams", false, false);
        add("D3-02", "Avro ve Schema Registry", "exercises/day3/D3-02-avro-schema-registry", false, true);
        add("D3-03", "Mimari workshop", "exercises/day3/D3-03-mimari-workshop", false, false);
    }

    private Labs() {
    }

    private static void add(String id, String title, String directory, boolean postgres, boolean schema) {
        ALL.put(id, new Lab(id, title, directory, postgres, schema));
    }

    static Lab get(String id) {
        Lab lab = ALL.get(id.toUpperCase(Locale.ROOT));
        if (lab == null) {
            throw new CheckFailed("Bilinmeyen lab: " + id, "Geçerli lab'ler: " + String.join(", ", ALL.keySet()));
        }
        return lab;
    }

    static List<Lab> all() {
        return List.copyOf(ALL.values());
    }
}
