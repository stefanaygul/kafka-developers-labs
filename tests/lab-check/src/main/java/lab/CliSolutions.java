package lab;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Komutla yapılan lab'lerin çözümü: öğrenenin yazacağı komutların aynısı. */
final class CliSolutions {

    private record Step(String description, String input, String... command) {
    }

    private static final String BOOTSTRAP = "kafka-1:19092";

    private static final Map<String, List<Step>> STEPS = Map.of(
            "D1-01", List.of(
                    new Step("orders topic'ini aç", null, "kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--create",
                            "--if-not-exists", "--topic", "orders", "--partitions", "6", "--replication-factor", "3",
                            "--config", "min.insync.replicas=2"),
                    new Step("Key'li kayıt üret", """
                            order-1:{"orderId":"order-1","status":"CREATED"}
                            order-1:{"orderId":"order-1","status":"PAID"}
                            order-1:{"orderId":"order-1","status":"SHIPPED"}
                            order-2:{"orderId":"order-2","status":"CREATED"}
                            order-3:{"orderId":"order-3","status":"CREATED"}
                            """, "kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP, "--topic", "orders",
                            "--reader-property", "parse.key=true", "--reader-property", "key.separator=:"),
                    new Step("Key'siz kayıt üret", """
                            {"orderId":"order-4","status":"CREATED"}
                            """, "kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP, "--topic", "orders"),
                    new Step("Group ile tüket", null, "kafka-console-consumer.sh", "--bootstrap-server", BOOTSTRAP,
                            "--topic", "orders", "--group", "d1-01-grup", "--from-beginning", "--max-messages", "6",
                            "--timeout-ms", "15000")),
            "D1-03", List.of(
                    new Step("Ölçüm topic'ini aç", null, "kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--create",
                            "--if-not-exists", "--topic", "orders-perf", "--partitions", "6", "--replication-factor", "3",
                            "--config", "min.insync.replicas=2")),
            "D2-01", List.of(
                    new Step("orders topic'ini aç", null, "kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--create",
                            "--if-not-exists", "--topic", "orders", "--partitions", "6", "--replication-factor", "3",
                            "--config", "min.insync.replicas=2")));

    private CliSolutions() {
    }

    static boolean has(String lab) {
        return STEPS.containsKey(lab);
    }

    static void show(String lab) {
        System.out.println("Bu lab'in komut çözümü (container içinde, /opt/kafka/bin altında):");
        for (Step step : STEPS.get(lab)) {
            System.out.println();
            System.out.println("# " + step.description());
            System.out.println(String.join(" ", step.command()));
            if (step.input() != null) {
                step.input().lines().forEach(line -> System.out.println("> " + line));
            }
        }
    }

    static void apply(String lab) {
        for (Step step : STEPS.get(lab)) {
            List<String> command = new ArrayList<>(List.of("docker", "exec"));
            if (step.input() != null) {
                command.add("-i");
            }
            command.add("kafka-1");
            command.add("/opt/kafka/bin/" + step.command()[0]);
            command.addAll(List.of(step.command()).subList(1, step.command().length));
            Shell.Result result = Shell.captureWithInput(Duration.ofSeconds(60), command, step.input());
            if (!result.ok()) {
                throw new CheckFailed(step.description() + " başarısız: " + result.output());
            }
            System.out.println("Çalıştırıldı: " + step.description());
        }
    }
}
