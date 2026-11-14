package lab;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.Node;

/** Docker Compose yönetimi ve hazırlık kontrolleri. */
final class Infra {

    private static final String CLUSTER = "infrastructure/compose-cluster.yml";
    private static final String SCHEMA = "infrastructure/compose-schema.yml";
    private static final String SINGLE = "infrastructure/compose-single.yml";
    private static final String BROKEN = "exercises/day2/D2-01-kraft-cluster/compose-broken.yml";

    private Infra() {
    }

    static void requireDocker() {
        Shell.Result result = Shell.capture(Duration.ofSeconds(15), "docker", "info", "--format", "{{.ServerVersion}}");
        if (!result.ok()) {
            throw new CheckFailed("Docker çalışmıyor.", "Docker Desktop'ı başlat, sonra tekrar dene.");
        }
    }

    static int up(Labs.Lab lab, boolean broken, boolean single) {
        requireDocker();
        if (single) {
            composeDown(List.of("-f", CLUSTER, "-f", SCHEMA, "--profile", "postgres"));
            requirePortsFree(List.of(9092));
            return compose(List.of("-f", SINGLE), "up", "-d", "--wait");
        }
        composeDown(List.of("-f", SINGLE));
        List<String> files = new ArrayList<>(List.of("-f", CLUSTER));
        if (lab.schema()) {
            files.addAll(List.of("-f", SCHEMA));
        }
        if (broken) {
            files.addAll(List.of("-f", BROKEN));
        }
        if (lab.postgres()) {
            files.addAll(List.of("--profile", "postgres"));
        }
        requirePortsFree(portsFor(lab));
        int code = compose(files, "up", "-d", "--wait");
        if (code != 0) {
            throw new CheckFailed("Container'lar sağlıklı hale gelmedi.", "docker compose logs ile incele: ./lab.sh status");
        }
        try (Kafka kafka = new Kafka()) {
            awaitCluster(kafka);
            prepare(lab, kafka);
        }
        System.out.println();
        System.out.println("Hazır. Rehber: " + lab.readme());
        return 0;
    }

    /** Opsiyonel demolar: ksqlDB ve Kafka Connect. Ana lab akışı bunlara bağlı değildir. */
    static int upOptional() {
        requireDocker();
        requirePortsFree(List.of(9092, 9094, 9096, Env.schemaRegistryPort(), 8083, 8088));
        return compose(List.of("-f", CLUSTER, "-f", SCHEMA, "--profile", "optional"), "up", "-d", "--wait");
    }

    /** Veriyi silmez: named volume'ler kalır. */
    static int down() {
        requireDocker();
        composeDown(List.of("-f", SINGLE));
        return composeDown(List.of("-f", CLUSTER, "-f", SCHEMA, "--profile", "postgres", "--profile", "optional"));
    }

    static int reset(boolean confirmed) {
        if (!confirmed) {
            throw new CheckFailed("Bu komut tüm lab verisini (topic'ler, PostgreSQL, schema'lar) siler.",
                    "Emin isen: ./lab.sh reset --destructive-reset");
        }
        requireDocker();
        List<String> all = new ArrayList<>(List.of("-f", CLUSTER, "-f", SCHEMA, "--profile", "postgres", "--profile", "optional"));
        compose(List.of("-f", SINGLE), "down", "-v");
        return compose(all, "down", "-v");
    }

    /** Container çalışıyor olması yetmez: broker sayısı ve quorum doğrulanır. */
    static void awaitCluster(Kafka kafka) {
        long deadline = System.currentTimeMillis() + 60_000;
        String last = "";
        while (System.currentTimeMillis() < deadline) {
            try {
                int brokers = kafka.brokers().size();
                QuorumInfo quorum = kafka.admin().describeMetadataQuorum().quorumInfo()
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (brokers == 3 && quorum.leaderId() >= 0) {
                    System.out.println("Cluster hazır: 3 broker, aktif controller " + quorum.leaderId());
                    return;
                }
                last = brokers + " broker görünüyor";
            } catch (Exception e) {
                last = e.getMessage();
            }
            App.sleep(1000);
        }
        throw new CheckFailed("Kafka hazır değil: " + last, "docker logs kafka-1 çıktısına bak.");
    }

    private static void prepare(Labs.Lab lab, Kafka kafka) {
        switch (lab.id()) {
            case "D1-01", "D1-02", "D1-03", "D2-01", "D3-02", "D3-03" -> {
                // Öğrenen topic'i kendisi açar ya da gerek yok.
            }
            case "D3-01" -> {
                ensureOrders(kafka);
                Emit.customers(kafka);
            }
            case "D2-02" -> {
                ensureOrders(kafka);
                // Broker kaydı reddetsin diye: send() başarılı döner, hata sonradan gelir.
                kafka.createTopic("orders-tiny", 6, (short) 3, java.util.Map.of("max.message.bytes", "64"));
            }
            default -> ensureOrders(kafka);
        }
    }

    private static void ensureOrders(Kafka kafka) {
        if (!kafka.topicExists("orders")) {
            kafka.createOrdersLikeTopic("orders");
            System.out.println("orders topic'i oluşturuldu (6 partition, RF 3, min.insync.replicas=2)");
        }
    }

    static int status() {
        requireDocker();
        Shell.Result containers = Shell.capture(Duration.ofSeconds(10), "docker", "ps", "-a", "--filter",
                "label=com.docker.compose.project=kafka-labs", "--format", "table {{.Names}}\t{{.Status}}");
        System.out.println(containers.output().trim());
        Shell.Result single = Shell.capture(Duration.ofSeconds(10), "docker", "ps", "--filter", "name=^kafka-single$",
                "--format", "{{.Names}} {{.Status}}");
        if (!single.output().isBlank()) {
            System.out.println(single.output().trim());
        }
        System.out.println();
        if (!portOpen(9092) && !portOpen(9094) && !portOpen(9096)) {
            System.out.println("Kafka: kapalı. Başlatmak için: ./lab.sh up D1-01");
            return 0;
        }
        try (Kafka kafka = new Kafka()) {
            for (Node node : kafka.brokers()) {
                System.out.println("Broker " + node.id() + ": " + node.host() + ":" + node.port());
            }
            QuorumInfo quorum = kafka.admin().describeMetadataQuorum().quorumInfo()
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            System.out.println("Aktif controller: " + quorum.leaderId());
            System.out.println("Topic'ler: " + kafka.topicNames().stream().filter(t -> !t.startsWith("__")).sorted().toList());
        } catch (CheckFailed e) {
            System.out.println("Kafka: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Kafka: yanıt vermiyor: " + e.getMessage());
        }
        return 0;
    }

    private static List<Integer> portsFor(Labs.Lab lab) {
        List<Integer> ports = new ArrayList<>(List.of(9092, 9094, 9096));
        if (lab.schema()) {
            ports.add(Env.schemaRegistryPort());
        }
        if (lab.postgres()) {
            ports.add(Env.postgresPort());
        }
        return ports;
    }

    /** Port dolu ama sahibi bizim container değilse tam olarak kimin tuttuğunu söyler. */
    private static void requirePortsFree(List<Integer> ports) {
        Set<String> ours = Set.of("kafka-1", "kafka-2", "kafka-3", "kafka-single", "schema-registry", "orders-postgres",
                "ksqldb-server", "connect");
        Map<Integer, String> env = Map.of(Env.schemaRegistryPort(), "SCHEMA_REGISTRY_PORT",
                Env.postgresPort(), "POSTGRES_PORT");
        for (int port : ports) {
            if (!portOpen(port)) {
                continue;
            }
            String owner = Shell.capture(Duration.ofSeconds(10), "docker", "ps", "--filter", "publish=" + port,
                    "--format", "{{.Names}}").output().trim();
            if (ours.contains(owner)) {
                continue;
            }
            String who = owner.isEmpty() ? "Docker dışı bir süreç" : "'" + owner + "' container'ı";
            String hint = env.containsKey(port)
                    ? "Süreci durdur ya da başka port seç: " + env.get(port) + "=<port> ./lab.sh up ..."
                    : "Bu port slaytlarla sabit. " + who + " durdurulmalı.";
            throw new CheckFailed("Port " + port + " dolu: " + who + " kullanıyor.", hint);
        }
    }

    static boolean portOpen(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", port), 300);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static int composeDown(List<String> files) {
        return compose(files, "down");
    }

    private static int compose(List<String> files, String... args) {
        List<String> command = new ArrayList<>(List.of("docker", "compose"));
        command.addAll(files);
        command.addAll(List.of(args));
        return Shell.inherit(command);
    }
}
