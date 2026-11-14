package lab;

import java.nio.file.Path;
import java.util.UUID;

/** Ortam değerleri. Varsayılanlar slaytlarla aynıdır. */
final class Env {

    static final String BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096";
    static final Path ROOT = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    static final boolean WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

    private Env() {
    }

    static int schemaRegistryPort() {
        return intEnv("SCHEMA_REGISTRY_PORT", 8081);
    }

    static int postgresPort() {
        return intEnv("POSTGRES_PORT", 15432);
    }

    static String schemaRegistryUrl() {
        return "http://localhost:" + schemaRegistryPort();
    }

    static String jdbcUrl() {
        return "jdbc:postgresql://localhost:" + postgresPort() + "/orders";
    }

    /** Her check kendi topic ve group adlarıyla çalışır: tekrarlar birbirini etkilemez. */
    static String unique(String prefix) {
        return prefix + "-check-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static int intEnv(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }
}
