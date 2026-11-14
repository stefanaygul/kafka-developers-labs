package lab;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Dış komut çalıştırma. */
final class Shell {

    record Result(int exitCode, String output) {
        boolean ok() {
            return exitCode == 0;
        }
    }

    private Shell() {
    }

    static Result capture(Duration timeout, String... command) {
        return capture(timeout, List.of(command));
    }

    static Result capture(Duration timeout, List<String> command) {
        return captureWithInput(timeout, command, null);
    }

    static Result captureWithInput(Duration timeout, List<String> command, String input) {
        try {
            Process process = new ProcessBuilder(command).directory(Env.ROOT.toFile()).redirectErrorStream(true).start();
            try (var stdin = process.getOutputStream()) {
                if (input != null) {
                    stdin.write(input.getBytes(StandardCharsets.UTF_8));
                }
            }
            StringBuilder output = new StringBuilder();
            Thread reader = new Thread(() -> {
                try {
                    output.append(new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
                } catch (IOException ignored) {
                    // Süreç kapandı.
                }
            });
            reader.start();
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                reader.join(2000);
                return new Result(124, output + "\n(zaman aşımı: " + timeout.toSeconds() + " sn)");
            }
            reader.join(5000);
            return new Result(process.exitValue(), output.toString());
        } catch (IOException e) {
            return new Result(127, "Komut çalıştırılamadı: " + command.get(0) + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(130, "Kesildi");
        }
    }

    /** Çıktıyı doğrudan kullanıcıya gösterir. */
    static int inherit(List<String> command) {
        try {
            return new ProcessBuilder(command).directory(Env.ROOT.toFile()).inheritIO().start().waitFor();
        } catch (IOException e) {
            throw new CheckFailed("Komut çalıştırılamadı: " + command.get(0) + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 130;
        }
    }

    static List<String> mvnw(String... args) {
        List<String> command = new ArrayList<>();
        command.add(Env.WINDOWS ? Env.ROOT.resolve("mvnw.cmd").toString() : Env.ROOT.resolve("mvnw").toString());
        command.addAll(List.of(args));
        return command;
    }

    /** Container içinde Kafka komutu. */
    static Result kafkaCli(Duration timeout, String... args) {
        List<String> command = new ArrayList<>(List.of("docker", "exec", "kafka-1"));
        command.add("/opt/kafka/bin/" + args[0]);
        command.addAll(List.of(args).subList(1, args.length));
        return capture(timeout, command);
    }
}
