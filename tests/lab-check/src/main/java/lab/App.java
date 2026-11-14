package lab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Öğrenenin kodunu derler, ayrı bir süreç olarak başlatır ve logunu izler. */
final class App implements AutoCloseable {

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** Sarmalanmamis deserializer saniyede on binlerce satir log uretir. Bellek ve disk korunur. */
    private static final int MAX_LINES = 100_000;

    private final String name;
    private final Process process;
    private final Path logFile;
    private final List<String> lines = Collections.synchronizedList(new ArrayList<>());

    private App(String name, Process process, Path logFile) {
        this.name = name;
        this.process = process;
        this.logFile = logFile;
        Thread reader = new Thread(this::pump, name + "-log");
        reader.setDaemon(true);
        reader.start();
    }

    /** Derleme hatası varsa son satırları gösterir: öğrenen hatayı burada görür. */
    static void build(String... modules) {
        List<String> args = new ArrayList<>(List.of("-q", "-DskipTests", "-pl"));
        args.add(String.join(",", modules));
        args.add("package");
        Shell.Result result = Shell.capture(Duration.ofMinutes(5), Shell.mvnw(args.toArray(String[]::new)));
        if (!result.ok()) {
            String tail = result.output().lines().filter(line -> line.contains("ERROR")).limit(8)
                    .reduce("", (a, b) -> a + "\n          " + b);
            throw new CheckFailed("Derleme başarısız: " + String.join(", ", modules) + tail,
                    "Hatanın gösterdiği dosya ve satırı düzelt, sonra tekrar çalıştır.");
        }
    }

    static App start(String module, String... args) {
        try {
            Path work = Env.ROOT.resolve("target/lab-check");
            Files.createDirectories(work);
            String suffix = Long.toString(System.nanoTime(), 36);
            Path jar = work.resolve(module + "-" + suffix + ".jar");
            Files.copy(Env.ROOT.resolve("applications/" + module + "/target/" + module + ".jar"), jar,
                    StandardCopyOption.REPLACE_EXISTING);
            jar.toFile().deleteOnExit();
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            command.add("-Xmx384m");
            command.add("-jar");
            command.add(jar.toString());
            command.addAll(List.of(args));
            Process process = new ProcessBuilder(command).directory(work.toFile()).redirectErrorStream(true).start();
            return new App(module, process, work.resolve(module + "-" + suffix + ".log"));
        } catch (IOException e) {
            throw new CheckFailed(module + " başlatılamadı: " + e.getMessage());
        }
    }

    /**
     * Uygulama --server.port=0 ile açılır, gerçek port logdan okunur. Önceden boş port seçmek yarışa
     * açıktır: port seçilip uygulama açılana kadar başka bir süreç onu alabilir.
     */
    int httpPort() {
        Matcher matcher = await("Tomcat started on port (\\d+)", Duration.ofSeconds(5));
        if (matcher == null) {
            throw new CheckFailed(name + " HTTP portunu bildirmedi." + tail(6), "Log dosyası: " + logFile);
        }
        return Integer.parseInt(matcher.group(1));
    }

    private void pump() {
        try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(Files.newBufferedWriter(logFile), true)) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Surec bloklanmasin diye okumaya devam edilir, sinirdan sonrasi saklanmaz.
                if (lines.size() < MAX_LINES) {
                    lines.add(line);
                    writer.println(line);
                }
            }
        } catch (IOException ignored) {
            // Süreç kapandı.
        }
    }

    /** İlk eşleşen log satırını bekler; yoksa null. Süreç ölürse hemen döner. */
    Matcher await(String regex, Duration timeout) {
        Pattern pattern = Pattern.compile(regex);
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int checked = 0;
        while (true) {
            List<String> current = snapshot();
            for (; checked < current.size(); checked++) {
                Matcher matcher = pattern.matcher(current.get(checked));
                if (matcher.find()) {
                    return matcher;
                }
            }
            if (System.currentTimeMillis() >= deadline || (!process.isAlive() && checked >= lines.size())) {
                return null;
            }
            sleep(100);
        }
    }

    /** Spring ayağa kalkmadıysa nedenini gösterir. */
    void awaitStarted() {
        if (await("Started \\w+ in", Duration.ofSeconds(45)) == null) {
            throw new CheckFailed(name + " başlamadı. Son log satırları:" + tail(12),
                    "Log dosyası: " + logFile);
        }
    }

    long count(String regex) {
        Pattern pattern = Pattern.compile(regex);
        return snapshot().stream().filter(line -> pattern.matcher(line).find()).count();
    }

    List<String> matching(String regex) {
        Pattern pattern = Pattern.compile(regex);
        return snapshot().stream().filter(line -> pattern.matcher(line).find()).toList();
    }

    String tail(int count) {
        List<String> current = snapshot();
        int from = Math.max(0, current.size() - count);
        StringBuilder text = new StringBuilder();
        for (String line : current.subList(from, current.size())) {
            text.append("\n          ").append(line.length() > 170 ? line.substring(0, 170) : line);
        }
        return text.toString();
    }

    Path logFile() {
        return logFile;
    }

    private List<String> snapshot() {
        synchronized (lines) {
            return new ArrayList<>(lines);
        }
    }

    boolean alive() {
        return process.isAlive();
    }

    /** Sürecin kendi kendine kapanmasını bekler ve çıkış kodunu döndürür. */
    Integer awaitExit(Duration timeout) {
        try {
            return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS) ? process.exitValue() : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** kill -9 etkisi: kapanış kodu çalışmaz. */
    void kill() {
        process.destroyForcibly();
        awaitExit(Duration.ofSeconds(5));
    }

    static HttpResponse<String> post(String url) {
        return http(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.noBody()).build());
    }

    static HttpResponse<String> get(String url) {
        return http(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15)).GET().build());
    }

    static HttpResponse<String> http(HttpRequest request) {
        try {
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new CheckFailed("HTTP isteği başarısız: " + request.uri() + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CheckFailed("Kesildi");
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        process.destroy();
        if (awaitExit(Duration.ofSeconds(8)) == null) {
            process.destroyForcibly();
        }
    }
}
